import { randomUUID } from "node:crypto";
import { childOutcome } from "./children.js";
import { DEFAULT_COMPACT_AFTER, compactHistory } from "./compaction.js";
import { type ReplayContext, appendEvent, createContext } from "./context.js";
import { type DashboardHandler, type DashboardOptions, dashboard } from "./dashboard.js";
import { chainRoot, continuationRunId, runGeneration } from "./continuation.js";
import { TERMINAL, isDue } from "./due.js";
import {
  ConflictError,
  NondeterminismError,
  RunNotFoundError,
  SignalRejectedError,
  Suspend,
  errorMessage,
} from "./errors.js";
import { type LifecycleHooks, notify, runEvent } from "./hooks.js";
import { DEFAULT_RETRY } from "./retry.js";
import { compensatedError, runCompensations } from "./saga.js";
import { type ScheduleOptions, type ScheduledRun, schedulePeriod, scheduleRunId } from "./schedule.js";
import { type SignalDefinition, SignalRegistry, recordRejection, signalName } from "./signals.js";
import { normalizeTags } from "./tags.js";
import type { ChildHandle, RetryPolicy, RunPage, RunQuery, RunRecord, RunStore, WorkflowDefinition } from "./types.js";
import { type ChainView, type RunView, renderChain, renderRun } from "./view.js";
import { WorkflowRegistry, runVersion } from "./versions.js";
import { WorkerWait, isWakeupSource } from "./wakeups.js";

export interface EngineOptions {
  store: RunStore;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  workflows: WorkflowDefinition<any, any>[];
  /** Milliseconds. Injected for tests. */
  now?: () => number;
  /** How long one worker may hold a run before another may take it. Default 30s. */
  leaseMs?: number;
  /**
   * Fold a run's settled history once it holds more than this many events, so
   * replay stops scanning all of it. Default DEFAULT_COMPACT_AFTER; Infinity
   * never folds, which keeps every retry attempt on the run view's timeline.
   */
  compactAfter?: number;
  defaultRetry?: Partial<RetryPolicy>;
  /**
   * The signals this engine checks payloads against when it is handed a name
   * rather than a definition — what an admin endpoint taking `{ name, payload }`
   * off the wire needs. A name that is not here is unvalidated.
   */
  signals?: SignalDefinition<unknown>[];
  /** Observers for metrics and alerting. See LifecycleHooks. */
  hooks?: LifecycleHooks;
  idFactory?: () => string;
}

export interface WorkerHandle {
  stop(): Promise<void>;
}

export class Engine {
  private readonly store: RunStore;
  private readonly workflows = new WorkflowRegistry();
  private readonly signals = new SignalRegistry();
  private readonly now: () => number;
  private readonly leaseMs: number;
  private readonly compactAfter: number;
  private readonly defaultRetry: RetryPolicy;
  private readonly hooks: LifecycleHooks;
  private readonly newId: () => string;

  constructor(options: EngineOptions) {
    this.store = options.store;
    for (const wf of options.workflows) this.workflows.add(wf);
    for (const signal of options.signals ?? []) this.signals.add(signal);
    this.now = options.now ?? (() => Date.now());
    this.leaseMs = options.leaseMs ?? 30_000;
    this.compactAfter = options.compactAfter ?? DEFAULT_COMPACT_AFTER;
    this.defaultRetry = { ...DEFAULT_RETRY, ...options.defaultRetry };
    this.hooks = options.hooks ?? {};
    this.newId = options.idFactory ?? randomUUID;
  }

  /**
   * Create a run. It executes on the next tick / worker pass, not here.
   * A name starts on the highest registered version; a definition starts on
   * its own, which is how a caller pins a run to an older one.
   */
  async start<Input>(
    workflow: WorkflowDefinition<Input, unknown> | string,
    input: Input,
    options: {
      id?: string;
      parent?: RunRecord["parent"];
      /** Set by ctx.continueAsNew: where this run sits in a continuation chain. */
      chain?: RunRecord["chain"];
      /** Set by ctx.continueAsNew: what the predecessor had buffered and never consumed. */
      pendingSignals?: Record<string, unknown[]>;
      /**
       * The application's own names for this run — an order id, a tenant — to
       * find it by later with `list({ tag })`. Normalized here and fixed for
       * the run's life.
       */
      tags?: readonly string[];
    } = {},
  ): Promise<string> {
    const definition =
      typeof workflow === "string" ? this.workflows.latest(workflow) : this.workflows.get(workflow.name, workflow.version);
    const now = this.now();
    const run: RunRecord = {
      id: options.id ?? this.newId(),
      workflow: definition.name,
      workflowVersion: definition.version,
      input,
      parent: options.parent ?? null,
      ...(options.chain ? { chain: options.chain } : {}),
      ...(options.tags?.length ? { tags: normalizeTags(options.tags) } : {}),
      status: "running",
      history: [],
      pendingSignals: options.pendingSignals ?? {},
      wakeAt: null,
      waitingFor: null,
      pendingTimer: null,
      output: undefined,
      error: null,
      version: 0,
      leaseUntil: null,
      createdAt: now,
      updatedAt: now,
    };
    await this.store.create(run);
    return run.id;
  }

  /**
   * Start this period's run of a schedule, unless it is already there.
   *
   * Call it from whatever loop you already have, as often as you like and in
   * as many processes as you like: the run id is derived from the schedule and
   * the period, so the period starts one run however many calls it takes.
   */
  async schedule<Input>(
    workflow: WorkflowDefinition<Input, unknown> | string,
    input: Input,
    options: ScheduleOptions,
  ): Promise<ScheduledRun> {
    const periodStart = schedulePeriod(this.now(), options.every);
    const name = options.name ?? (typeof workflow === "string" ? workflow : workflow.name);
    const runId = scheduleRunId(name, periodStart);

    if (await this.store.get(runId)) return { runId, periodStart, created: false };
    try {
      await this.start(workflow, input, { id: runId });
    } catch (err) {
      // Another caller created the period's run between that read and this
      // write. The id is the primary key in every store, so the loser of the
      // race lands here rather than starting the period a second time — but
      // only a duplicate explains it, so anything else is still an error.
      if (!(await this.store.get(runId))) throw err;
      return { runId, periodStart, created: false };
    }
    return { runId, periodStart, created: true };
  }

  get(id: string): Promise<RunRecord | null> {
    return this.store.get(id);
  }

  /**
   * A page of runs, newest first. Pass the page's `cursor` back for the next
   * one, and `tag` to find the runs for one order, tenant or invoice.
   */
  list(query: RunQuery = {}): Promise<RunPage> {
    return this.store.list(query);
  }

  /** The run as an admin screen wants it: what it is blocked on, history as a timeline. */
  async view(id: string): Promise<RunView | null> {
    const run = await this.store.get(id);
    return run ? renderRun(run) : null;
  }

  /**
   * The whole continuation chain the run belongs to, as one piece of work:
   * every generation's view oldest first, their timelines on one axis, and the
   * chain's own input, outcome and elapsed time.
   *
   * `view` answers about the record you named, which is what makes a
   * generation inspectable on its own. This is the other question — an
   * operator holding `run-1` wants what happened to the work, not what
   * happened before the first handover — and either id answers it, since the
   * chain is found from the root the run names rather than from the id passed in.
   *
   * It reads once per generation, the same walk `signal` and `cancel` already
   * do to find the live run. A chain long enough for that to matter is one
   * whose generations an operator should be reading a page of with `list({ tag })`.
   */
  async viewChain(id: string): Promise<ChainView | null> {
    const named = await this.store.get(id);
    if (!named) return null;

    // Forward along the recorded handovers rather than by deriving the ids:
    // the pointer is what live() follows, so this renders the chain the engine
    // would address and not one the naming scheme implies.
    let run = chainRoot(named) === named.id ? named : await this.load(chainRoot(named));
    const generations = [run];
    while (run.continuation) {
      run = await this.load(run.continuation.runId);
      generations.push(run);
    }
    return renderChain(generations);
  }

  /**
   * That admin screen, rendered: a `Request` to `Response` handler serving the
   * run list, one run's timeline and a form to signal it, as self-contained
   * HTML. Mount it on whatever path your admin surface already authenticates —
   * it reads every run's input and output and can resume one, and it does not
   * authenticate anybody itself.
   */
  dashboard(options: DashboardOptions = {}): DashboardHandler {
    return dashboard(this, options);
  }

  /** One execution pass, if the run is due and unleased. Returns the run as persisted afterwards. */
  async tick(id: string): Promise<RunRecord> {
    const run = await this.load(id);
    const now = this.now();
    if (TERMINAL.has(run.status) || !isDue(run, now)) return run;
    if (run.leaseUntil !== null && run.leaseUntil > now) throw new ConflictError(id);

    run.leaseUntil = now + this.leaseMs;
    await this.persist(run);
    return this.execute(run);
  }

  /**
   * Tick while the run is due — through timers and retries whose time has come,
   * and on into the generations a continuation hands the work to. For tests and
   * one-shot scripts.
   */
  async settle(id: string, options: { maxTicks?: number } = {}): Promise<RunRecord> {
    const max = options.maxTicks ?? 100;
    let run = await this.live(id);
    for (let i = 0; i < max && !TERMINAL.has(run.status) && isDue(run, this.now()); i++) {
      await this.tick(run.id);
      run = await this.live(run.id);
    }
    return run;
  }

  /**
   * Deliver a signal. If the run is waiting for it, the run resumes now. If
   * not, the payload is buffered and consumed by the matching waitFor later —
   * a signal that arrives early is not lost.
   *
   * `payload` is `unknown` whichever way the signal is named: a signal arrives
   * from outside the process, so what a schema buys is the check here and the
   * type ctx.waitFor returns, not a compiler that was never in the way.
   */
  signal<T>(id: string, signal: SignalDefinition<T>, payload: unknown): Promise<RunRecord>;
  signal(id: string, name: string, payload?: unknown): Promise<RunRecord>;
  async signal(id: string, target: SignalDefinition<unknown> | string, payload: unknown = null): Promise<RunRecord> {
    const run = await this.live(id);
    if (TERMINAL.has(run.status)) throw new Error(`run ${run.id} is ${run.status}; cannot signal`);
    const name = signalName(target);
    const checked = await this.check(run, target, payload);

    if (run.waitingFor?.name === name) {
      appendEvent(run, { call: run.waitingFor.call, type: "signal.received", name, payload: checked }, this.now());
      run.waitingFor = null;
      run.wakeAt = null;
      run.status = "running";
      await this.persist(run);
      return this.tick(run.id);
    }

    (run.pendingSignals[name] ??= []).push(checked);
    await this.persist(run);
    return run;
  }

  /**
   * Check a payload against the signal's schema and return what the schema
   * made of it, or refuse it. This is the only place a payload is validated:
   * the run is entered here, so everything in `pendingSignals` and in history
   * below is a value some schema already approved — which is what lets
   * ctx.waitFor return the schema's type without parsing again, where it could
   * only disagree with the record.
   *
   * A definition the caller handed over is used whether or not the engine
   * knows it; a bare name is checked only if a schema is registered for it,
   * because the alternative would refuse every untyped signal and every
   * outcome a child reports.
   */
  private async check(run: RunRecord, target: SignalDefinition<unknown> | string, payload: unknown): Promise<unknown> {
    const definition = typeof target === "string" ? this.signals.get(target) : target;
    if (!definition) return payload;
    try {
      return definition.parse(payload);
    } catch (err) {
      const reason = errorMessage(err);
      recordRejection(run, { name: definition.name, error: reason, at: this.now() });
      try {
        await this.persist(run);
      } catch (persistErr) {
        // The audit line is worth losing to a concurrent writer; the answer to
        // the caller is not. Another worker moving the run on does not make a
        // payload this one refused delivered.
        if (!(persistErr instanceof ConflictError)) throw persistErr;
      }
      throw new SignalRejectedError(definition.name, reason, { cause: err });
    }
  }

  async cancel(id: string): Promise<RunRecord> {
    const run = await this.live(id);
    if (TERMINAL.has(run.status)) return run;
    run.status = "canceled";
    run.leaseUntil = null;
    run.wakeAt = null;
    run.waitingFor = null;
    await this.persist(run);
    await this.announce(run);
    await this.notifyParent(run);
    return run;
  }

  /** One worker pass: lease everything due, execute it. Returns how many runs were executed. */
  async processDue(limit = 10): Promise<number> {
    const claimed = await this.store.claimDue(this.now(), this.leaseMs, limit);
    let executed = 0;
    for (const run of claimed) {
      try {
        await this.execute(run);
        executed++;
      } catch (err) {
        if (!(err instanceof ConflictError)) throw err;
        // Someone else got there first — their result stands.
      }
    }
    return executed;
  }

  /**
   * Poll for due runs until stopped. Safe to run in several processes at once.
   *
   * Where the store offers wakeups, the worker subscribes to them and `pollMs`
   * becomes the fallback rather than the latency of a run that became due
   * elsewhere: the write that leaves a run claimable reaches every listening
   * worker at once, and a wake time it is told in advance is one it can be
   * there for. Pass `events: false` to poll and nothing else.
   */
  worker(options: { pollMs?: number; batch?: number; events?: boolean } = {}): WorkerHandle {
    const pollMs = options.pollMs ?? 500;
    const batch = options.batch ?? 10;
    const wait = new WorkerWait(this.now);
    const source = options.events === false || !isWakeupSource(this.store) ? null : this.store;
    let running = true;

    const loop = (async () => {
      // Subscribed before the first pass, so nothing written between the two
      // has to wait for a poll. A store that cannot be subscribed to at all
      // raises here, where a worker starting up still has someone to tell.
      const watching = source === null ? null : await source.watch((wake) => wait.push(wake));
      try {
        while (running) {
          const n = await this.processDue(batch);
          if (n === 0 && running) await wait.wait(pollMs);
        }
      } finally {
        await watching?.close();
      }
    })();

    return {
      stop: async () => {
        running = false;
        wait.close();
        await loop;
      },
    };
  }

  // ---------------------------------------------------------------------------

  /** Execute a run this worker already holds a lease on. */
  private async execute(run: RunRecord): Promise<RunRecord> {
    const now = this.now();

    // Turn the reason we woke up into history, then run from the top.
    if (run.status === "sleeping") {
      if (run.pendingTimer) {
        appendEvent(run, { call: run.pendingTimer.call, type: "timer.fired", name: run.pendingTimer.name }, now);
        run.pendingTimer = null;
      }
      run.status = "running";
      run.wakeAt = null;
    } else if (run.status === "waiting" && run.waitingFor) {
      appendEvent(run, { call: run.waitingFor.call, type: "signal.timeout", name: run.waitingFor.name }, now);
      run.waitingFor = null;
      run.status = "running";
      run.wakeAt = null;
    }

    // Fold before replaying rather than after: it is this pass that pays for a
    // history it has to scan, and the wake-up event above may settle a call too.
    if (run.history.length > this.compactAfter) compactHistory(run, now);

    // The version the run started on, not the newest: a deploy must not change
    // what a run already in flight means.
    const definition = this.workflows.get(run.workflow, runVersion(run));

    const ctx = createContext(run, {
      now: this.now,
      defaultRetry: this.defaultRetry,
      persist: (r) => this.persist(r),
      startChild: (target, input, handle) => this.startChildRun(run, target, input, handle),
      continueAsNew: (input) => this.startContinuation(run, input),
      hooks: this.hooks,
    });

    try {
      const output = await definition.run(ctx, run.input);
      if (ctx.suspended) {
        // The function returned even though a ctx call unwound it: user code
        // caught Suspend. The recorded history no longer matches what ran.
        throw new NondeterminismError(
          "workflow returned after a suspension — a ctx.waitFor/sleep/step/continueAsNew was wrapped in try/catch",
        );
      }
      run.status = "completed";
      run.output = output;
    } catch (err) {
      if (err instanceof Suspend) {
        // status/wakeAt already set and persisted by the context
      } else if (err instanceof ConflictError) {
        throw err;
      } else {
        await this.fail(run, ctx, err);
      }
    }

    run.leaseUntil = null;
    run.updatedAt = this.now();
    await this.persist(run);
    if (TERMINAL.has(run.status)) {
      // Observers first: a hook reports what is already persisted, and must not
      // be skipped because telling the parent went wrong.
      await this.announce(run);
      await this.notifyParent(run);
    }
    return run;
  }

  /**
   * Turn a failure that escaped the workflow function into the run's outcome,
   * undoing on the way out what the run registered with ctx.compensate.
   *
   * The undos run before the status is set, so a run is never seen as failed
   * while they are still outstanding: the phase suspends on a backoff like any
   * other durable unit, and the run stays asleep until it is through.
   *
   * Nondeterminism is the exception. The code and the history disagree, so which
   * undos the run owes is precisely what cannot be established from either —
   * undoing off that history would be guesswork about what actually ran.
   */
  private async fail(run: RunRecord, ctx: ReplayContext<unknown>, err: unknown): Promise<void> {
    const cause = errorMessage(err);
    if (err instanceof NondeterminismError || ctx.compensations.length === 0) {
      run.status = "failed";
      run.error = cause;
      return;
    }

    const report = await runCompensations(ctx);
    if (report.suspended) return; // asleep on an undo's backoff, already persisted
    run.status = "failed";
    run.error = compensatedError(cause, report);
  }

  /**
   * Report a run that has just reached a terminal state. Every terminal
   * transition comes through here exactly once — a terminal run is neither
   * executed nor canceled again — so this is the one place that decides which
   * of them an observer hears about.
   *
   * A canceled run is deliberately not one of them: engine.cancel returns to
   * the caller that asked for the cancellation, which is the only party a hook
   * would be telling.
   */
  private async announce(run: RunRecord): Promise<void> {
    const at = this.now();
    const event = { ...runEvent(run, at), durationMs: at - run.createdAt };
    if (run.status === "completed") await notify(this.hooks.onRunCompleted, { ...event, output: run.output });
    else if (run.status === "failed") await notify(this.hooks.onRunFailed, { ...event, error: run.error ?? "" });
  }

  /** Create the run behind a ctx.startChild handle, unless a replay already did. */
  private async startChildRun(
    parent: RunRecord,
    target: { name: string; version?: number },
    input: unknown,
    handle: ChildHandle,
  ): Promise<void> {
    // The id is derived from the parent and the call position, so a run already
    // sitting under it is this child — started by a pass that died before it
    // could record the event — and not a second one.
    if (await this.store.get(handle.runId)) return;
    const definition =
      target.version === undefined ? this.workflows.latest(target.name) : this.workflows.get(target.name, target.version);
    await this.start(definition, input, { id: handle.runId, parent: { runId: parent.id, signal: handle.signal } });
  }

  /**
   * Create the next generation behind ctx.continueAsNew, unless a pass that
   * died before recording the handover already did, and return its id.
   *
   * It starts on the newest registered version rather than the one its
   * predecessor is pinned to. A run that continues forever would otherwise
   * never reach new code, and a handover is the one point in its life where no
   * history has to survive the change — which is what makes it the place a
   * deploy drains through.
   *
   * The successor is written before the handover is recorded, so a run that
   * names a continuation always names one that exists.
   */
  private async startContinuation(run: RunRecord, input: unknown): Promise<string> {
    const chain = { root: chainRoot(run), generation: runGeneration(run) + 1 };
    const id = continuationRunId(chain.root, chain.generation);
    if (!(await this.store.get(id))) {
      await this.start(this.workflows.latest(run.workflow), input, {
        id,
        // The parent is owed the chain's outcome, not this generation's, so the
        // link moves on with the work.
        parent: run.parent,
        chain,
        // A signal that arrived before the handover was never consumed. Dropping
        // it would make losing a signal a matter of which side of the handover
        // it landed on.
        pendingSignals: { ...run.pendingSignals },
        // The chain is one piece of work under one set of names, so the tags
        // follow it. An operator searching by order id wants the generation
        // doing the work, not only the one they happened to start.
        ...(run.tags ? { tags: run.tags } : {}),
      });
    }
    return id;
  }

  /**
   * Tell a parent its child is done. It is an ordinary signal: a parent that has
   * not reached waitForChild yet buffers it, one that is waiting resumes now.
   * Retried on conflict because there is no caller to hand the error to, and a
   * dropped notification would leave the parent waiting forever.
   */
  private async notifyParent(child: RunRecord, attempts = 3): Promise<void> {
    const link = child.parent;
    if (!link) return;
    // A continued run has not finished. The successor inherited the link, so it
    // is the generation whose outcome the parent is owed.
    if (child.continuation) return;
    const outcome = childOutcome(child);

    for (let attempt = 1; ; attempt++) {
      const parent = await this.store.get(link.runId);
      if (!parent || TERMINAL.has(parent.status)) return; // nobody left to tell
      try {
        await this.signal(link.runId, link.signal, outcome);
        return;
      } catch (err) {
        // Another writer moved the parent between the read and the write — most
        // often the parent's own worker, persisting the suspension we are
        // answering. Re-read and deliver again.
        if (!(err instanceof ConflictError) || attempt === attempts) throw err;
      }
    }
  }

  /**
   * The generation at the end of a continuation chain. A continued run is a
   * forwarding pointer: whoever holds an older id — an admin screen, a child
   * about to report its outcome — is addressing the work, not the generation
   * that happened to be running when they picked the id up.
   */
  private async live(id: string): Promise<RunRecord> {
    let run = await this.load(id);
    // Generations only ever count up, so this walks to an end.
    while (run.continuation) run = await this.load(run.continuation.runId);
    return run;
  }

  private async load(id: string): Promise<RunRecord> {
    const run = await this.store.get(id);
    if (!run) throw new RunNotFoundError(id);
    return run;
  }

  private async persist(run: RunRecord): Promise<void> {
    run.updatedAt = this.now();
    const ok = await this.store.save(run, run.version);
    if (!ok) throw new ConflictError(run.id);
  }
}
