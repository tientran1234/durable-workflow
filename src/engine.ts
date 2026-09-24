import { randomUUID } from "node:crypto";
import { appendEvent, createContext } from "./context.js";
import { TERMINAL, isDue } from "./due.js";
import {
  ConflictError,
  NondeterminismError,
  RunNotFoundError,
  Suspend,
  WorkflowNotFoundError,
  errorMessage,
} from "./errors.js";
import { DEFAULT_RETRY } from "./retry.js";
import type { RetryPolicy, RunPage, RunQuery, RunRecord, RunStore, WorkflowDefinition } from "./types.js";
import { type RunView, renderRun } from "./view.js";

export interface EngineOptions {
  store: RunStore;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  workflows: WorkflowDefinition<any, any>[];
  /** Milliseconds. Injected for tests. */
  now?: () => number;
  /** How long one worker may hold a run before another may take it. Default 30s. */
  leaseMs?: number;
  defaultRetry?: Partial<RetryPolicy>;
  idFactory?: () => string;
}

export interface WorkerHandle {
  stop(): Promise<void>;
}

export class Engine {
  private readonly store: RunStore;
  private readonly workflows = new Map<string, WorkflowDefinition>();
  private readonly now: () => number;
  private readonly leaseMs: number;
  private readonly defaultRetry: RetryPolicy;
  private readonly newId: () => string;

  constructor(options: EngineOptions) {
    this.store = options.store;
    for (const wf of options.workflows) this.workflows.set(wf.name, wf);
    this.now = options.now ?? (() => Date.now());
    this.leaseMs = options.leaseMs ?? 30_000;
    this.defaultRetry = { ...DEFAULT_RETRY, ...options.defaultRetry };
    this.newId = options.idFactory ?? randomUUID;
  }

  /** Create a run. It executes on the next tick / worker pass, not here. */
  async start<Input>(
    workflow: WorkflowDefinition<Input, unknown> | string,
    input: Input,
    options: { id?: string; parent?: RunRecord["parent"] } = {},
  ): Promise<string> {
    const name = typeof workflow === "string" ? workflow : workflow.name;
    if (!this.workflows.has(name)) throw new WorkflowNotFoundError(name);
    const now = this.now();
    const run: RunRecord = {
      id: options.id ?? this.newId(),
      workflow: name,
      input,
      parent: options.parent ?? null,
      status: "running",
      history: [],
      pendingSignals: {},
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

  get(id: string): Promise<RunRecord | null> {
    return this.store.get(id);
  }

  /** A page of runs, newest first. Pass the page's `cursor` back for the next one. */
  list(query: RunQuery = {}): Promise<RunPage> {
    return this.store.list(query);
  }

  /** The run as an admin screen wants it: what it is blocked on, history as a timeline. */
  async view(id: string): Promise<RunView | null> {
    const run = await this.store.get(id);
    return run ? renderRun(run) : null;
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

  /** Tick while the run is due — through timers and retries whose time has come. For tests and one-shot scripts. */
  async settle(id: string, options: { maxTicks?: number } = {}): Promise<RunRecord> {
    const max = options.maxTicks ?? 100;
    let run = await this.load(id);
    for (let i = 0; i < max && !TERMINAL.has(run.status) && isDue(run, this.now()); i++) {
      run = await this.tick(id);
    }
    return run;
  }

  /**
   * Deliver a signal. If the run is waiting for it, the run resumes now. If
   * not, the payload is buffered and consumed by the matching waitFor later —
   * a signal that arrives early is not lost.
   */
  async signal(id: string, name: string, payload: unknown = null): Promise<RunRecord> {
    const run = await this.load(id);
    if (TERMINAL.has(run.status)) throw new Error(`run ${id} is ${run.status}; cannot signal`);

    if (run.waitingFor?.name === name) {
      appendEvent(run, { call: run.waitingFor.call, type: "signal.received", name, payload }, this.now());
      run.waitingFor = null;
      run.wakeAt = null;
      run.status = "running";
      await this.persist(run);
      return this.tick(id);
    }

    (run.pendingSignals[name] ??= []).push(payload);
    await this.persist(run);
    return run;
  }

  async cancel(id: string): Promise<RunRecord> {
    const run = await this.load(id);
    if (TERMINAL.has(run.status)) return run;
    run.status = "canceled";
    run.leaseUntil = null;
    run.wakeAt = null;
    run.waitingFor = null;
    await this.persist(run);
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

  /** Poll for due runs until stopped. Safe to run in several processes at once. */
  worker(options: { pollMs?: number; batch?: number } = {}): WorkerHandle {
    const pollMs = options.pollMs ?? 500;
    const batch = options.batch ?? 10;
    let running = true;

    const loop = (async () => {
      while (running) {
        const n = await this.processDue(batch);
        if (n === 0 && running) {
          await new Promise<void>((resolve) => setTimeout(resolve, pollMs).unref());
        }
      }
    })();

    return {
      stop: async () => {
        running = false;
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

    const definition = this.workflows.get(run.workflow);
    if (!definition) throw new WorkflowNotFoundError(run.workflow);

    const ctx = createContext(run, {
      now: this.now,
      defaultRetry: this.defaultRetry,
      persist: (r) => this.persist(r),
    });

    try {
      const output = await definition.run(ctx, run.input);
      if (ctx.suspended) {
        // The function returned even though a ctx call unwound it: user code
        // caught Suspend. The recorded history no longer matches what ran.
        throw new NondeterminismError(
          "workflow returned after a suspension — a ctx.waitFor/sleep/step was wrapped in try/catch",
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
        run.status = "failed";
        run.error = errorMessage(err);
      }
    }

    run.leaseUntil = null;
    run.updatedAt = this.now();
    await this.persist(run);
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
