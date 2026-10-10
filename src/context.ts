import { childHandle, childRunId } from "./children.js";
import { nextSeq, snapshotEvent } from "./compaction.js";
import {
  ChildFailedError,
  CompensationFailedError,
  NondeterminismError,
  StepFailedError,
  Suspend,
  WaitTimeoutError,
  errorMessage,
} from "./errors.js";
import { type LifecycleHooks, notify, runEvent } from "./hooks.js";
import { DEFAULT_RETRY, backoffMs } from "./retry.js";
import { type SignalDefinition, signalName } from "./signals.js";
import { neverAborted, withTimeout } from "./timeout.js";
import type {
  ChildHandle,
  ChildOutcome,
  Compensation,
  HistoryEvent,
  NewEvent,
  RetryPolicy,
  RunRecord,
  StepFn,
  StepOptions,
  WorkflowContext,
  WorkflowDefinition,
} from "./types.js";

/** Append to history with the next sequence number. Used by the context and the engine. */
export function appendEvent(run: RunRecord, event: NewEvent, now: number): void {
  run.history.push({ ...event, seq: nextSeq(run), at: now } as HistoryEvent);
}

/**
 * Which pair of history events a durable unit records: the work a workflow
 * asked for, or an undo the engine ran because the run failed. Keeping them
 * apart is what stops a timeline reading an undo as the work it reversed.
 */
type DurableKind = "step" | "compensation";

const COMPLETED = { step: "step.completed", compensation: "compensation.completed" } as const;
const FAILED = { step: "step.failed", compensation: "compensation.failed" } as const;

/** The two events that carry an attempt number and a backoff. */
type FailureEvent = Extract<HistoryEvent, { type: "step.failed" | "compensation.failed" }>;

function completedEvent(kind: DurableKind, call: number, name: string, result: unknown): NewEvent {
  // An undo records no result: nothing downstream reads what an undo returned.
  return kind === "step"
    ? { call, type: "step.completed", name, result }
    : { call, type: "compensation.completed", name };
}

function failedEvent(
  kind: DurableKind,
  call: number,
  name: string,
  attempt: number,
  error: string,
  retryAt: number | undefined,
): NewEvent {
  const backoff = retryAt !== undefined ? { retryAt } : {};
  return kind === "step"
    ? { call, type: "step.failed", name, attempt, error, ...backoff }
    : { call, type: "compensation.failed", name, attempt, error, ...backoff };
}

/**
 * What a unit throws once its policy is spent. A step's failure goes to the
 * workflow function; an undo's has nowhere to go but the engine, which is the
 * whole difference between them.
 */
function exhausted(
  kind: DurableKind,
  name: string,
  attempt: number,
  error: string,
  options?: { cause?: unknown },
): Error {
  return kind === "step"
    ? new StepFailedError(name, attempt, error, options)
    : new CompensationFailedError(name, attempt, error, options);
}

export interface ContextDeps {
  now: () => number;
  defaultRetry: RetryPolicy;
  /** Persist the run, or throw ConflictError. */
  persist: (run: RunRecord) => Promise<void>;
  /**
   * Create the child run behind `handle`. Idempotent: an existing run under
   * that id is that child. `version` is absent when the child was named by
   * string, which starts it on the latest.
   */
  startChild: (target: { name: string; version?: number }, input: unknown, handle: ChildHandle) => Promise<void>;
  /**
   * Create the next generation of this run's chain and return its id.
   * Idempotent, for the same reason startChild is: the id is derived, so a run
   * already sitting under it is that successor.
   */
  continueAsNew: (input: unknown) => Promise<string>;
  /** Observers for metrics and alerting. The step frontier reports failed attempts. */
  hooks: LifecycleHooks;
}

export interface ReplayContext<Input> extends WorkflowContext<Input> {
  /** True once this pass hit a waitFor/sleep/retry and unwound. */
  readonly suspended: boolean;
  /**
   * The undos ctx.compensate reached on this pass, in registration order. The
   * engine runs them when the pass ends in a failure; see src/saga.ts.
   */
  readonly compensations: readonly Compensation[];
  /** Run one of those undos as a durable unit of its own. */
  undo(compensation: Compensation): Promise<void>;
}

/**
 * The replay engine. The workflow function is re-executed from the top on
 * every tick; each ctx call counts its position and looks for its own event
 * in history. Found → return the recorded outcome instantly. Not found → this
 * is the frontier: do the work, record it, persist.
 */
export function createContext<Input>(run: RunRecord, deps: ContextDeps): ReplayContext<Input> {
  let call = 0;
  let suspended = false;
  const compensations: Compensation[] = [];

  const at = (c: number, types: HistoryEvent["type"][]) => {
    // A settled call is one event in the snapshot, found by position. That is
    // what stops a replay scanning the whole history once per call.
    const folded = snapshotEvent(run, c);
    if (folded) return types.includes(folded.type) ? [folded] : [];
    return run.history.filter((e) => e.call === c && types.includes(e.type));
  };

  const push = (event: NewEvent) => appendEvent(run, event, deps.now());

  const expectName = (event: HistoryEvent, name: string, kind: string) => {
    if (event.name !== name) {
      throw new NondeterminismError(
        `${kind} at position ${event.call} was "${event.name}" in history but "${name}" now — workflow code changed mid-run`,
      );
    }
  };

  // `after` runs once the suspension is persisted: a hook must not report an
  // attempt that a lost write threw away.
  const suspend = async (after?: () => Promise<void>): Promise<never> => {
    suspended = true;
    await deps.persist(run);
    await after?.();
    throw new Suspend();
  };

  /**
   * The frontier of one durable unit — a ctx.step, or one of the undos the
   * engine runs when a pass ends in a failure. Both memoise their outcome by
   * call position, derive their attempt count from history and serve their
   * backoff as a persisted wake time, so the kind decides only which events they
   * record and which error they throw once the policy is spent.
   */
  const runDurable = async <T>(
    kind: DurableKind,
    c: number,
    name: string,
    fn: StepFn<T>,
    options: StepOptions | undefined,
  ): Promise<T> => {
    const completed = at(c, [COMPLETED[kind]])[0];
    if (completed) {
      expectName(completed, name, kind);
      return (completed.type === "step.completed" ? completed.result : undefined) as T;
    }

    const failures = at(c, [FAILED[kind]]) as FailureEvent[];
    if (failures[0]) expectName(failures[0], name, kind);

    const final = failures.find((f) => f.retryAt === undefined);
    if (final) throw exhausted(kind, name, final.attempt, final.error);

    const last = failures[failures.length - 1];
    if (last?.retryAt !== undefined && last.retryAt > deps.now()) {
      // Woken early (e.g. a direct tick) — go back to sleep until the backoff elapses.
      run.status = "sleeping";
      run.wakeAt = last.retryAt;
      return suspend();
    }

    const attempt = failures.length + 1;
    try {
      const timeoutMs = options?.timeoutMs;
      // The bound is per attempt, not per step: the retries below are a
      // succession of attempts, and each gets the whole of it.
      const result =
        timeoutMs === undefined ? await fn(neverAborted()) : await withTimeout(name, timeoutMs, fn);
      push(completedEvent(kind, c, name, result));
      await deps.persist(run);
      return result;
    } catch (err) {
      const policy: RetryPolicy = { ...deps.defaultRetry, ...options?.retry };
      const retryAt = attempt < policy.maxAttempts ? deps.now() + backoffMs(policy, attempt) : undefined;
      const error = errorMessage(err);
      push(failedEvent(kind, c, name, attempt, error, retryAt));

      // Only the frontier gets here — a replay reads the attempt back out of
      // history above — so an attempt is reported the once it happened.
      const report = () =>
        notify(deps.hooks.onStepFailed, {
          ...runEvent(run, deps.now()),
          kind,
          step: name,
          attempt,
          error,
          retryAt: retryAt ?? null,
        });

      if (retryAt === undefined) {
        await deps.persist(run);
        await report();
        throw exhausted(kind, name, attempt, error, { cause: err });
      }
      // Durable backoff: the delay is a persisted wake time, not a setTimeout.
      // A restart during the wait loses nothing.
      run.status = "sleeping";
      run.wakeAt = retryAt;
      return suspend(report);
    }
  };

  const ctx: ReplayContext<Input> = {
    runId: run.id,
    input: run.input as Input,
    get suspended() {
      return suspended;
    },
    get compensations() {
      return compensations;
    },

    async step<T>(name: string, fn: StepFn<T>, options?: StepOptions): Promise<T> {
      return runDurable("step", call++, name, fn, options);
    },

    compensate(name: string, fn: StepFn<unknown>, options?: StepOptions): void {
      // No call position and no event. Registering decides nothing, so there is
      // nothing for a later replay to read back — and a position that never got
      // an event of its own would stop compaction folding past it for the rest
      // of the run.
      compensations.push({ name, fn, ...(options ? { options } : {}) });
    },

    async undo(compensation: Compensation): Promise<void> {
      // An undo takes a call position above everything the workflow function
      // reached. The pass running it has already unwound, so those positions are
      // as fixed across replays as the calls below them.
      await runDurable("compensation", call++, compensation.name, compensation.fn, compensation.options);
    },

    async waitFor<T>(target: SignalDefinition<T> | string, options?: { timeoutMs?: number }): Promise<T> {
      const c = call++;
      // A definition carries the schema the engine already applied on the way
      // in, so nothing is parsed here: what history holds is what that schema
      // returned, and re-checking it would only be able to disagree.
      const name = signalName(target);

      const received = at(c, ["signal.received"])[0];
      if (received && received.type === "signal.received") {
        expectName(received, name, "waitFor");
        return received.payload as T;
      }
      const timedOut = at(c, ["signal.timeout"])[0];
      if (timedOut) {
        expectName(timedOut, name, "waitFor");
        throw new WaitTimeoutError(name);
      }

      // A signal that arrived before we got here is consumed now, in order.
      const buffered = run.pendingSignals[name];
      if (buffered && buffered.length > 0) {
        const payload = buffered.shift();
        if (buffered.length === 0) delete run.pendingSignals[name];
        push({ call: c, type: "signal.received", name, payload });
        await deps.persist(run);
        return payload as T;
      }

      run.status = "waiting";
      run.waitingFor = { name, call: c };
      run.wakeAt = options?.timeoutMs !== undefined ? deps.now() + options.timeoutMs : null;
      return suspend();
    },

    async startChild<ChildInput, ChildOutput>(
      workflow: WorkflowDefinition<ChildInput, ChildOutput> | string,
      input: ChildInput,
    ): Promise<ChildHandle<ChildOutput>> {
      const c = call++;
      const name = typeof workflow === "string" ? workflow : workflow.name;

      const started = at(c, ["child.started"])[0];
      if (started && started.type === "child.started") {
        expectName(started, name, "startChild");
        return childHandle<ChildOutput>(started.childRunId, name);
      }

      const handle = childHandle<ChildOutput>(childRunId(run.id, c), name);
      await deps.startChild(typeof workflow === "string" ? { name } : { name, version: workflow.version }, input, handle);
      push({ call: c, type: "child.started", name, childRunId: handle.runId });
      await deps.persist(run);
      return handle;
    },

    async waitForChild<Output>(handle: ChildHandle<Output>, options?: { timeoutMs?: number }): Promise<Output> {
      // Nothing durable is added here: the engine signals the parent when the
      // child finishes, so a child that finishes first is an early signal like
      // any other, and the wait survives a restart because waitFor does.
      const outcome = await ctx.waitFor<ChildOutcome>(handle.signal, options);
      if (outcome.status !== "completed") {
        throw new ChildFailedError(handle.workflow, handle.runId, outcome.status, outcome.error);
      }
      return outcome.output as Output;
    },

    async sleep(name: string, ms: number): Promise<void> {
      const c = call++;

      const fired = at(c, ["timer.fired"])[0];
      if (fired) {
        expectName(fired, name, "sleep");
        return;
      }

      run.status = "sleeping";
      run.wakeAt = deps.now() + ms;
      run.pendingTimer = { name, call: c };
      return suspend();
    },

    async continueAsNew(input: Input): Promise<never> {
      // No call position and no history event. The successor's id is derived,
      // so creating it is idempotent, and this run is terminal the moment the
      // handover is recorded — there is nothing for a later replay to read
      // back, and nothing above this call to keep a position for.
      const runId = await deps.continueAsNew(input);

      run.status = "continued";
      run.continuation = { runId };
      run.wakeAt = null;
      run.waitingFor = null;
      run.pendingTimer = null;
      // The successor was created holding these. Leaving a copy here would
      // show an operator payloads that another run is going to consume.
      run.pendingSignals = {};
      return suspend();
    },
  };

  return ctx;
}
