import { NondeterminismError, StepFailedError, Suspend, WaitTimeoutError, errorMessage } from "./errors.js";
import { DEFAULT_RETRY, backoffMs } from "./retry.js";
import type { HistoryEvent, NewEvent, RetryPolicy, RunRecord, StepOptions, WorkflowContext } from "./types.js";

/** Append to history with the next sequence number. Used by the context and the engine. */
export function appendEvent(run: RunRecord, event: NewEvent, now: number): void {
  run.history.push({ ...event, seq: run.history.length, at: now } as HistoryEvent);
}

export interface ContextDeps {
  now: () => number;
  defaultRetry: RetryPolicy;
  /** Persist the run, or throw ConflictError. */
  persist: (run: RunRecord) => Promise<void>;
}

export interface ReplayContext<Input> extends WorkflowContext<Input> {
  /** True once this pass hit a waitFor/sleep/retry and unwound. */
  readonly suspended: boolean;
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

  const at = (c: number, types: HistoryEvent["type"][]) =>
    run.history.filter((e) => e.call === c && types.includes(e.type));

  const push = (event: NewEvent) => appendEvent(run, event, deps.now());

  const expectName = (event: HistoryEvent, name: string, kind: string) => {
    if (event.name !== name) {
      throw new NondeterminismError(
        `${kind} at position ${event.call} was "${event.name}" in history but "${name}" now — workflow code changed mid-run`,
      );
    }
  };

  const suspend = async (): Promise<never> => {
    suspended = true;
    await deps.persist(run);
    throw new Suspend();
  };

  const ctx: ReplayContext<Input> = {
    runId: run.id,
    input: run.input as Input,
    get suspended() {
      return suspended;
    },

    async step<T>(name: string, fn: () => Promise<T> | T, options?: StepOptions): Promise<T> {
      const c = call++;

      const completed = at(c, ["step.completed"])[0];
      if (completed && completed.type === "step.completed") {
        expectName(completed, name, "step");
        return completed.result as T;
      }

      const failures = at(c, ["step.failed"]) as Extract<HistoryEvent, { type: "step.failed" }>[];
      if (failures[0]) expectName(failures[0], name, "step");

      const final = failures.find((f) => f.retryAt === undefined);
      if (final) throw new StepFailedError(name, final.attempt, final.error);

      const last = failures[failures.length - 1];
      if (last?.retryAt !== undefined && last.retryAt > deps.now()) {
        // Woken early (e.g. a direct tick) — go back to sleep until the backoff elapses.
        run.status = "sleeping";
        run.wakeAt = last.retryAt;
        return suspend();
      }

      const attempt = failures.length + 1;
      try {
        const result = await fn();
        push({ call: c, type: "step.completed", name, result });
        await deps.persist(run);
        return result;
      } catch (err) {
        const policy: RetryPolicy = { ...deps.defaultRetry, ...options?.retry };
        const retryAt = attempt < policy.maxAttempts ? deps.now() + backoffMs(policy, attempt) : undefined;
        push({ call: c, type: "step.failed", name, attempt, error: errorMessage(err), ...(retryAt !== undefined ? { retryAt } : {}) });

        if (retryAt === undefined) {
          await deps.persist(run);
          throw new StepFailedError(name, attempt, errorMessage(err), { cause: err });
        }
        // Durable backoff: the delay is a persisted wake time, not a setTimeout.
        // A restart during the wait loses nothing.
        run.status = "sleeping";
        run.wakeAt = retryAt;
        return suspend();
      }
    },

    async waitFor<T>(name: string, options?: { timeoutMs?: number }): Promise<T> {
      const c = call++;

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
  };

  return ctx;
}
