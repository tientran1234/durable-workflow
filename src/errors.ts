/**
 * Internal control flow: thrown by ctx.waitFor / ctx.sleep / a retrying step to
 * unwind the workflow function. The engine catches it. Workflow code must let it
 * through — `try { await ctx.waitFor(...) } catch {}` would swallow it, which
 * the engine detects and reports as NondeterminismError.
 */
export class Suspend extends Error {
  override readonly name = "WorkflowSuspended";
  constructor() {
    super("workflow suspended (this is control flow, not a failure — do not catch it)");
  }
}

export class StepFailedError extends Error {
  override readonly name = "StepFailedError";
  constructor(
    readonly step: string,
    readonly attempts: number,
    readonly lastError: string,
    options?: { cause?: unknown },
  ) {
    super(`step "${step}" failed after ${attempts} attempt(s): ${lastError}`, options);
  }
}

/**
 * One attempt at a step — or at an undo — ran past its `timeoutMs` and was
 * abandoned. It never reaches workflow code on its own: the frontier records it
 * as a failed attempt and retries like any other failure, so this arrives as
 * the `cause` of the StepFailedError the last attempt throws, where code that
 * treats a hang differently from a refusal can tell the two apart.
 */
export class StepTimeoutError extends Error {
  override readonly name = "StepTimeoutError";
  constructor(
    readonly step: string,
    readonly timeoutMs: number,
  ) {
    // The step is named by the history event and by the error that wraps this
    // one, so the message says only what the attempt did.
    super(`timed out after ${timeoutMs}ms`);
  }
}

/**
 * A registered undo exhausted its retries, so the work it was there to undo is
 * still done. It never reaches workflow code — the function has already unwound
 * by the time compensations run — so the engine is what catches it, records the
 * undo as not done, and carries on with the rest of them.
 */
export class CompensationFailedError extends Error {
  override readonly name = "CompensationFailedError";
  constructor(
    readonly compensation: string,
    readonly attempts: number,
    readonly lastError: string,
    options?: { cause?: unknown },
  ) {
    super(`compensation "${compensation}" failed after ${attempts} attempt(s): ${lastError}`, options);
  }
}

/**
 * A signal's payload did not satisfy the signal's schema, so the engine refused
 * it. Thrown to whoever sent it — an admin endpoint, a webhook handler — rather
 * than into the workflow: the run never saw the payload, and the caller is the
 * only party that can do anything about its shape.
 */
export class SignalRejectedError extends Error {
  override readonly name = "SignalRejectedError";
  constructor(
    readonly signal: string,
    readonly reason: string,
    options?: { cause?: unknown },
  ) {
    super(`signal "${signal}" was rejected: ${reason}`, options);
  }
}

export class WaitTimeoutError extends Error {
  override readonly name = "WaitTimeoutError";
  constructor(readonly signal: string) {
    super(`timed out waiting for signal "${signal}"`);
  }
}

/**
 * A child run finished without a result. Thrown into the parent by
 * ctx.waitForChild, so supervising a child is plain code: catch it, compensate,
 * carry on — the same shape as StepFailedError.
 */
export class ChildFailedError extends Error {
  override readonly name = "ChildFailedError";
  constructor(
    readonly workflow: string,
    readonly childRunId: string,
    readonly status: "failed" | "canceled",
    readonly reason: string,
  ) {
    super(`child workflow "${workflow}" (${childRunId}) ${status}: ${reason}`);
  }
}

/** The workflow code no longer matches its own history — e.g. a step was renamed or reordered mid-run. */
export class NondeterminismError extends Error {
  override readonly name = "NondeterminismError";
}

/** Another worker persisted this run first. Drop the work and re-read. */
export class ConflictError extends Error {
  override readonly name = "ConflictError";
  constructor(readonly runId: string) {
    super(`run ${runId} was modified concurrently`);
  }
}

export class RunNotFoundError extends Error {
  override readonly name = "RunNotFoundError";
  constructor(readonly runId: string) {
    super(`run ${runId} not found`);
  }
}

export class WorkflowNotFoundError extends Error {
  override readonly name = "WorkflowNotFoundError";
  constructor(
    readonly workflow: string,
    readonly version?: number,
  ) {
    super(
      version === undefined
        ? `workflow "${workflow}" is not registered`
        : // Retiring a version while runs are still pinned to it strands them:
          // they cannot replay until that code is registered again.
          `workflow "${workflow}" version ${version} is not registered — runs started on it cannot replay without it`,
    );
  }
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
