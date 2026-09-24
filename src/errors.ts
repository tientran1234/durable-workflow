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
  constructor(readonly workflow: string) {
    super(`workflow "${workflow}" is not registered`);
  }
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
