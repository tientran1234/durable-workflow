import type { RunRecord } from "./types.js";
import { runVersion } from "./versions.js";

/** What every hook payload carries: which run, on which workflow and version. */
export interface RunEvent {
  runId: string;
  workflow: string;
  /** The version the run replays on, not the newest registered. */
  workflowVersion: number;
  /** When the engine reported this, on the engine's clock. */
  at: number;
}

export interface RunCompletedEvent extends RunEvent {
  output: unknown;
  /** Start to terminal state — the number a duration histogram wants. */
  durationMs: number;
}

export interface RunFailedEvent extends RunEvent {
  error: string;
  durationMs: number;
}

export interface StepFailedEvent extends RunEvent {
  step: string;
  /** 1 on the step's first failure in this run. */
  attempt: number;
  error: string;
  /**
   * When the engine will try again, or null when that attempt was the last —
   * the difference between a blip worth counting and a failure worth a page.
   */
  retryAt: number | null;
}

/**
 * Observers the engine calls as runs reach these points. They are for metrics
 * and alerting only: a hook cannot change what the run does, and the state it
 * is told about is already persisted when it is called.
 */
export interface LifecycleHooks {
  onRunCompleted?(event: RunCompletedEvent): void | Promise<void>;
  onRunFailed?(event: RunFailedEvent): void | Promise<void>;
  /** Once per failed attempt, including the ones that will be retried. */
  onStepFailed?(event: StepFailedEvent): void | Promise<void>;
}

export function runEvent(run: RunRecord, at: number): RunEvent {
  return { runId: run.id, workflow: run.workflow, workflowVersion: runVersion(run), at };
}

/**
 * Call a hook without letting it reach the run. A metrics or alerting call is
 * allowed to be broken in ways a workflow is not, so whatever it throws is
 * dropped here: the alternative is failing a run that already succeeded, over
 * a statsd socket.
 */
export async function notify<E>(hook: ((event: E) => void | Promise<void>) | undefined, event: E): Promise<void> {
  if (!hook) return;
  try {
    await hook(event);
  } catch {
    // Deliberately swallowed — see above.
  }
}
