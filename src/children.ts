import type { ChildHandle, ChildOutcome, RunRecord } from "./types.js";

/**
 * A child's run id. It is derived from the parent's id and the position of the
 * startChild call rather than generated, so a replay — including one that
 * follows a crash between creating the child and recording it — addresses the
 * same child instead of forking a second one.
 */
export function childRunId(parentRunId: string, call: number): string {
  return `${parentRunId}#${call}`;
}

/** The signal the engine delivers to the parent when the child reaches a terminal state. */
export function childSignal(childRunId: string): string {
  return `child:${childRunId}`;
}

export function childHandle<Output>(childRunId: string, workflow: string): ChildHandle<Output> {
  return { runId: childRunId, workflow, signal: childSignal(childRunId) };
}

/** A finished child as its parent sees it. */
export function childOutcome(child: RunRecord): ChildOutcome {
  if (child.status === "completed") return { status: "completed", output: child.output };
  if (child.status === "canceled") return { status: "canceled", error: "child run was canceled" };
  return { status: "failed", error: child.error ?? "child run failed" };
}
