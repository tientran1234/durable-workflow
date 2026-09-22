import type { RunRecord, RunStatus } from "./types.js";

export const TERMINAL: ReadonlySet<RunStatus> = new Set(["completed", "failed", "canceled"]);

/** Should a worker pick this run up at `now`? Leases are checked separately. */
export function isDue(run: Pick<RunRecord, "status" | "wakeAt">, now: number): boolean {
  if (run.status === "running") return true;
  if (run.status === "sleeping" || run.status === "waiting") {
    return run.wakeAt !== null && run.wakeAt <= now;
  }
  return false;
}
