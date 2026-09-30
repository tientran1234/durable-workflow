import type { ReplayContext } from "./context.js";
import { CompensationFailedError, Suspend } from "./errors.js";

/** What the phase needs of a replay context: the registrations, and a way to run one. */
type Compensating = Pick<ReplayContext<unknown>, "compensations" | "undo">;

/**
 * What one pass of the compensation phase did. Nothing of it is kept on the
 * run: an undo that already ran is memoised by its history event, so a pass
 * that replays the phase rebuilds the same report from the same events.
 */
export interface CompensationReport {
  /** The undos that completed, in the order they ran. */
  undone: string[];
  /** The undos that exhausted their retries — the work each was there to reverse is still done. */
  failed: { name: string; error: string }[];
  /** True while an undo is serving out a backoff, which means the phase is not finished. */
  suspended: boolean;
}

/**
 * Undo what a failing run registered along the way, newest first.
 *
 * Reverse order because a saga is a stack: the later work was done on top of
 * the earlier, so undoing outwards is the only order in which each undo finds
 * the state it was registered against.
 *
 * An undo that exhausts its retries does not stop the phase. The registrations
 * below it cover work that is still done, and abandoning them would leave more
 * of the saga applied than the failure already has — so the phase carries on
 * and the run says which undos did not happen.
 */
export async function runCompensations(ctx: Compensating): Promise<CompensationReport> {
  const report: CompensationReport = { undone: [], failed: [], suspended: false };

  for (const compensation of [...ctx.compensations].reverse()) {
    try {
      await ctx.undo(compensation);
      report.undone.push(compensation.name);
    } catch (err) {
      if (err instanceof Suspend) {
        // The frontier has persisted the wake time. The next pass replays into
        // this phase and picks the same positions up where this one left them.
        report.suspended = true;
        return report;
      }
      // A ConflictError, and anything else the phase has no answer for, is the
      // engine's to handle.
      if (!(err instanceof CompensationFailedError)) throw err;
      report.failed.push({ name: compensation.name, error: err.lastError });
    }
  }
  return report;
}

/**
 * The error a compensated run fails with. The cause stays the subject — it is
 * what failed — and the undos that did not happen are named after it, because a
 * saga left half-applied needs a person and the failure that caused it may not.
 */
export function compensatedError(cause: string, report: CompensationReport): string {
  if (report.failed.length === 0) return cause;
  const failed = report.failed.map((f) => `"${f.name}" (${f.error})`).join(", ");
  return `${cause}; compensation did not complete: ${failed}`;
}
