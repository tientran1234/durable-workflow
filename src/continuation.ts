import type { RunRecord } from "./types.js";

/**
 * Ending a long run to start a fresh one.
 *
 * Compaction stops replay paying for history it has already settled, but the
 * settled results themselves are kept because replay needs them, so a run that
 * never ends still grows. A continuation ends the run and hands the work to a
 * successor with an empty history, carrying forward whatever state the
 * workflow chose to pass on.
 */

/**
 * The id a chain started under. A successor is derived from the root and its
 * generation rather than from the run it follows, so a chain that continues
 * forever does not grow a suffix per generation.
 */
export function chainRoot(run: Pick<RunRecord, "id" | "chain">): string {
  return run.chain?.root ?? run.id;
}

/** Which generation of its chain a run is. 1 for a run that was started directly. */
export function runGeneration(run: Pick<RunRecord, "chain">): number {
  return run.chain?.generation ?? 1;
}

/**
 * The id a generation takes. Derived, like a child's, so a pass that dies
 * between creating the successor and recording it replays into the same run
 * instead of forking a second chain.
 */
export function continuationRunId(root: string, generation: number): string {
  return `${root}~${generation}`;
}
