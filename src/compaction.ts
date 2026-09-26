import type { HistoryEvent, RunRecord } from "./types.js";

/**
 * How many events a run's history may hold before the engine folds its settled
 * prefix. Small enough that no tick scans much, large enough that a run of
 * ordinary length is never compacted and keeps a complete timeline.
 */
export const DEFAULT_COMPACT_AFTER = 1_000;

/**
 * The seq the next appended event takes. It is not `history.length`: compaction
 * moves events out of history without reusing their sequence numbers, so the
 * count of everything that came before lives on the snapshot.
 */
export function nextSeq(run: Pick<RunRecord, "history" | "snapshot">): number {
  return (run.snapshot?.nextSeq ?? 0) + run.history.length;
}

/**
 * Does this event settle its ctx call — is it what replay returns or throws? A
 * `step.failed` carrying `retryAt` settles nothing: the step is going to run
 * again, and its attempt count is derived from those events.
 */
function settles(event: HistoryEvent): boolean {
  return event.type !== "step.failed" || event.retryAt === undefined;
}

/**
 * How many leading call positions are settled, the snapshot's included. Only a
 * prefix is ever foldable, and that is free rather than lucky: a pass suspends
 * at the frontier, so positions above an unsettled one have no events at all.
 */
export function settledPrefix(run: RunRecord): number {
  const settled = new Set<number>();
  for (const event of run.history) if (settles(event)) settled.add(event.call);

  let calls = run.snapshot?.calls ?? 0;
  while (settled.has(calls)) calls++;
  return calls;
}

/**
 * Fold the settled prefix of history into the snapshot: one event per call,
 * indexed by position. Returns false when there is nothing new to fold.
 *
 * Lossy on purpose. The attempts a step made before it succeeded are dropped,
 * because replay only ever needs the outcome — which is also why a run that
 * retried thousands of times stops carrying them.
 */
export function compactHistory(run: RunRecord, now: number): boolean {
  const calls = settledPrefix(run);
  if (calls === (run.snapshot?.calls ?? 0)) return false;

  const events = run.snapshot ? [...run.snapshot.events] : [];
  const tail: HistoryEvent[] = [];
  let dropped = 0;

  for (const event of run.history) {
    if (event.call >= calls) tail.push(event);
    else if (settles(event)) events[event.call] = event;
    else dropped++;
  }

  // Sequence numbers are absolute, so the tail keeps its own and the snapshot
  // carries the offset the next append continues from.
  const seqBefore = nextSeq(run) - tail.length;
  run.snapshot = {
    calls,
    events,
    nextSeq: seqBefore,
    droppedEvents: (run.snapshot?.droppedEvents ?? 0) + dropped,
    at: now,
  };
  run.history = tail;
  return true;
}

/** The snapshot's event for a settled call, or undefined if the snapshot does not cover it. */
export function snapshotEvent(run: Pick<RunRecord, "snapshot">, call: number): HistoryEvent | undefined {
  const snapshot = run.snapshot;
  return snapshot !== undefined && call < snapshot.calls ? snapshot.events[call] : undefined;
}

/** Everything still on record, oldest first: the snapshot's events, then history's. */
export function historyEvents(run: Pick<RunRecord, "history" | "snapshot">): HistoryEvent[] {
  return [...(run.snapshot?.events ?? []), ...run.history];
}
