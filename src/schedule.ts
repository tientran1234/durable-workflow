/**
 * Starting a run every period, from a loop that has no memory of the last one.
 *
 * Nothing here is persisted. A period's run *is* the record that the period
 * fired: the id it takes is derived from the schedule and the period rather
 * than generated, so a second call in the same period addresses the run the
 * first one created instead of starting a second one.
 */

export interface ScheduleOptions {
  /** The period, in milliseconds. */
  every: number;
  /**
   * Which schedule this is, when one workflow has more than one. Defaults to
   * the workflow's name. It is part of the run id, so two schedules over one
   * workflow share a period's run unless they are named apart.
   */
  name?: string;
}

/** What a call to `engine.schedule` did, or found already done. */
export interface ScheduledRun {
  runId: string;
  /** Start of the period the call fell in, on the engine's clock. */
  periodStart: number;
  /** False when the period's run already existed: this call started nothing. */
  created: boolean;
}

/**
 * The start of the period `now` falls in. The grid is anchored at the epoch,
 * not at the first call, so every process computes the same boundaries without
 * agreeing on a start time first — which is what lets the schedule be driven
 * from all of them at once.
 */
export function schedulePeriod(now: number, every: number): number {
  if (!Number.isFinite(every) || every <= 0) {
    throw new RangeError(`schedule "every" must be a positive number of milliseconds, got ${every}`);
  }
  return Math.floor(now / every) * every;
}

/** The id a period's run takes. Its uniqueness is what makes the period fire once. */
export function scheduleRunId(name: string, periodStart: number): string {
  return `${name}@${periodStart}`;
}
