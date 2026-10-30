/**
 * Starting a run every period, from a loop that has no memory of the last one.
 *
 * Nothing here is persisted. A period's run *is* the record that the period
 * fired: the id it takes is derived from the schedule and the period rather
 * than generated, so a second call in the same period addresses the run the
 * first one created instead of starting a second one.
 */

import { cronPrevious } from "./cron.js";

/**
 * How many periods behind this one a single call will look back over. Bounded
 * because each one it reaches is a read, and the gap this closes is a loop that
 * was down for a while: replaying a schedule from further back than this is an
 * operator deciding to, with the ids in hand, rather than a tick catching up.
 *
 * Sixty-four covers a nightly schedule down for two months and an hourly one
 * down for two and a half days.
 */
export const MAX_CATCH_UP = 64;

/** What both kinds of schedule take, whatever a period is under them. */
interface SharedSchedule {
  /**
   * Which schedule this is, when one workflow has more than one. Defaults to
   * the workflow's name. It is part of the run id, so two schedules over one
   * workflow share a period's run unless they are named apart.
   */
  name?: string;
  /**
   * How many periods before this one to start if nothing has: the gap a worker
   * that was down leaves behind. Defaults to none, which is the old behaviour —
   * a period nobody called during stays skipped.
   */
  catchUp?: number;
}

/** A period off the epoch's grid: the same boundaries in every process. */
export interface EverySchedule extends SharedSchedule {
  /** The period, in milliseconds. */
  every: number;
}

/** A period a calendar decides: the first of a month, the last Friday, 9am local. */
export interface CronSchedule extends SharedSchedule {
  /** `minute hour day-of-month month day-of-week`, with `L` for a month's last. */
  cron: string;
  /** The zone the expression is written in, as an IANA name. Defaults to UTC. */
  tz?: string;
}

export type ScheduleOptions = EverySchedule | CronSchedule;

/** A period, and the run whose id is derived from it. */
export interface ScheduledPeriod {
  runId: string;
  /**
   * The period this is: where it began on an `every` grid, and the firing the
   * expression named at or before the call on a `cron` one.
   */
  periodStart: number;
}

/** What a call to `engine.schedule` did, or found already done. */
export interface ScheduledRun extends ScheduledPeriod {
  /** False when the period's run already existed: this call started nothing. */
  created: boolean;
  /**
   * The periods behind this one that `catchUp` reached and found empty, oldest
   * first, now each holding a run this call started. Absent when it started
   * none, which is every call a loop that kept up makes.
   */
  caughtUp?: readonly ScheduledPeriod[];
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

/**
 * The period a call falls in, however the schedule says what a period is. Both
 * answers come from the clock and the schedule alone, which is what lets the
 * loop driving them remember nothing.
 */
export function schedulePeriodFor(now: number, options: ScheduleOptions): number {
  return "cron" in options ? cronPrevious(now, options.cron, options.tz) : schedulePeriod(now, options.every);
}

/**
 * How many periods behind this one the call was told to look back over.
 *
 * Read and checked before anything is written, so a look-back this cannot
 * honour fails the call rather than the half of it that is left.
 */
export function catchUpCount(options: ScheduleOptions): number {
  const count = options.catchUp ?? 0;
  if (!Number.isInteger(count) || count < 0 || count > MAX_CATCH_UP) {
    throw new RangeError(`schedule "catchUp" must be a whole number of periods between 0 and ${MAX_CATCH_UP}, got ${options.catchUp}`);
  }
  return count;
}

/**
 * The `count` periods before `periodStart`, oldest first.
 *
 * Each is derived from the schedule and a moment exactly as the current period
 * is, so a call catching up computes the ids the calls that should have made
 * them would have — which is the whole of why a backfill can be idempotent
 * without a schedule remembering anything.
 *
 * Stepping back a millisecond is what crosses a boundary: a grid period's start
 * and a firing both land on a whole minute, so the instant before one is inside
 * the period before it, whatever a calendar says the gap between them is.
 */
export function previousPeriods(periodStart: number, count: number, options: ScheduleOptions): number[] {
  const periods: number[] = [];
  let at = periodStart;
  for (let back = 0; back < count; back++) {
    at = schedulePeriodFor(at - 1, options);
    periods.push(at);
  }
  return periods.reverse();
}

/** The id a period's run takes. Its uniqueness is what makes the period fire once. */
export function scheduleRunId(name: string, periodStart: number): string {
  return `${name}@${periodStart}`;
}
