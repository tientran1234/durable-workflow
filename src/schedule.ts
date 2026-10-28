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
 * Which schedule this is, when one workflow has more than one. Defaults to the
 * workflow's name. It is part of the run id, so two schedules over one workflow
 * share a period's run unless they are named apart.
 */
interface NamedSchedule {
  name?: string;
}

/** A period off the epoch's grid: the same boundaries in every process. */
export interface EverySchedule extends NamedSchedule {
  /** The period, in milliseconds. */
  every: number;
}

/** A period a calendar decides: the first of a month, the last Friday, 9am local. */
export interface CronSchedule extends NamedSchedule {
  /** `minute hour day-of-month month day-of-week`, with `L` for a month's last. */
  cron: string;
  /** The zone the expression is written in, as an IANA name. Defaults to UTC. */
  tz?: string;
}

export type ScheduleOptions = EverySchedule | CronSchedule;

/** What a call to `engine.schedule` did, or found already done. */
export interface ScheduledRun {
  runId: string;
  /**
   * The period this call belongs to: where it began on an `every` grid, and the
   * firing the expression named at or before the call on a `cron` one.
   */
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

/**
 * The period a call falls in, however the schedule says what a period is. Both
 * answers come from the clock and the schedule alone, which is what lets the
 * loop driving them remember nothing.
 */
export function schedulePeriodFor(now: number, options: ScheduleOptions): number {
  return "cron" in options ? cronPrevious(now, options.cron, options.tz) : schedulePeriod(now, options.every);
}

/** The id a period's run takes. Its uniqueness is what makes the period fire once. */
export function scheduleRunId(name: string, periodStart: number): string {
  return `${name}@${periodStart}`;
}
