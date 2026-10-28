/**
 * The firing a cron expression names at or before a moment.
 *
 * `every` divides the epoch into equal periods, which is everything a schedule
 * needs until the period has to mean something on a calendar: the first of a
 * month is not a number of milliseconds, nor is the last Friday, nor nine in
 * the morning in a place whose clocks moved in March.
 *
 * What is kept from `every` is the property that makes a schedule idempotent.
 * A firing is derived from the expression and the clock and from nothing else,
 * so every process computes the same instant without speaking, and the run id
 * built from it starts the firing once however many callers reach it.
 */

/**
 * How far back a firing is looked for before the expression is called
 * unsatisfiable. Four years and change, because the shortest real gap between
 * two firings of a legal expression is the leap day's: `0 0 29 2 *` fires once
 * every four years, and anything rarer than that cannot be written down here.
 */
export const CRON_LOOKBACK_DAYS = 1_500;

/**
 * The last firing at or before `now`, as an instant.
 *
 * Firings land on a minute, so the search starts at the minute `now` falls in
 * and walks back a calendar day at a time — a day rather than a fixed span,
 * because in a zone that observes DST two of them are not the same length and
 * it is the wall clock the expression is written about.
 */
export function cronPrevious(now: number, expr: string, tz = "UTC"): number {
  throw new Error(`cronPrevious is not implemented yet: ${expr} in ${tz} at ${now}`);
}
