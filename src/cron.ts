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

/** Where a field sits in the expression, in the order cron reads them. */
const FIELDS = [
  { name: "minute", min: 0, max: 59 },
  { name: "hour", min: 0, max: 23 },
  { name: "day-of-month", min: 1, max: 31 },
  { name: "month", min: 1, max: 12 },
  { name: "day-of-week", min: 0, max: 6 },
] as const;

const MONTH_NAMES = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
const DAY_NAMES = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

/**
 * A parsed expression. The day fields keep `null` for a bare `*` rather than
 * the full set, because cron's oldest rule needs the distinction: a day matches
 * either day field when both are restricted, and only the restricted one when
 * one of them is not. `0 0 13 * 5` is the thirteenth *or* a Friday.
 */
interface CronFields {
  minutes: Set<number>;
  hours: Set<number>;
  daysOfMonth: Set<number> | null;
  /** `L` in the day-of-month field: whichever day that month ends on. */
  lastDayOfMonth: boolean;
  months: Set<number>;
  daysOfWeek: Set<number> | null;
  /** `5L` in the day-of-week field: the last Friday of the month, not every one. */
  lastWeekdays: Set<number>;
}

/** A wall-clock reading in some zone. What a cron expression is written about. */
interface Wall {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
}

function parseCron(expr: string): CronFields {
  const parts = expr.trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new RangeError(`cron expression must have 5 fields (minute hour day-of-month month day-of-week), got "${expr}"`);
  }
  const [minute = "", hour = "", monthDay = "", month = "", weekDay = ""] = parts;

  const byMonthDay = parseDayOfMonth(monthDay, expr);
  const byWeekDay = parseDayOfWeek(weekDay, expr);
  return {
    minutes: whole(parseField(minute, FIELDS[0], expr), FIELDS[0]),
    hours: whole(parseField(hour, FIELDS[1], expr), FIELDS[1]),
    daysOfMonth: byMonthDay.days,
    lastDayOfMonth: byMonthDay.last,
    months: whole(parseField(month, FIELDS[3], expr, MONTH_NAMES), FIELDS[3]),
    daysOfWeek: byWeekDay.days,
    lastWeekdays: byWeekDay.lastWeekdays,
  };
}

/**
 * The last firing at or before `now`, as an instant.
 *
 * Firings land on a minute, so the search starts at the minute `now` falls in
 * and walks back a calendar day at a time — a day rather than a fixed span,
 * because in a zone that observes DST two of them are not the same length and
 * it is the wall clock the expression is written about.
 */
export function cronPrevious(now: number, expr: string, tz = "UTC"): number {
  const fields = parseCron(expr);
  const hours = [...fields.hours].sort((a, b) => b - a);
  const minutes = [...fields.minutes].sort((a, b) => b - a);

  const from = zonedParts(tz, now);
  let { year, month, day } = from;
  let ceiling = from.hour * 60 + from.minute;

  for (let back = 0; back <= CRON_LOOKBACK_DAYS; back++) {
    if (fields.months.has(month) && dayMatches(fields, year, month, day)) {
      for (const hour of hours) {
        for (const minute of minutes) {
          if (hour * 60 + minute > ceiling) continue;
          const at = zonedEpoch(tz, { year, month, day, hour, minute });
          // A reading the zone does not have, or one that lands after `now`
          // anyway: neither is this day's firing, and an earlier one may be.
          if (at !== null && at <= now) return at;
        }
      }
    }
    ({ year, month, day } = previousDay(year, month, day));
    ceiling = 23 * 60 + 59;
  }

  throw new RangeError(`cron expression "${expr}" has no firing in the ${CRON_LOOKBACK_DAYS} days before ${new Date(now).toISOString()}`);
}

/** Cron's either-or rule over the two day fields, plus the two `L` forms. */
function dayMatches(fields: CronFields, year: number, month: number, day: number): boolean {
  const last = daysInMonth(year, month);
  const byMonth = fields.daysOfMonth !== null || fields.lastDayOfMonth;
  const byWeek = fields.daysOfWeek !== null || fields.lastWeekdays.size > 0;

  const weekday = dayOfWeek(year, month, day);
  const monthHit = (fields.daysOfMonth?.has(day) ?? false) || (fields.lastDayOfMonth && day === last);
  const weekHit =
    (fields.daysOfWeek?.has(weekday) ?? false) || (fields.lastWeekdays.has(weekday) && day + 7 > last);

  if (byMonth && byWeek) return monthHit || weekHit;
  if (byMonth) return monthHit;
  if (byWeek) return weekHit;
  return true;
}

function parseDayOfMonth(spec: string, expr: string): { days: Set<number> | null; last: boolean } {
  const terms = spec.split(",");
  const kept = terms.filter((term) => term.toUpperCase() !== "L");
  const last = kept.length !== terms.length;
  if (kept.length === 0) return { days: null, last };
  return { days: parseField(kept.join(","), FIELDS[2], expr), last };
}

function parseDayOfWeek(spec: string, expr: string): { days: Set<number> | null; lastWeekdays: Set<number> } {
  const lastWeekdays = new Set<number>();
  const kept: string[] = [];
  for (const term of spec.split(",")) {
    const match = /^(.+)L$/i.exec(term);
    if (match) {
      const one = parseField(match[1] ?? "", FIELDS[4], expr, DAY_NAMES);
      const [only] = one ?? [];
      if (one?.size !== 1 || only === undefined) {
        throw new RangeError(`"${term}" in cron expression "${expr}" must name one day of the week`);
      }
      lastWeekdays.add(only);
    } else {
      kept.push(term);
    }
  }
  if (kept.length === 0) return { days: null, lastWeekdays };
  return { days: parseField(kept.join(","), FIELDS[4], expr, DAY_NAMES), lastWeekdays };
}

/** `null` for a field that restricts nothing, which only the day fields read. */
function parseField(
  spec: string,
  field: (typeof FIELDS)[number],
  expr: string,
  names?: string[],
): Set<number> | null {
  if (spec === "*" || spec === "?") return null;

  const values = new Set<number>();
  for (const term of spec.split(",")) {
    const [range = "", step] = term.split("/");
    if (step !== undefined && !/^\d+$/.test(step)) {
      throw new RangeError(`step "${step}" in the ${field.name} field of cron expression "${expr}" is not a number`);
    }
    const by = step === undefined ? 1 : Number(step);
    if (by === 0) throw new RangeError(`step in the ${field.name} field of cron expression "${expr}" must be above zero`);

    const [from, to] = bounds(range, field, expr, step !== undefined, names);
    if (from > to) {
      throw new RangeError(`range "${range}" in the ${field.name} field of cron expression "${expr}" runs backwards`);
    }
    for (let value = from; value <= to; value += by) values.add(value);
  }
  return values;
}

function bounds(
  range: string,
  field: (typeof FIELDS)[number],
  expr: string,
  stepped: boolean,
  names?: string[],
): [number, number] {
  if (range === "*" || range === "") return [field.min, field.max];
  const [low = "", high] = range.split("-");
  if (range.split("-").length > 2) {
    throw new RangeError(`range "${range}" in the ${field.name} field of cron expression "${expr}" is not a range`);
  }
  const from = value(low, field, expr, names);
  // `9-17/4` stops where its range does; `30/15` is cron's other reading of a
  // step, where a bare value is where the count starts rather than all of it.
  if (high === undefined) return stepped ? [from, field.max] : [from, from];
  return [from, value(high, field, expr, names)];
}

function value(token: string, field: (typeof FIELDS)[number], expr: string, names?: string[]): number {
  const named = names?.indexOf(token.toLowerCase()) ?? -1;
  // Sunday is both 0 and 7 in every cron there has ever been.
  const raw = named >= 0 ? named + field.min : Number(token);
  const n = field.name === "day-of-week" && raw === 7 ? 0 : raw;
  if (!/^\d+$/.test(token) && named < 0) {
    throw new RangeError(`"${token}" in the ${field.name} field of cron expression "${expr}" is not a value`);
  }
  if (n < field.min || n > field.max) {
    throw new RangeError(`"${token}" in the ${field.name} field of cron expression "${expr}" is outside ${field.min}-${field.max}`);
  }
  return n;
}

/** `*` in a field that has no either-or rule to keep it for: the whole range. */
function whole(values: Set<number> | null, field: (typeof FIELDS)[number]): Set<number> {
  if (values) return values;
  const all = new Set<number>();
  for (let value = field.min; value <= field.max; value++) all.add(value);
  return all;
}

const FORMATTERS = new Map<string, Intl.DateTimeFormat>();

/**
 * Keyed by zone and kept, because the search reads one per day it walks back
 * and building a formatter costs more than the day it is used for. Zones come
 * from a schedule in the caller's code, so the map is as big as their list of
 * them.
 */
function formatter(tz: string): Intl.DateTimeFormat {
  const held = FORMATTERS.get(tz);
  if (held) return held;
  let made: Intl.DateTimeFormat;
  try {
    made = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      hourCycle: "h23",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    throw new RangeError(`unknown time zone "${tz}"`);
  }
  FORMATTERS.set(tz, made);
  return made;
}

function zonedParts(tz: string, epoch: number): Wall {
  const parts = formatter(tz).formatToParts(new Date(epoch));
  const read = (type: string) => Number(parts.find((p) => p.type === type)?.value);
  return {
    year: read("year"),
    month: read("month"),
    day: read("day"),
    hour: read("hour"),
    minute: read("minute"),
  };
}

/**
 * The instant a wall-clock reading names in `tz`, or `null` when it names
 * none: the hour a spring-forward skips is on the calendar and not on that
 * day, so a schedule set inside it does not fire that day rather than firing
 * at some neighbouring time nobody asked for.
 *
 * Where a fall-back gives a reading twice, this is the first of the two, which
 * is the one at or before every other candidate and so the one a search going
 * backwards should settle on.
 */
function zonedEpoch(tz: string, wall: Wall): number | null {
  const naive = Date.UTC(wall.year, wall.month - 1, wall.day, wall.hour, wall.minute);
  // The first offset is the one in force at the wrong instant; reading it again
  // at the instant that offset implies is what corrects a guess that landed on
  // the far side of a transition.
  const once = naive - zoneOffset(tz, naive);
  const epoch = naive - zoneOffset(tz, once);
  const back = zonedParts(tz, epoch);
  const same =
    back.year === wall.year &&
    back.month === wall.month &&
    back.day === wall.day &&
    back.hour === wall.hour &&
    back.minute === wall.minute;
  return same ? epoch : null;
}

/** How far ahead of UTC the zone is at this instant, in milliseconds. */
function zoneOffset(tz: string, epoch: number): number {
  const wall = zonedParts(tz, epoch);
  const asUtc = Date.UTC(wall.year, wall.month - 1, wall.day, wall.hour, wall.minute);
  return asUtc - Math.floor(epoch / 60_000) * 60_000;
}

function previousDay(year: number, month: number, day: number): Wall & { hour: 0; minute: 0 } {
  if (day > 1) return { year, month, day: day - 1, hour: 0, minute: 0 };
  const earlier = month === 1 ? { year: year - 1, month: 12 } : { year, month: month - 1 };
  return { ...earlier, day: daysInMonth(earlier.year, earlier.month), hour: 0, minute: 0 };
}

function daysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

/** The weekday of a civil date, which no zone has an opinion about. */
function dayOfWeek(year: number, month: number, day: number): number {
  return new Date(Date.UTC(year, month - 1, day)).getUTCDay();
}
