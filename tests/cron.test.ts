import { describe, expect, it } from "vitest";
import { CRON_LOOKBACK_DAYS, cronPrevious } from "../src/index.js";

/** The instant an ISO reading names, and the reading an instant is. */
const at = (iso: string) => Date.parse(iso);
const fired = (ms: number) => new Date(ms).toISOString();

describe("cron firings", () => {
  it("answers with the last firing at or before the moment asked about", () => {
    expect(fired(cronPrevious(at("2026-10-28T11:34:12.500Z"), "0 * * * *"))).toBe("2026-10-28T11:00:00.000Z");
    expect(fired(cronPrevious(at("2026-10-28T08:00:00Z"), "0 9 * * *"))).toBe("2026-10-27T09:00:00.000Z");
  });

  it("counts a moment that is itself a firing as that firing", () => {
    // The loop calls at the top of the hour as often as it calls anywhere else,
    // and the firing it is standing on is the one whose run it should start.
    expect(fired(cronPrevious(at("2026-10-28T09:00:00Z"), "0 9 * * *"))).toBe("2026-10-28T09:00:00.000Z");
  });

  it("reads lists, ranges, steps and names in every field", () => {
    const twelveFifteen = at("2026-10-28T12:15:00Z"); // a Wednesday
    expect(fired(cronPrevious(twelveFifteen, "*/15 9-17 * jan-dec mon-fri"))).toBe("2026-10-28T12:15:00.000Z");
    expect(fired(cronPrevious(twelveFifteen, "0,30 * * * *"))).toBe("2026-10-28T12:00:00.000Z");
    // A step on a bare value counts from it, which is cron's other reading of `/`.
    expect(fired(cronPrevious(twelveFifteen, "40/15 * * * *"))).toBe("2026-10-28T11:55:00.000Z");
    // Sunday is 0 and 7 in every cron there has ever been.
    expect(cronPrevious(twelveFifteen, "0 0 * * 0")).toBe(cronPrevious(twelveFifteen, "0 0 * * 7"));
  });

  it("stays outside the office when the range says to", () => {
    // 9-17 is nine firings, not a window: the one before 09:00 is yesterday's.
    expect(fired(cronPrevious(at("2026-10-28T08:30:00Z"), "0 9-17 * * *"))).toBe("2026-10-27T17:00:00.000Z");
  });

  it("matches either day field when both of them are restricted", () => {
    // Cron's oldest rule, and the reason a bare `*` is not the full set: the
    // thirteenth *or* a Friday, so the Friday before the 28th is the answer.
    expect(fired(cronPrevious(at("2026-10-28T12:00:00Z"), "0 0 13 * 5"))).toBe("2026-10-23T00:00:00.000Z");
    // Restrict one of them and only that one is read: the 13th, a Tuesday.
    expect(fired(cronPrevious(at("2026-10-28T12:00:00Z"), "0 0 13 * *"))).toBe("2026-10-13T00:00:00.000Z");
  });

  it("says the things a period cannot: a month's last day, and its last Friday", () => {
    // What `every` has no way to write down, which is why this is here at all.
    expect(fired(cronPrevious(at("2026-11-02T00:00:00Z"), "30 23 L * *"))).toBe("2026-10-31T23:30:00.000Z");
    expect(fired(cronPrevious(at("2026-11-02T00:00:00Z"), "0 17 * * 5L"))).toBe("2026-10-30T17:00:00.000Z");
    // The last Friday, not every Friday: the 23rd is one and is not the answer
    // for a call made on the 26th.
    expect(fired(cronPrevious(at("2026-10-26T00:00:00Z"), "0 17 * * 5L"))).toBe("2026-09-25T17:00:00.000Z");
  });

  it("holds a wall-clock hour across a zone's offset changing under it", () => {
    // The whole point of a zone: nine in the morning is nine in the morning in
    // March and in July, and the instants it names are an hour apart.
    expect(fired(cronPrevious(at("2026-01-20T20:00:00Z"), "0 9 * * *", "America/New_York"))).toBe(
      "2026-01-20T14:00:00.000Z",
    );
    expect(fired(cronPrevious(at("2026-07-20T20:00:00Z"), "0 9 * * *", "America/New_York"))).toBe(
      "2026-07-20T13:00:00.000Z",
    );
  });

  it("skips the day a spring-forward leaves the firing's reading off the clock", () => {
    // 02:30 does not happen in New York on 8 March 2026. A schedule set to it
    // misses that day rather than firing at an hour nobody wrote down — and the
    // day before is 02:30 EST, which is 07:30 UTC.
    expect(fired(cronPrevious(at("2026-03-08T12:00:00Z"), "30 2 * * *", "America/New_York"))).toBe(
      "2026-03-07T07:30:00.000Z",
    );
  });

  it("fires once on the reading a fall-back gives twice", () => {
    // 01:30 happens twice in New York on 1 November 2026, an hour apart. The
    // first of them is the firing, so the second is inside a period that has
    // already run and starts nothing.
    const during = cronPrevious(at("2026-11-01T06:45:00Z"), "30 1 * * *", "America/New_York");
    expect(fired(during)).toBe("2026-11-01T05:30:00.000Z");
    expect(cronPrevious(at("2026-11-01T05:45:00Z"), "30 1 * * *", "America/New_York")).toBe(during);
  });

  it("walks back past a year and a leap day to find a rare firing", () => {
    expect(fired(cronPrevious(at("2026-10-28T12:00:00Z"), "0 0 29 2 *"))).toBe("2024-02-29T00:00:00.000Z");
  });

  it("refuses an expression no calendar satisfies rather than searching forever", () => {
    // 30 February: the search has to stop somewhere, and a schedule that has
    // never fired is a deploy to fix and not a run to start.
    expect(() => cronPrevious(at("2026-10-28T12:00:00Z"), "0 0 30 2 *")).toThrow(
      new RegExp(`no firing in the ${CRON_LOOKBACK_DAYS} days`),
    );
  });

  it("refuses an expression it cannot read, naming the field", () => {
    const now = at("2026-10-28T12:00:00Z");
    expect(() => cronPrevious(now, "0 9 * *")).toThrow(/must have 5 fields/);
    expect(() => cronPrevious(now, "0 99 * * *")).toThrow(/hour field.*outside 0-23/);
    expect(() => cronPrevious(now, "0 9 * * funday")).toThrow(/day-of-week field/);
    expect(() => cronPrevious(now, "17-9 9 * * *")).toThrow(/runs backwards/);
    expect(() => cronPrevious(now, "*/0 9 * * *")).toThrow(/must be above zero/);
    expect(() => cronPrevious(now, "0 9 * * *", "Mars/Olympus")).toThrow(/unknown time zone/);
  });
});
