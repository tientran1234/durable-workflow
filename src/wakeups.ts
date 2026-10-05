import { TERMINAL } from "./due.js";
import type { RunRecord } from "./types.js";

/**
 * Why a worker should look for work: a run nobody is holding is due — now, or
 * at `wakeAt`.
 *
 * Both halves are needed, because the two ways a run becomes due are not the
 * same shape. A signal makes a run due immediately, and the store can say so
 * in the moment it writes it. A timer is the other case: nothing writes the
 * run when its wake time arrives — that write happened when it went to sleep —
 * so the only way to be there on time is to have been told the time in
 * advance.
 */
export interface Wakeup {
  runId: string;
  /** When the run becomes due, or null for "already". */
  wakeAt: number | null;
}

/** A live subscription to a store's wakeups. */
export interface WakeupSubscription {
  /** Stop listening, and release whatever the subscription was holding. */
  close(): Promise<void>;
}

/**
 * A store that can tell a worker a run became due instead of leaving it to
 * find out on its next poll. Optional, and latency only: `engine.worker()`
 * subscribes where the store has it and keeps polling either way, so a wakeup
 * that is never delivered costs one poll interval and nothing else.
 *
 * `watch` may reject if the subscription cannot be established at all — a
 * worker starting against a store it cannot subscribe to is worth hearing
 * about — but losing it later is the store's own to recover from, because by
 * then there is nobody left to hand the error to.
 */
export interface WakeupSource {
  watch(onWake: (wake: Wakeup) => void): Promise<WakeupSubscription>;
}

export function isWakeupSource(store: object): store is WakeupSource {
  return typeof (store as Partial<WakeupSource>).watch === "function";
}

/**
 * The wakeup a run is worth sending after a write, or null if it is not worth
 * one. Stores call this, so what a notification means does not depend on which
 * store sent it.
 *
 * A leased run is somebody's: the worker holding it is already executing it,
 * and the write that clears the lease is the one that becomes a wakeup. A run
 * `waiting` with no deadline is the case a `wakeAt` of null must not be
 * allowed to swallow — it is waiting indefinitely, not due now — so there is
 * nothing to wake for until a signal makes it `running`.
 */
export function wakeupFor(run: RunRecord): Wakeup | null {
  if (TERMINAL.has(run.status) || run.leaseUntil !== null) return null;
  if (run.status === "running") return { runId: run.id, wakeAt: null };
  return run.wakeAt === null ? null : { runId: run.id, wakeAt: run.wakeAt };
}

/** A wakeup as it travels over a channel that carries only text. */
export function encodeWakeup(wake: Wakeup): string {
  return JSON.stringify(wake);
}

/**
 * A wakeup read back off such a channel, or null if that is not what it was.
 * A payload arrives from outside the process and is only ever a reason to look
 * for work, so anything unreadable is dropped rather than raised: the poll
 * interval is the answer to a wakeup that does not arrive.
 */
export function decodeWakeup(payload: string | undefined): Wakeup | null {
  if (!payload) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const { runId, wakeAt } = parsed as Partial<Wakeup>;
  if (typeof runId !== "string") return null;
  if (wakeAt !== null && typeof wakeAt !== "number") return null;
  return { runId, wakeAt };
}

/**
 * How many future wake times one worker keeps. A worker hears about every run
 * written while it is listening and a store may hold millions of sleeping
 * ones, so the soonest are kept and the rest let go: being on time is what
 * this is for, and polling still finds whatever was dropped here.
 */
export const MAX_PENDING_WAKEUPS = 1024;

/**
 * What a worker waits on between passes: the poll interval, cut short as soon
 * as a wakeup says there is something to do — now, or at a wake time that
 * falls inside the interval.
 *
 * One timer at a time, re-armed when a sooner wake time arrives, so a worker
 * carrying a thousand pending wake times still carries one timer. The delay it
 * is armed with is `wakeAt - now()`: a wake time is written on the engine's
 * clock, so the distance to it is measured on that clock, and only the waiting
 * itself is real time.
 */
export class WorkerWait {
  /** A wakeup has arrived that no wait has consumed yet. */
  private due = false;
  /** Future wake times, soonest first and distinct. */
  private readonly pending: number[] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;
  /** The wait in progress, if there is one, and what its timer is set for. */
  private wake: (() => void) | null = null;
  private target = 0;
  private closed = false;

  constructor(private readonly now: () => number) {}

  /** Take what a store said, cutting a wait already in progress short if it should. */
  push(wake: Wakeup): void {
    if (this.closed) return;
    if (wake.wakeAt === null || wake.wakeAt <= this.now()) {
      this.due = true;
      this.end();
      return;
    }
    this.remember(wake.wakeAt);
    if (this.wake !== null && wake.wakeAt < this.target) this.arm(wake.wakeAt);
  }

  /** Resolves once a run is due, or after `pollMs` — whichever comes first. */
  wait(pollMs: number): Promise<void> {
    const now = this.now();
    // A wake time that has arrived is a run to claim rather than a time to
    // wait for. Counting it as due costs one extra pass after a timer fires,
    // and removes the question of whether the pass that woke for a wake time
    // ran before or after the moment it woke for.
    while (this.pending.length > 0 && this.pending[0]! <= now) {
      this.pending.shift();
      this.due = true;
    }
    if (this.due || this.closed) {
      this.due = false;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.wake = resolve;
      const soonest = this.pending[0];
      this.arm(soonest !== undefined && soonest < now + pollMs ? soonest : now + pollMs);
    });
  }

  /** End the wait in progress, and every wait after it. */
  close(): void {
    this.closed = true;
    this.end();
  }

  private remember(at: number): void {
    const next = this.pending.findIndex((p) => p >= at);
    if (next >= 0 && this.pending[next] === at) return;
    if (next < 0) this.pending.push(at);
    else this.pending.splice(next, 0, at);
    if (this.pending.length > MAX_PENDING_WAKEUPS) this.pending.pop();
  }

  private arm(target: number): void {
    if (this.timer !== null) clearTimeout(this.timer);
    this.target = target;
    this.timer = setTimeout(() => this.end(), Math.max(0, target - this.now()));
    // A worker that is only waiting is not a reason to keep the process alive.
    this.timer.unref();
  }

  private end(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    const wake = this.wake;
    this.wake = null;
    wake?.();
  }
}
