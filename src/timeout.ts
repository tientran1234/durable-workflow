import { StepTimeoutError } from "./errors.js";
import type { StepFn } from "./types.js";

/**
 * The signal handed to an attempt that has no `timeoutMs`: nothing aborts it.
 *
 * A fresh controller per attempt rather than one shared signal, because a step
 * that registers an abort listener would otherwise pile its listeners onto a
 * value that outlives every run in the process.
 */
export function neverAborted(): AbortSignal {
  return new AbortController().signal;
}

/**
 * Run `fn` with a wall-clock bound on this attempt, raising StepTimeoutError if
 * it has not settled in time.
 *
 * The bound is a real `setTimeout` rather than the engine's injected clock,
 * which is the one place in the library where that is the right instrument. A
 * durable wake time is only read on a later tick, and the tick is what is stuck:
 * the hang is happening in this process, now, with the worker's lease ticking
 * down. So it is deliberately not durable either — nothing about it survives a
 * restart, because a restart already ends the attempt it was bounding.
 *
 * A promise cannot be interrupted, so the attempt is abandoned rather than
 * stopped — but `fn` is given an `AbortSignal` that is aborted when the bound
 * elapses, which is the one thing that can reach the work from here. A
 * cooperating client (`fetch`, a driver that takes a signal, anything that
 * watches one) therefore stops with the attempt instead of carrying on against
 * a result nobody will read. A step that ignores the signal is exactly where it
 * was: the work may still finish, and whatever it touched on the way is a side
 * effect the retry will produce again — which is why a step with a timeout
 * wants the same idempotency key a step that retries wants.
 */
export async function withTimeout<T>(name: string, timeoutMs: number, fn: StepFn<T>): Promise<T> {
  // One controller per call, so the signal's life is this attempt's: a retry
  // comes back through here and gets a fresh one rather than one already spent.
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      // Called inside the race so a synchronous throw rejects here rather than
      // escaping past the timer that would otherwise be left running.
      (async () => fn(controller.signal))(),
      new Promise<never>((_, reject) => {
        // Not unref'd: if the step never answers, this timer is the only thing
        // left that will move the run on.
        timer = setTimeout(() => {
          const timedOut = new StepTimeoutError(name, timeoutMs);
          // Reject before aborting. A cancelled client rejects with the reason
          // it was aborted with, and the race reports whichever settles first;
          // rejecting first makes that this error every time, so the attempt
          // records the bound it ran past rather than whatever the client made
          // of being cancelled — or nothing at all, from a step that ignores
          // the signal. The abort still reaches the work: race has already
          // settled, but nothing here depends on that promise again.
          reject(timedOut);
          controller.abort(timedOut);
        }, timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
