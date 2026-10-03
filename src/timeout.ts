import { StepTimeoutError } from "./errors.js";

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
 * Nothing cancels `fn`. A promise cannot be interrupted, so the work is
 * abandoned rather than stopped: it may still finish, and whatever it touched on
 * the way is a side effect the retry will produce again. That is the exposure a
 * lease expiry already has — the difference is that the retry is now recorded,
 * counted against the step's policy, and taken by the worker that still holds
 * the run.
 */
export async function withTimeout<T>(name: string, timeoutMs: number, fn: () => Promise<T> | T): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      // Called inside the race so a synchronous throw rejects here rather than
      // escaping past the timer that would otherwise be left running.
      (async () => fn())(),
      new Promise<never>((_, reject) => {
        // Not unref'd: if the step never answers, this timer is the only thing
        // left that will move the run on.
        timer = setTimeout(() => reject(new StepTimeoutError(name, timeoutMs)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
