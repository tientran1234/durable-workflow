import type { RetryPolicy } from "./types.js";

export const DEFAULT_RETRY: RetryPolicy = {
  maxAttempts: 3,
  initialDelayMs: 1_000,
  factor: 2,
  maxDelayMs: 60_000,
};

/** Delay before attempt `attempt + 1`, capped. Attempt numbers start at 1. */
export function backoffMs(policy: RetryPolicy, attempt: number): number {
  const raw = policy.initialDelayMs * policy.factor ** (attempt - 1);
  return Math.min(raw, policy.maxDelayMs);
}
