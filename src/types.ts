export type RunStatus = "running" | "sleeping" | "waiting" | "completed" | "failed" | "canceled";

/**
 * The append-only record of everything a run has decided. Replay reads it;
 * execution appends to it. `call` is the position of the ctx.* call that
 * produced the event, which is what lets a re-executed workflow function find
 * its own past.
 */
export type HistoryEvent =
  | { seq: number; call: number; type: "step.completed"; name: string; result: unknown; at: number }
  | { seq: number; call: number; type: "step.failed"; name: string; attempt: number; error: string; retryAt?: number; at: number }
  | { seq: number; call: number; type: "signal.received"; name: string; payload: unknown; at: number }
  | { seq: number; call: number; type: "signal.timeout"; name: string; at: number }
  | { seq: number; call: number; type: "timer.fired"; name: string; at: number };

/** Omit that distributes over a union — plain Omit collapses HistoryEvent to its common keys. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

/** A history event before the engine assigns `seq` and `at`. */
export type NewEvent = DistributiveOmit<HistoryEvent, "seq" | "at">;

export interface RunRecord {
  id: string;
  workflow: string;
  input: unknown;
  status: RunStatus;
  history: HistoryEvent[];
  /** Signals that arrived before the workflow reached the matching waitFor. */
  pendingSignals: Record<string, unknown[]>;
  /** sleeping: when to wake. waiting: the deadline, or null for no timeout. */
  wakeAt: number | null;
  waitingFor: { name: string; call: number } | null;
  pendingTimer: { name: string; call: number } | null;
  output: unknown;
  error: string | null;
  /** Optimistic-concurrency counter. Every persisted change increments it. */
  version: number;
  leaseUntil: number | null;
  createdAt: number;
  updatedAt: number;
}

export interface RetryPolicy {
  maxAttempts: number;
  initialDelayMs: number;
  factor: number;
  maxDelayMs: number;
}

export interface StepOptions {
  retry?: Partial<RetryPolicy>;
}

export interface WorkflowContext<Input> {
  readonly runId: string;
  readonly input: Input;
  /**
   * Run `fn` at most once per run. On replay a completed step returns its
   * stored result without calling `fn`. Results must be JSON-serialisable.
   */
  step<T>(name: string, fn: () => Promise<T> | T, options?: StepOptions): Promise<T>;
  /** Suspend until `engine.signal(runId, name, payload)` — or until `timeoutMs`, which throws WaitTimeoutError. */
  waitFor<T = unknown>(name: string, options?: { timeoutMs?: number }): Promise<T>;
  /** A durable timer: survives restarts, costs nothing while pending. */
  sleep(name: string, ms: number): Promise<void>;
}

export interface WorkflowDefinition<Input = unknown, Output = unknown> {
  name: string;
  run: (ctx: WorkflowContext<Input>, input: Input) => Promise<Output>;
}

export function defineWorkflow<Input, Output>(
  name: string,
  run: (ctx: WorkflowContext<Input>, input: Input) => Promise<Output>,
): WorkflowDefinition<Input, Output> {
  return { name, run };
}

export interface RunStore {
  create(run: RunRecord): Promise<void>;
  get(id: string): Promise<RunRecord | null>;
  /**
   * Persist `run` only if its stored version still equals `expectedVersion`.
   * On success the store bumps `run.version`. Returns false on conflict.
   */
  save(run: RunRecord, expectedVersion: number): Promise<boolean>;
  /**
   * Atomically lease up to `limit` runs that are due at `now` and not held by
   * another worker. Leased runs come back with `leaseUntil` and `version` updated.
   */
  claimDue(now: number, leaseMs: number, limit: number): Promise<RunRecord[]>;
}
