import type { SignalDefinition } from "./signals.js";
import { DEFAULT_VERSION } from "./versions.js";

export type RunStatus = "running" | "sleeping" | "waiting" | "completed" | "failed" | "canceled" | "continued";

/**
 * The append-only record of everything a run has decided. Replay reads it;
 * execution appends to it. `call` is the position of the ctx.* call that
 * produced the event, which is what lets a re-executed workflow function find
 * its own past.
 *
 * A compensation records the same pair a step does, under positions above the
 * call the run failed at: it is a durable unit like any other, and keeping it a
 * type of its own is what stops an undo reading as the work it undid.
 */
export type HistoryEvent =
  | { seq: number; call: number; type: "step.completed"; name: string; result: unknown; at: number }
  | { seq: number; call: number; type: "step.failed"; name: string; attempt: number; error: string; retryAt?: number; at: number }
  | { seq: number; call: number; type: "compensation.completed"; name: string; at: number }
  | { seq: number; call: number; type: "compensation.failed"; name: string; attempt: number; error: string; retryAt?: number; at: number }
  | { seq: number; call: number; type: "signal.received"; name: string; payload: unknown; at: number }
  | { seq: number; call: number; type: "signal.timeout"; name: string; at: number }
  | { seq: number; call: number; type: "timer.fired"; name: string; at: number }
  | { seq: number; call: number; type: "child.started"; name: string; childRunId: string; at: number };

/**
 * The settled prefix of a run's history, folded into one event per ctx call and
 * indexed by position. Replay reads a settled call from here by position
 * instead of scanning history for it, so a long run's ticks stop getting
 * slower as its history grows.
 */
export interface HistorySnapshot {
  /** Calls 0..calls-1 are settled by `events`; history holds only what is above them. */
  calls: number;
  /** The settling event for each call below `calls`, in position order. */
  events: HistoryEvent[];
  /** The seq the next appended event takes, since history no longer counts from zero. */
  nextSeq: number;
  /** Superseded retry attempts compaction dropped. Audit only; replay never needed them. */
  droppedEvents: number;
  at: number;
}

/** Omit that distributes over a union — plain Omit collapses HistoryEvent to its common keys. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

/** A history event before the engine assigns `seq` and `at`. */
export type NewEvent = DistributiveOmit<HistoryEvent, "seq" | "at">;

/**
 * A signal the engine refused because its payload failed the signal's schema.
 * Nothing was delivered and nothing was buffered, so this is the only record
 * that it was ever sent.
 *
 * The payload is deliberately not kept. It is by definition not a shape
 * anything here understands, and it came from outside the process, so what is
 * worth recording is which signal was refused and why.
 */
export interface RejectedSignal {
  name: string;
  /** What the schema said when it refused the payload. */
  error: string;
  at: number;
}

export interface RunRecord {
  id: string;
  workflow: string;
  /**
   * The workflow version this run started on. It replays on that version's
   * code for its whole life, so a deploy cannot change the meaning of a run
   * that is already in flight. Absent on runs created before versions existed.
   */
  workflowVersion?: number;
  input: unknown;
  /** Set when ctx.startChild created this run: where to deliver its outcome. */
  parent: { runId: string; signal: string } | null;
  /**
   * Where this run sits in a continuation chain, when ctx.continueAsNew
   * created it. Absent on a run that was started directly, which is
   * generation 1 of its own chain.
   */
  chain?: { root: string; generation: number };
  /**
   * Set when this run ended by continuing: the successor that carries the work
   * on. The run is terminal, but the chain is not finished, which is why a
   * signal or a cancellation addressed to it is forwarded to the successor.
   */
  continuation?: { runId: string };
  status: RunStatus;
  history: HistoryEvent[];
  /**
   * The folded prefix of that history. Absent until the run's history first
   * grows past the engine's `compactAfter`, and on runs written before
   * compaction existed.
   */
  snapshot?: HistorySnapshot;
  /** Signals that arrived before the workflow reached the matching waitFor. */
  pendingSignals: Record<string, unknown[]>;
  /**
   * Payloads the engine refused, newest first and capped at
   * MAX_REJECTED_SIGNALS. Absent until a run is sent one, which is most runs.
   */
  rejectedSignals?: RejectedSignal[];
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

/**
 * An undo registered by ctx.compensate, as the engine holds it. `fn` is a plain
 * function rather than a workflow: it runs inside a durable unit of its own, so
 * it must not call ctx.* itself.
 */
export interface Compensation {
  name: string;
  fn: () => Promise<unknown> | unknown;
  options?: StepOptions;
}

/**
 * A child run as its parent addresses it. Everything in it is derived from the
 * parent's run id and the position of the startChild call, so the same handle
 * comes back on every replay.
 */
export interface ChildHandle<Output = unknown> {
  runId: string;
  workflow: string;
  /** The signal the engine delivers to the parent when the child finishes. */
  signal: string;
  /** Phantom: carries the child's output type to waitForChild. Never present at runtime. */
  readonly __output?: Output;
}

/** The payload of a child's completion signal. */
export type ChildOutcome =
  | { status: "completed"; output: unknown }
  | { status: "failed" | "canceled"; error: string };

export interface WorkflowContext<Input> {
  readonly runId: string;
  readonly input: Input;
  /**
   * Run `fn` at most once per run. On replay a completed step returns its
   * stored result without calling `fn`. Results must be JSON-serialisable.
   */
  step<T>(name: string, fn: () => Promise<T> | T, options?: StepOptions): Promise<T>;
  /**
   * Register an undo for what was just done — the step above this call. If the
   * run goes on to fail, every undo registered by then runs, newest first, each
   * as a durable step of its own with its own retries.
   *
   * Registering is not durable and records nothing: replay re-registers by
   * re-executing, so the undos that exist are exactly the ones this pass
   * reached. It therefore does not suspend, which is why it returns nothing to
   * await.
   */
  compensate(name: string, fn: () => Promise<unknown> | unknown, options?: StepOptions): void;
  /**
   * Suspend until `engine.signal(runId, signal, payload)` — or until
   * `timeoutMs`, which throws WaitTimeoutError.
   *
   * Waiting on a definition is what makes the payload typed rather than cast:
   * the engine checked it against that same schema on the way in, so the
   * schema's output type is what arrives here.
   */
  waitFor<T>(signal: SignalDefinition<T>, options?: { timeoutMs?: number }): Promise<T>;
  waitFor<T = unknown>(name: string, options?: { timeoutMs?: number }): Promise<T>;
  /** A durable timer: survives restarts, costs nothing while pending. */
  sleep(name: string, ms: number): Promise<void>;
  /**
   * End this run and hand the work to a fresh run of the same workflow with
   * `input` — an empty history, so replay stops paying for what is already
   * settled. Never returns: it unwinds the function the way waitFor does.
   *
   * The successor starts on the newest registered version, and anything still
   * addressing this run (a signal, a cancellation, the parent that is waiting
   * on it) reaches the successor instead.
   */
  continueAsNew(input: Input): Promise<never>;
  /**
   * Start `workflow` as an independent run, at most once per parent run, and
   * return a handle to wait on. The child runs on its own; this does not block.
   */
  startChild<ChildInput, ChildOutput>(
    workflow: WorkflowDefinition<ChildInput, ChildOutput> | string,
    input: ChildInput,
  ): Promise<ChildHandle<ChildOutput>>;
  /**
   * Suspend until the child finishes. Returns its output, or throws
   * ChildFailedError if it failed or was canceled — and WaitTimeoutError if
   * `timeoutMs` elapses first.
   */
  waitForChild<Output>(handle: ChildHandle<Output>, options?: { timeoutMs?: number }): Promise<Output>;
}

export interface WorkflowDefinition<Input = unknown, Output = unknown> {
  name: string;
  /** Which revision of this code it is. Runs pin it; see DEFAULT_VERSION. */
  version: number;
  run: (ctx: WorkflowContext<Input>, input: Input) => Promise<Output>;
}

export interface WorkflowOptions {
  /**
   * Bump this when a change would make live runs replay differently — a
   * renamed or reordered ctx call, a branch that skips one. Both versions stay
   * registered; runs already in flight keep replaying the old one.
   */
  version?: number;
}

export function defineWorkflow<Input, Output>(
  name: string,
  run: (ctx: WorkflowContext<Input>, input: Input) => Promise<Output>,
  options: WorkflowOptions = {},
): WorkflowDefinition<Input, Output> {
  return { name, version: options.version ?? DEFAULT_VERSION, run };
}

export interface RunQuery {
  workflow?: string;
  status?: RunStatus;
  /** Page size. Defaults to 50, capped at 500. */
  limit?: number;
  /** Opaque position from the previous page's `cursor`. */
  cursor?: string;
}

export interface RunPage {
  runs: RunRecord[];
  /** Pass back as `cursor` for the next page, or null when this was the last one. */
  cursor: string | null;
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
  /**
   * A page of runs matching `query`, ordered by (createdAt, id) descending.
   * The order is part of the contract: it is what lets a cursor name an exact
   * position instead of an offset that shifts as new runs are created.
   */
  list(query: RunQuery): Promise<RunPage>;
}
