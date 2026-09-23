import { TERMINAL } from "./due.js";
import type { HistoryEvent, RunRecord, RunStatus } from "./types.js";

/** One history event as a line on a timeline. */
export interface TimelineEntry {
  seq: number;
  at: number;
  /** Milliseconds since the run started — a timeline is read by offset, not by epoch. */
  elapsedMs: number;
  /** Position of the ctx.* call that produced the event. */
  call: number;
  type: HistoryEvent["type"];
  name: string;
  summary: string;
}

/** What the run is blocked on right now. */
export interface RunBlockedOn {
  kind: "signal" | "timer" | "retry";
  name: string;
  /** When the wait ends on its own: a timeout, a wake time. Null for an open-ended waitFor. */
  until: number | null;
}

/**
 * A run rendered for an admin screen: plain JSON, no store access, nothing the
 * caller has to derive from history itself.
 */
export interface RunView {
  id: string;
  workflow: string;
  status: RunStatus;
  createdAt: number;
  updatedAt: number;
  /** Start to last persisted change. For a live run this grows with every tick. */
  durationMs: number;
  input: unknown;
  output: unknown;
  error: string | null;
  blockedOn: RunBlockedOn | null;
  /** Signals buffered ahead of their waitFor, by name, with how many payloads each. */
  pendingSignals: Record<string, number>;
  timeline: TimelineEntry[];
}

export function renderRun(run: RunRecord): RunView {
  return {
    id: run.id,
    workflow: run.workflow,
    status: run.status,
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
    durationMs: run.updatedAt - run.createdAt,
    input: run.input,
    output: run.output,
    error: run.error,
    blockedOn: blockedOn(run),
    pendingSignals: Object.fromEntries(
      Object.entries(run.pendingSignals).map(([name, payloads]) => [name, payloads.length]),
    ),
    timeline: [...run.history]
      .sort((a, b) => a.seq - b.seq)
      .map((event) => ({
        seq: event.seq,
        at: event.at,
        elapsedMs: event.at - run.createdAt,
        call: event.call,
        type: event.type,
        name: event.name,
        summary: summarise(event),
      })),
  };
}

function blockedOn(run: RunRecord): RunBlockedOn | null {
  if (TERMINAL.has(run.status)) return null;
  if (run.status === "waiting" && run.waitingFor) {
    return { kind: "signal", name: run.waitingFor.name, until: run.wakeAt };
  }
  if (run.status === "sleeping") {
    if (run.pendingTimer) return { kind: "timer", name: run.pendingTimer.name, until: run.wakeAt };
    // Sleeping with no timer means a step is serving out its retry backoff,
    // which lives in history rather than on the record.
    const failed = [...run.history].reverse().find((e) => e.type === "step.failed" && e.retryAt !== undefined);
    if (failed) return { kind: "retry", name: failed.name, until: run.wakeAt };
  }
  return null;
}

function summarise(event: HistoryEvent): string {
  switch (event.type) {
    case "step.completed":
      return `step "${event.name}" completed`;
    case "step.failed":
      return event.retryAt === undefined
        ? `step "${event.name}" failed on attempt ${event.attempt}, no attempts left: ${event.error}`
        : `step "${event.name}" failed on attempt ${event.attempt}, retrying: ${event.error}`;
    case "signal.received":
      return `signal "${event.name}" received`;
    case "signal.timeout":
      return `waitFor "${event.name}" timed out`;
    case "timer.fired":
      return `timer "${event.name}" fired`;
  }
}
