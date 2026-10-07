import { historyEvents } from "./compaction.js";
import { chainRoot, runGeneration } from "./continuation.js";
import { TERMINAL } from "./due.js";
import type { HistoryEvent, RejectedSignal, RunRecord, RunStatus } from "./types.js";
import { runVersion } from "./versions.js";

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
  /** The version this run replays on — which code an operator is looking at. */
  workflowVersion: number;
  status: RunStatus;
  /**
   * Which generation of a continuation chain this run is, and the id the chain
   * started under — so an operator handed any id can find the rest of it.
   */
  chain: { root: string; generation: number };
  /** Set when the run ended by continuing: the generation the work went to. */
  continuation: { runId: string } | null;
  createdAt: number;
  updatedAt: number;
  /** Start to last persisted change. For a live run this grows with every tick. */
  durationMs: number;
  input: unknown;
  output: unknown;
  error: string | null;
  blockedOn: RunBlockedOn | null;
  /** The application's names for this run, as `list({ tag })` matches them. */
  tags: string[];
  /** Signals buffered ahead of their waitFor, by name, with how many payloads each. */
  pendingSignals: Record<string, number>;
  /**
   * Payloads a schema refused, newest first. A refused signal never reached
   * history, so the timeline below says nothing about it — which is the point:
   * a run that looks like it is still waiting for a signal somebody insists
   * they sent is explained here and nowhere else.
   */
  rejectedSignals: RejectedSignal[];
  /**
   * Set once the run's history has been compacted, which is also the caveat on
   * the timeline below: `droppedEvents` retry attempts within the first `calls`
   * positions are no longer on record.
   */
  compaction: { calls: number; droppedEvents: number; at: number } | null;
  timeline: TimelineEntry[];
}

/**
 * One history event on a chain's timeline: the line a single run's timeline
 * carries, plus which generation it came off.
 */
export interface ChainTimelineEntry extends TimelineEntry {
  generation: number;
  runId: string;
}

/**
 * A continuation chain rendered as the one piece of work it is: every
 * generation's view, oldest first, and their timelines on a single axis.
 *
 * The fields a chain has in common with a run answer about the chain rather
 * than about any generation of it — the input it was started with, the outcome
 * the last generation reported, the whole elapsed time — because that is the
 * question an operator holding an order id is asking.
 */
export interface ChainView {
  /** The id the chain started under, which every generation's id derives from. */
  root: string;
  /** The generation carrying the work now: the run `signal` and `cancel` reach. */
  live: string;
  workflow: string;
  /** How many generations the chain has reached so far. */
  generations: number;
  /**
   * The live generation's status. `continued` cannot appear here: a run with a
   * successor is not the live one.
   */
  status: RunStatus;
  /** What the chain was started with — the root's input, not the live generation's. */
  input: unknown;
  /** The outcome the chain reported, which is the last generation's. */
  output: unknown;
  error: string | null;
  blockedOn: RunBlockedOn | null;
  /** The chain's names, as `list({ tag })` matches them. Tags follow a handover. */
  tags: string[];
  /** The root's start, so an offset on the timeline below is an age of the work. */
  createdAt: number;
  updatedAt: number;
  durationMs: number;
  /** Each generation as `view` renders it, oldest first. */
  runs: RunView[];
  /**
   * Every generation's events on one axis, in order, each saying which run it
   * is recorded on. A handover has no event of its own — nothing replays it —
   * so the only thing that marks one is `generation` changing.
   */
  timeline: ChainTimelineEntry[];
}

export function renderRun(run: RunRecord): RunView {
  return {
    id: run.id,
    workflow: run.workflow,
    workflowVersion: runVersion(run),
    status: run.status,
    chain: { root: chainRoot(run), generation: runGeneration(run) },
    continuation: run.continuation ?? null,
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
    durationMs: run.updatedAt - run.createdAt,
    input: run.input,
    output: run.output,
    error: run.error,
    blockedOn: blockedOn(run),
    tags: run.tags ?? [],
    pendingSignals: Object.fromEntries(
      Object.entries(run.pendingSignals).map(([name, payloads]) => [name, payloads.length]),
    ),
    rejectedSignals: run.rejectedSignals ?? [],
    compaction: run.snapshot
      ? { calls: run.snapshot.calls, droppedEvents: run.snapshot.droppedEvents, at: run.snapshot.at }
      : null,
    timeline: historyEvents(run)
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

/**
 * Render a chain from its generations, oldest first, as the caller's walk along
 * the recorded handovers produced them.
 *
 * The last generation is the live one by construction: the walk ends at the run
 * that names no successor. A crash between creating a successor and recording
 * the handover leaves the predecessor last for one tick, which is also the run
 * a signal would reach, so the view and the engine agree about what is live
 * even while they are both wrong about it.
 */
export function renderChain(generations: RunRecord[]): ChainView {
  const runs = generations.map(renderRun);
  const root = runs[0]!;
  const live = runs[runs.length - 1]!;
  return {
    root: root.id,
    live: live.id,
    workflow: live.workflow,
    generations: runs.length,
    status: live.status,
    input: root.input,
    output: live.output,
    error: live.error,
    blockedOn: live.blockedOn,
    tags: live.tags,
    createdAt: root.createdAt,
    updatedAt: live.updatedAt,
    durationMs: live.updatedAt - root.createdAt,
    runs,
    timeline: runs.flatMap((run) =>
      run.timeline.map((entry) => ({
        ...entry,
        // Offsets from the root rather than from the generation the event is
        // on: one axis is the point, and a per-run offset would restart at
        // zero at every handover.
        elapsedMs: entry.at - root.createdAt,
        generation: run.chain.generation,
        runId: run.id,
      })),
    ),
  };
}

function blockedOn(run: RunRecord): RunBlockedOn | null {
  if (TERMINAL.has(run.status)) return null;
  if (run.status === "waiting" && run.waitingFor) {
    return { kind: "signal", name: run.waitingFor.name, until: run.wakeAt };
  }
  if (run.status === "sleeping") {
    if (run.pendingTimer) return { kind: "timer", name: run.pendingTimer.name, until: run.wakeAt };
    // Sleeping with no timer means a step, or an undo the run is compensating
    // with, is serving out its retry backoff — which lives in history rather
    // than on the record.
    const retrying = [...run.history]
      .reverse()
      .find((e) => (e.type === "step.failed" || e.type === "compensation.failed") && e.retryAt !== undefined);
    if (retrying) return { kind: "retry", name: retrying.name, until: run.wakeAt };
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
    case "compensation.completed":
      return `compensation "${event.name}" completed`;
    case "compensation.failed":
      return event.retryAt === undefined
        ? `compensation "${event.name}" failed on attempt ${event.attempt}, no attempts left: ${event.error}`
        : `compensation "${event.name}" failed on attempt ${event.attempt}, retrying: ${event.error}`;
    case "signal.received":
      return `signal "${event.name}" received`;
    case "signal.timeout":
      return `waitFor "${event.name}" timed out`;
    case "timer.fired":
      return `timer "${event.name}" fired`;
    case "child.started":
      return `child "${event.name}" started as run ${event.childRunId}`;
  }
}
