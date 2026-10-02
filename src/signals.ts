import type { RejectedSignal, RunRecord } from "./types.js";

/**
 * What a signal's payload is checked against: anything with zod's `parse`.
 * Structural on purpose — a zod schema, a valibot one behind a wrapper and a
 * hand-written `{ parse }` all satisfy it — so which schema library a caller
 * uses stays theirs to decide and none of them is a dependency here.
 */
export interface SignalSchema<T> {
  parse(payload: unknown): T;
}

/**
 * A signal's name and the schema its payloads must satisfy, in one value. The
 * sender and the waiting workflow take both from it, which is what stops them
 * disagreeing about either.
 */
export interface SignalDefinition<T = unknown> {
  name: string;
  parse(payload: unknown): T;
}

export function defineSignal<T>(name: string, schema: SignalSchema<T>): SignalDefinition<T> {
  return { name, parse: (payload) => schema.parse(payload) };
}

/** The name a signal is addressed by, however it was named. */
export function signalName(target: SignalDefinition<unknown> | string): string {
  return typeof target === "string" ? target : target.name;
}

/**
 * The signals an engine checks when it is handed a name rather than a
 * definition. A name that is not here is unvalidated rather than refused:
 * untyped signals predate schemas, and the names the engine derives for itself
 * — a child reporting its outcome to its parent — have no schema to register.
 */
export class SignalRegistry {
  private readonly byName = new Map<string, SignalDefinition<unknown>>();

  add(signal: SignalDefinition<unknown>): void {
    if (this.byName.has(signal.name)) {
      // Two schemas for one name means a payload is valid or not depending on
      // which registration the engine happened to find, which is the confusion
      // naming the signal once exists to prevent.
      throw new Error(`signal "${signal.name}" is registered twice`);
    }
    this.byName.set(signal.name, signal);
  }

  get(name: string): SignalDefinition<unknown> | undefined {
    return this.byName.get(name);
  }
}

/**
 * How many refusals a run keeps. The recent ones answer "I sent the approval
 * and nothing happened"; a caller retrying a malformed payload in a loop must
 * not be able to grow the record without bound.
 */
export const MAX_REJECTED_SIGNALS = 10;

/** Record a refusal on the run, newest first, dropping the oldest past the cap. */
export function recordRejection(run: RunRecord, rejection: RejectedSignal): void {
  run.rejectedSignals = [rejection, ...(run.rejectedSignals ?? [])].slice(0, MAX_REJECTED_SIGNALS);
}
