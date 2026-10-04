import type { RunRecord } from "./types.js";

/**
 * A run id is the engine's name for a run; an operator arrives holding the
 * application's — order 4182, tenant acme, the invoice a customer is asking
 * about. A tag records that name on the run at start, indexed by every store,
 * so `engine.list({ tag })` answers "which run is handling this" without the
 * caller having kept the id and without a scan over every run.
 *
 * Tags are set once, by `engine.start`, and never change afterwards. They say
 * what the run is about, which is settled before the first step; a store is
 * therefore free to mirror them into an index at create and never look again.
 */

/**
 * How many tags one run may carry, and how long one may be. Tags are an index
 * key, not a place to put the payload: the input is already on the record, and
 * a caller tagging a run with everything it knows turns every store's index
 * into a second copy of it.
 */
export const MAX_TAGS = 16;
export const MAX_TAG_LENGTH = 128;

/**
 * The tags a run is created with: trimmed, deduplicated and sorted.
 *
 * Sorted because the order tags were passed in is not information — it would
 * otherwise show up as a difference between two runs tagged the same way — and
 * refused rather than silently dropped when one is empty or oversized, because
 * the failure mode of a tag that did not survive is an operator searching for
 * a run they cannot find.
 */
export function normalizeTags(tags: readonly string[]): string[] {
  if (tags.length > MAX_TAGS) throw new Error(`a run may carry at most ${MAX_TAGS} tags, got ${tags.length}`);
  const seen = new Set<string>();
  for (const tag of tags) seen.add(validTag(tag));
  return [...seen].sort();
}

/**
 * The tag a `list({ tag })` query means, or null if it did not ask for one.
 * Every store calls this, the way each one calls `pageLimit`: the same spelling
 * has to match the same runs whichever store answers.
 */
export function queryTag(tag: string | undefined): string | null {
  return tag === undefined ? null : validTag(tag);
}

/** Does this run carry `tag`? For stores that filter in memory. */
export function hasTag(run: Pick<RunRecord, "tags">, tag: string): boolean {
  return run.tags?.includes(tag) ?? false;
}

function validTag(tag: string): string {
  const trimmed = tag.trim();
  if (trimmed === "") throw new Error("a tag cannot be empty");
  if (trimmed.length > MAX_TAG_LENGTH) throw new Error(`tag "${trimmed.slice(0, 32)}…" is longer than ${MAX_TAG_LENGTH}`);
  return trimmed;
}
