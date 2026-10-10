import type { RunRecord } from "./types.js";

/**
 * A run id is the engine's name for a run; an operator arrives holding the
 * application's — order 4182, tenant acme, the invoice a customer is asking
 * about. A tag records that name on the run at start, indexed by every store,
 * so `engine.list({ tag })` answers "which run is handling this" without the
 * caller having kept the id and without a scan over every run.
 *
 * Tags are set by `engine.start` and rewritten only by `engine.retag`. Every
 * other write a run makes leaves them where they were, which is why a store
 * mirrors them into an index at create and revisits it in that one place
 * rather than on every tick.
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
 * The tags a run carries: trimmed, deduplicated and sorted. Applied where they
 * are set — at start, and again where a retag replaces them.
 *
 * Sorted because the order tags were passed in is not information — it would
 * otherwise show up as a difference between two runs tagged the same way — and
 * refused rather than silently dropped when one is empty or oversized, because
 * the failure mode of a tag that did not survive is an operator searching for
 * a run they cannot find.
 */
export function normalizeTags(tags: readonly string[]): string[] {
  if (tags.length > MAX_TAGS) throw new Error(`a run may carry at most ${MAX_TAGS} tags, got ${tags.length}`);
  return normalized(tags);
}

/**
 * The tags a `list({ tag })` query means: none, one, or a set of them. A run
 * matches a set only by carrying every tag in it, so the set narrows the
 * listing the way `workflow` and `status` do rather than widening it.
 *
 * Normalized exactly as a run's own tags are, because the two are compared: a
 * query spelled `" order:1 "` has to reach the run stored under `"order:1"`.
 * An empty set asks for nothing and so narrows nothing, which is what a caller
 * assembling a query from filters the operator left blank means by it.
 *
 * Every store calls this, the way each one calls `pageLimit`: the same spelling
 * has to match the same runs whichever store answers. It replaces the
 * single-tag reader stores used to call, rather than sitting beside it, so a
 * store written against that one fails to compile instead of quietly answering
 * a two-tag query with the first tag's runs.
 */
export function queryTags(tag: string | readonly string[] | undefined): string[] {
  if (tag === undefined) return [];
  if (typeof tag === "string") return [validTag(tag)];
  if (tag.length > MAX_TAGS) throw new Error(`a tag query may name at most ${MAX_TAGS} tags, got ${tag.length}`);
  return normalized(tag);
}

/**
 * Are these the same tags? Both sides come out of `normalizeTags`, so sameness
 * is element-wise: a retag to the tags a run already carries is a write worth
 * not making, since it would delete index rows to put them back and bump the
 * version under whatever worker holds the run.
 */
export function sameTags(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((tag, i) => tag === b[i]);
}

/** Does this run carry `tag`? For stores that filter in memory. */
export function hasTag(run: Pick<RunRecord, "tags">, tag: string): boolean {
  return run.tags?.includes(tag) ?? false;
}

/** Trimmed, deduplicated and sorted — the one spelling a tag is stored and matched under. */
function normalized(tags: readonly string[]): string[] {
  const seen = new Set<string>();
  for (const tag of tags) seen.add(validTag(tag));
  return [...seen].sort();
}

function validTag(tag: string): string {
  const trimmed = tag.trim();
  if (trimmed === "") throw new Error("a tag cannot be empty");
  if (trimmed.length > MAX_TAG_LENGTH) throw new Error(`tag "${trimmed.slice(0, 32)}…" is longer than ${MAX_TAG_LENGTH}`);
  return trimmed;
}
