import { Buffer } from "node:buffer";
import type { RunRecord } from "./types.js";

export const DEFAULT_LIMIT = 50;
export const MAX_LIMIT = 500;

/** Clamp a caller-supplied page size, so one query cannot pull a whole table. */
export function pageLimit(limit: number | undefined): number {
  if (limit === undefined) return DEFAULT_LIMIT;
  return Math.min(Math.max(Math.trunc(limit), 1), MAX_LIMIT);
}

/**
 * A keyset position. Listing orders by (createdAt, id) descending and a cursor
 * names one exact row, rather than an offset: runs are created while an admin
 * pages through them, and an offset would skip or repeat rows as they arrive.
 * `id` breaks ties because a batch of runs can start in the same millisecond.
 */
export interface Cursor {
  createdAt: number;
  id: string;
}

export function encodeCursor(run: Pick<RunRecord, "createdAt" | "id">): string {
  return Buffer.from(JSON.stringify([run.createdAt, run.id])).toString("base64url");
}

export function decodeCursor(cursor: string): Cursor {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
  } catch {
    throw new Error(`invalid cursor: ${cursor}`);
  }
  if (!Array.isArray(parsed) || typeof parsed[0] !== "number" || typeof parsed[1] !== "string") {
    throw new Error(`invalid cursor: ${cursor}`);
  }
  return { createdAt: parsed[0], id: parsed[1] };
}

/** Newest first, id descending to break ties — the order every store must list in. */
export function byNewest(a: Cursor, b: Cursor): number {
  if (a.createdAt !== b.createdAt) return b.createdAt - a.createdAt;
  return a.id < b.id ? 1 : a.id > b.id ? -1 : 0;
}

/** Does `run` come strictly after `cursor` in that order? */
export function afterCursor(run: Cursor, cursor: Cursor): boolean {
  return run.createdAt < cursor.createdAt || (run.createdAt === cursor.createdAt && run.id < cursor.id);
}
