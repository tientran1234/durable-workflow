import { isDue } from "../due.js";
import type { RunRecord, RunStore } from "../types.js";

/** In-process store. Every read returns a copy, so callers cannot bypass save(). */
export class MemoryStore implements RunStore {
  private readonly runs = new Map<string, RunRecord>();

  async create(run: RunRecord): Promise<void> {
    if (this.runs.has(run.id)) throw new Error(`run ${run.id} already exists`);
    this.runs.set(run.id, structuredClone(run));
  }

  async get(id: string): Promise<RunRecord | null> {
    const run = this.runs.get(id);
    return run ? structuredClone(run) : null;
  }

  async save(run: RunRecord, expectedVersion: number): Promise<boolean> {
    const current = this.runs.get(run.id);
    if (!current || current.version !== expectedVersion) return false;
    run.version = expectedVersion + 1;
    this.runs.set(run.id, structuredClone(run));
    return true;
  }

  async claimDue(now: number, leaseMs: number, limit: number): Promise<RunRecord[]> {
    const due = [...this.runs.values()]
      .filter((r) => isDue(r, now) && (r.leaseUntil === null || r.leaseUntil < now))
      .sort((a, b) => (a.wakeAt ?? 0) - (b.wakeAt ?? 0) || a.createdAt - b.createdAt)
      .slice(0, limit);
    for (const run of due) {
      run.leaseUntil = now + leaseMs;
      run.version += 1;
    }
    return due.map((r) => structuredClone(r));
  }

  /** Test helper. */
  all(): RunRecord[] {
    return [...this.runs.values()].map((r) => structuredClone(r));
  }
}
