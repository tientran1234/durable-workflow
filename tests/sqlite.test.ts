/**
 * Against a real database file, because what the store promises is what SQLite
 * promises: a record that outlives the process, a version-guarded UPDATE that
 * rejects a stale write, and a lease no second connection can take twice.
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Engine, defineWorkflow } from "../src/index.js";
import { SqliteStore } from "../src/stores/sqlite.js";

describe("SqliteStore", () => {
  const T0 = 1_800_000_000_000;
  let dir: string;
  let file: string;
  let db: Database.Database;
  let store: SqliteStore;

  /** A store over the same file, as a second process sees it. */
  const connect = (): { db: Database.Database; store: SqliteStore } => {
    const other = new Database(file);
    const s = new SqliteStore(other);
    s.ensureSchema();
    return { db: other, store: s };
  };

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "durable-workflow-"));
    file = join(dir, "runs.db");
    ({ db, store } = connect());
  });
  afterEach(() => {
    db.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const wf = defineWorkflow<{ n: number }, number>("sqlite-sum", async (ctx, input) => {
    const a = await ctx.step("double", () => input.n * 2);
    await ctx.sleep("pause", 1_000);
    const ok = await ctx.waitFor<boolean>("confirm", { timeoutMs: 5_000 });
    return ok ? a : -1;
  });

  it("runs a full workflow — step, timer, signal — through the store", async () => {
    let now = T0;
    const engine = new Engine({ store, workflows: [wf], now: () => now });
    const id = await engine.start(wf, { n: 21 });

    expect((await engine.settle(id)).status).toBe("sleeping");
    now += 1_000;
    expect((await engine.settle(id)).status).toBe("waiting");
    const run = await engine.signal(id, "confirm", true);

    expect(run.status).toBe("completed");
    expect(run.output).toBe(42);
    expect(run.version).toBeGreaterThan(3);

    const stored = await store.get(id);
    expect(stored?.history.map((e) => e.type)).toEqual([
      "step.completed",
      "timer.fired",
      "signal.received",
    ]);
  });

  it("resumes a run from the file after the process that started it is gone", async () => {
    let now = T0;
    const id = await new Engine({ store, workflows: [wf], now: () => now }).start(wf, { n: 21 });
    await new Engine({ store, workflows: [wf], now: () => now }).settle(id); // sleeping on the timer
    db.close();

    // A fresh connection, as a restarted worker opens it: no memoised step re-runs.
    ({ db, store } = connect());
    now += 1_000;
    const engine = new Engine({ store, workflows: [wf], now: () => now });
    expect((await engine.settle(id)).status).toBe("waiting");
    const run = await engine.signal(id, "confirm", true);

    expect(run.output).toBe(42);
    expect(run.history.map((e) => e.type)).toEqual(["step.completed", "timer.fired", "signal.received"]);
  });

  it("rejects a stale write", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    const id = await engine.start(wf, { n: 1 });
    const a = (await store.get(id))!;
    const b = (await store.get(id))!;
    expect(await store.save(a, a.version)).toBe(true);
    expect(await store.save(b, b.version)).toBe(false);
  });

  it("refuses to create a second run under one id", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    await engine.start(wf, { n: 1 }, { id: "r1" });
    const run = (await store.get("r1"))!;
    await expect(store.create(run)).rejects.toThrow();
  });

  it("hands two connections disjoint sets of due runs", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    const ids = await Promise.all(Array.from({ length: 10 }, () => engine.start(wf, { n: 1 })));
    const second = connect();

    const a = await store.claimDue(T0, 30_000, 5);
    const b = await second.store.claimDue(T0, 30_000, 5);
    second.db.close();
    const claimed = [...a, ...b].map((r) => r.id);

    expect(new Set(claimed).size).toBe(10);
    expect(claimed.sort()).toEqual([...ids].sort());
    for (const r of [...a, ...b]) expect(r.leaseUntil).toBe(T0 + 30_000);
  });

  it("does not hand out a run whose lease is still held, and does once it lapses", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0, leaseMs: 10_000 });
    await engine.start(wf, { n: 1 });

    expect(await store.claimDue(T0, 10_000, 10)).toHaveLength(1);
    expect(await store.claimDue(T0 + 5_000, 10_000, 10)).toHaveLength(0);
    expect(await store.claimDue(T0 + 10_001, 10_000, 10)).toHaveLength(1);
  });

  it("does not hand out sleeping runs before their wake time", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    const id = await engine.start(wf, { n: 1 });
    await engine.settle(id); // sleeping until T0 + 1000

    expect(await store.claimDue(T0 + 999, 30_000, 10)).toHaveLength(0);
    expect((await store.claimDue(T0 + 1_000, 30_000, 10)).map((r) => r.id)).toEqual([id]);
  });

  it("lists runs newest first, filtered, and pages with an exact cursor", async () => {
    // Same createdAt for every run, so the page boundary rests on the id tiebreak.
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    for (const id of ["r1", "r2", "r3", "r4"]) await engine.start(wf, { n: 1 }, { id });
    const sleeping = await engine.start(wf, { n: 1 }, { id: "r5" });
    await engine.settle(sleeping); // sleeping, so it drops out of a status filter

    const first = await engine.list({ limit: 2 });
    expect(first.runs.map((r) => r.id)).toEqual(["r5", "r4"]);
    const second = await engine.list({ limit: 2, cursor: first.cursor ?? "" });
    expect(second.runs.map((r) => r.id)).toEqual(["r3", "r2"]);
    const third = await engine.list({ limit: 2, cursor: second.cursor ?? "" });
    expect(third.runs.map((r) => r.id)).toEqual(["r1"]);
    expect(third.cursor).toBeNull();

    expect((await engine.list({ status: "running" })).runs.map((r) => r.id)).toEqual(["r4", "r3", "r2", "r1"]);
    expect((await engine.list({ workflow: "nope" })).runs).toEqual([]);
  });

  it("hydrates listed runs from the same columns as get()", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    const id = await engine.start(wf, { n: 21 });
    await engine.settle(id);

    const [listed] = (await engine.list({ workflow: "sqlite-sum" })).runs;
    expect(listed).toEqual(await store.get(id));
  });
});
