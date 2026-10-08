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

  it("finds a tagged run from another connection, and pages a tag query", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    for (const id of ["r1", "r2", "r3"]) await engine.start(wf, { n: 1 }, { id, tags: ["tenant:acme"] });
    await engine.start(wf, { n: 1 }, { id: "r4", tags: ["tenant:other"] });

    // A second connection, as the admin process that did not start the runs.
    const other = connect();
    try {
      const reader = new Engine({ store: other.store, workflows: [wf], now: () => T0 });
      const first = await reader.list({ tag: "tenant:acme", limit: 2 });
      expect(first.runs.map((r) => r.id)).toEqual(["r3", "r2"]);
      const second = await reader.list({ tag: "tenant:acme", limit: 2, cursor: first.cursor ?? "" });
      expect(second.runs.map((r) => r.id)).toEqual(["r1"]);
      expect(second.cursor).toBeNull();
      expect((await reader.list({ tag: "tenant:acme" })).runs[0]?.tags).toEqual(["tenant:acme"]);
    } finally {
      other.db.close();
    }
  });

  it("answers a tag query from the tag index, in order, without sorting", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    await engine.start(wf, { n: 1 }, { id: "r1", tags: ["order:1"] });

    // The store's own statement, so this says something about the query that
    // runs rather than about a copy of it kept in a test.
    const prepare = db.prepare.bind(db);
    let sql: string | null = null;
    db.prepare = ((text: string) => {
      if (text.includes("workflow_runs_tags")) sql = text;
      return prepare(text);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    }) as any;
    await engine.list({ tag: "order:1" });
    db.prepare = prepare;
    expect(sql).not.toBeNull();

    const plan = (db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all({
      tag: "order:1",
      workflow: null,
      status: null,
      createdAt: null,
      id: null,
      limit: 51,
    }) as { detail: string }[]).map((p) => p.detail);

    // A seek into (tag, created_at DESC, run_id DESC) and nothing else: the tag
    // narrows and the same index supplies the order, so no temporary B-tree is
    // built and no run outside the page is read. An index on the tag alone
    // would still pass the first of these and fail the second.
    expect(plan.join("\n")).toMatch(/SEARCH t USING COVERING INDEX workflow_runs_tags_recent \(tag=\?\)/);
    expect(plan.join("\n")).not.toMatch(/TEMP B-TREE/);
  });

  it("answers a set of tags from the same index, still without sorting", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    await engine.start(wf, { n: 1 }, { id: "r1", tags: ["order:1", "tenant:acme"] });
    await engine.start(wf, { n: 1 }, { id: "r2", tags: ["order:1"] });
    await engine.start(wf, { n: 1 }, { id: "r3", tags: ["tenant:acme"] });

    expect((await engine.list({ tag: ["order:1", "tenant:acme"] })).runs.map((r) => r.id)).toEqual(["r1"]);

    const prepare = db.prepare.bind(db);
    let sql: string | null = null;
    db.prepare = ((text: string) => {
      if (text.includes("workflow_runs_tags")) sql = text;
      return prepare(text);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    }) as any;
    await engine.list({ tag: ["order:1", "tenant:acme"] });
    db.prepare = prepare;
    expect(sql).not.toBeNull();

    const plan = (db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all({
      tag: "order:1",
      rest: JSON.stringify(["tenant:acme"]),
      workflow: null,
      status: null,
      createdAt: null,
      id: null,
      limit: 51,
    }) as { detail: string }[]).map((p) => p.detail);

    // The extra tag did not change how the query enters the table: the same
    // seek supplies the page and its order, and the rest of the set is a probe
    // into the same index per candidate. A query that intersected the tags
    // instead would sort, and would read every run carrying either of them.
    expect(plan.join("\n")).toMatch(/SEARCH t USING COVERING INDEX workflow_runs_tags_recent \(tag=\?\)/);
    expect(plan.join("\n")).toMatch(/SEARCH o USING COVERING INDEX sqlite_autoindex_workflow_runs_tags_1/);
    expect(plan.join("\n")).not.toMatch(/TEMP B-TREE/);
  });

  it("hydrates listed runs from the same columns as get()", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    const id = await engine.start(wf, { n: 21 });
    await engine.settle(id);

    const [listed] = (await engine.list({ workflow: "sqlite-sum" })).runs;
    expect(listed).toEqual(await store.get(id));
  });
});
