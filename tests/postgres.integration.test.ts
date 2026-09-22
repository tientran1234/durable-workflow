/**
 * Against a real Postgres, because the guarantee under test is a database
 * guarantee: FOR UPDATE SKIP LOCKED hands disjoint sets of runs to concurrent
 * workers, and a version-guarded UPDATE rejects stale writes.
 *
 *   pnpm db:up && DATABASE_URL=postgresql://postgres:postgres@localhost:5434/workflow pnpm test
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Engine, defineWorkflow } from "../src/index.js";
import { PostgresStore } from "../src/stores/postgres.js";

const url = process.env.DATABASE_URL;

describe.skipIf(!url)("PostgresStore", () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let pool: any;
  let store: PostgresStore;
  const T0 = 1_800_000_000_000;

  beforeAll(async () => {
    const { Pool } = await import("pg");
    pool = new Pool({ connectionString: url });
    store = new PostgresStore(pool, "workflow_runs_test");
    await store.ensureSchema();
  });
  beforeEach(async () => {
    await pool.query("TRUNCATE workflow_runs_test");
  });
  afterAll(async () => {
    await pool.query("DROP TABLE IF EXISTS workflow_runs_test");
    await pool.end();
  });

  const wf = defineWorkflow<{ n: number }, number>("pg-sum", async (ctx, input) => {
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

  it("rejects a stale write", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    const id = await engine.start(wf, { n: 1 });
    const a = (await store.get(id))!;
    const b = (await store.get(id))!;
    expect(await store.save(a, a.version)).toBe(true);
    expect(await store.save(b, b.version)).toBe(false);
  });

  it("hands concurrent workers disjoint sets of due runs", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    const ids = await Promise.all(Array.from({ length: 12 }, () => engine.start(wf, { n: 1 })));

    const [a, b, c] = await Promise.all([
      store.claimDue(T0, 30_000, 5),
      store.claimDue(T0, 30_000, 5),
      store.claimDue(T0, 30_000, 5),
    ]);
    const claimed = [...a, ...b, ...c].map((r) => r.id);

    expect(claimed).toHaveLength(12);
    expect(new Set(claimed).size).toBe(12);
    expect(claimed.sort()).toEqual([...ids].sort());
    for (const r of [...a, ...b, ...c]) expect(r.leaseUntil).toBe(T0 + 30_000);
  });

  it("does not hand out a run whose lease is still held, and does once it lapses", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0, leaseMs: 10_000 });
    await engine.start(wf, { n: 1 });

    expect(await store.claimDue(T0, 10_000, 10)).toHaveLength(1);
    expect(await store.claimDue(T0 + 5_000, 10_000, 10)).toHaveLength(0);
    expect(await store.claimDue(T0 + 10_001, 10_000, 10)).toHaveLength(1);
  });

  it("does not hand out sleeping runs before their wake time", async () => {
    let now = T0;
    const engine = new Engine({ store, workflows: [wf], now: () => now });
    const id = await engine.start(wf, { n: 1 });
    await engine.settle(id); // sleeping until T0 + 1000

    expect(await store.claimDue(T0 + 999, 30_000, 10)).toHaveLength(0);
    const due = await store.claimDue(T0 + 1_000, 30_000, 10);
    expect(due.map((r) => r.id)).toEqual([id]);
  });
});
