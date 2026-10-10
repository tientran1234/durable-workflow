/**
 * Against a real Postgres, because the guarantee under test is a database
 * guarantee: FOR UPDATE SKIP LOCKED hands disjoint sets of runs to concurrent
 * workers, and a version-guarded UPDATE rejects stale writes.
 *
 *   pnpm db:up && DATABASE_URL=postgresql://postgres:postgres@localhost:5434/workflow pnpm test
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Engine, type RunRecord, type Wakeup, defineWorkflow } from "../src/index.js";
import { PostgresStore, WAKEUP_RECONNECT_MS } from "../src/stores/postgres.js";
import { until } from "./helpers.js";

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
    await pool.query("TRUNCATE workflow_runs_test, workflow_runs_test_tags");
  });
  afterAll(async () => {
    await pool.query("DROP TABLE IF EXISTS workflow_runs_test, workflow_runs_test_tags");
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

  it("lists runs newest first, filtered, and pages with an exact cursor", async () => {
    // Same createdAt for every run, so the page boundary rests on the id tiebreak.
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    for (const id of ["r1", "r2", "r3", "r4"]) await engine.start(wf, { n: 1 }, { id });
    const waiting = await engine.start(wf, { n: 1 }, { id: "r5" });
    await engine.settle(waiting); // sleeping, so it drops out of a status filter

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

  it("finds tagged runs and pages a tag query with an exact cursor", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    for (const id of ["r1", "r2", "r3"]) await engine.start(wf, { n: 1 }, { id, tags: ["tenant:acme"] });
    await engine.start(wf, { n: 1 }, { id: "r4", tags: ["tenant:other"] });

    const first = await engine.list({ tag: "tenant:acme", limit: 2 });
    expect(first.runs.map((r) => r.id)).toEqual(["r3", "r2"]);
    const second = await engine.list({ tag: "tenant:acme", limit: 2, cursor: first.cursor ?? "" });
    expect(second.runs.map((r) => r.id)).toEqual(["r1"]);
    expect(second.cursor).toBeNull();
    expect(first.runs[0]?.tags).toEqual(["tenant:acme"]);
    expect((await engine.list({ tag: "tenant:nobody" })).runs).toEqual([]);
  });

  it("narrows to the runs carrying every tag in a set, and pages them", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    for (const id of ["r1", "r2", "r3"]) {
      await engine.start(wf, { n: 1 }, { id, tags: ["tenant:acme", id === "r2" ? "order:2" : "order:1"] });
    }
    await engine.start(wf, { n: 1 }, { id: "r4", tags: ["order:1"] });

    // r4 carries the order but not the tenant, r2 the tenant but not the
    // order: the set is both, which is neither tag's own answer.
    expect((await engine.list({ tag: "order:1" })).runs.map((r) => r.id)).toEqual(["r4", "r3", "r1"]);
    const page = await engine.list({ tag: ["tenant:acme", "order:1"], limit: 1 });
    expect(page.runs.map((r) => r.id)).toEqual(["r3"]);
    const next = await engine.list({ tag: ["tenant:acme", "order:1"], limit: 1, cursor: page.cursor ?? "" });
    expect(next.runs.map((r) => r.id)).toEqual(["r1"]);
    expect(next.cursor).toBeNull();
    expect((await engine.list({ tag: ["tenant:acme", "order:nobody"] })).runs).toEqual([]);
  });

  it("answers a tag query from the tag index, in order, without sorting", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    await engine.start(wf, { n: 1 }, { id: "r1", tags: ["order:1"] });

    // The store's own statement, so this says something about the query that
    // runs rather than about a copy of it kept in a test.
    const query = pool.query.bind(pool);
    let captured: { text: string; values: unknown[] } | null = null;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    pool.query = (text: any, values?: any) => {
      if (typeof text === "string" && text.includes("workflow_runs_test_tags")) captured = { text, values };
      return query(text, values);
    };
    await engine.list({ tag: "order:1" });
    pool.query = query;
    expect(captured).not.toBeNull();

    const client = await pool.connect();
    try {
      // A table this small is cheapest to read whole, so the planner is told to
      // cost the alternatives out of the way. What is under test is that the
      // index can answer the query at all — which it only can if it carries the
      // tag and the listing order together.
      await client.query("SET enable_seqscan = off");
      await client.query("SET enable_bitmapscan = off");
      const explained = await client.query(`EXPLAIN (COSTS OFF) ${captured!.text}`, captured!.values);
      const plan = explained.rows.map((row: Record<string, string>) => row["QUERY PLAN"]).join("\n");

      // An ordered scan of (tag, created_at DESC, run_id DESC) and no Sort: the
      // tag narrows and the same index supplies the order, so the query reads
      // the page it returns. An index on the tag alone would still appear here
      // and would put a Sort above it.
      expect(plan).toMatch(/Index Only Scan using workflow_runs_test_tags_recent/);
      expect(plan).not.toMatch(/Sort/);

      // And a set of tags enters the table the same way: the extra tags are a
      // subquery on the rows that scan already named, so they add probes into
      // the same index rather than a second scan to intersect and sort.
      captured = null;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      pool.query = (text: any, values?: any) => {
        if (typeof text === "string" && text.includes("workflow_runs_test_tags")) captured = { text, values };
        return query(text, values);
      };
      await engine.list({ tag: ["order:1", "tenant:acme"] });
      pool.query = query;
      expect(captured).not.toBeNull();
      const forSet = await client.query(`EXPLAIN (COSTS OFF) ${captured!.text}`, captured!.values);
      const setPlan = forSet.rows.map((row: Record<string, string>) => row["QUERY PLAN"]).join("\n");

      expect(setPlan).toMatch(/Index Only Scan using workflow_runs_test_tags_recent/);
      expect(setPlan).not.toMatch(/Sort/);
    } finally {
      client.release();
    }
  });

  it("rewrites a run's tag index in one transaction, keeping the place the run was created in", async () => {
    let now = T0;
    const engine = new Engine({ store, workflows: [wf], now: () => now });
    await engine.start(wf, { n: 1 }, { id: "early", tags: ["order:1"] });
    now += 1_000;
    await engine.start(wf, { n: 1 }, { id: "late", tags: ["order:2", "tenant:acme"] });
    now += 1_000;

    const run = (await store.get("early"))!;
    // What engine.retag hands the store: the record as the retag leaves it,
    // which moves updatedAt. The index rows still have to carry createdAt.
    run.updatedAt = now;
    // One tag kept and one replaced: the kept tag is the row the delete and the
    // insert both name, which is why this is a transaction and not one
    // statement with CTEs over a single snapshot.
    run.tags = ["order:2", "tenant:acme"];
    expect(await store.retag(run, run.version)).toBe(true);

    expect((await engine.list({ tag: "order:1" })).runs).toEqual([]);
    expect((await store.get("early"))?.tags).toEqual(["order:2", "tenant:acme"]);
    // Created first, so it lists second. An index row stamped with the time of
    // the retag — the obvious thing for a write to carry — would have put it
    // above the run that was started after it.
    expect((await engine.list({ tag: "order:2" })).runs.map((r) => r.id)).toEqual(["late", "early"]);
    expect((await engine.list({ tag: ["order:2", "tenant:acme"] })).runs.map((r) => r.id)).toEqual(["late", "early"]);
  });

  it("rejects a stale retag, leaving the record and the index as they were", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    await engine.start(wf, { n: 1 }, { id: "r1", tags: ["order:1"] });
    const stale = (await store.get("r1"))!;
    await store.save((await store.get("r1"))!, stale.version);

    stale.tags = ["order:2"];
    expect(await store.retag(stale, stale.version)).toBe(false);

    // Refused as one write: a run the index answers under the new tag while
    // its record still carries the old one is the half-applied retag the
    // transaction is there to rule out.
    expect((await store.get("r1"))?.tags).toEqual(["order:1"]);
    expect((await engine.list({ tag: "order:2" })).runs).toEqual([]);
    expect((await engine.list({ tag: "order:1" })).runs.map((r) => r.id)).toEqual(["r1"]);
  });

  it("hydrates listed runs from the same columns as get()", async () => {
    const engine = new Engine({ store, workflows: [wf], now: () => T0 });
    const id = await engine.start(wf, { n: 21 });
    await engine.settle(id);

    const [listed] = (await engine.list({ workflow: "pg-sum" })).runs;
    expect(listed).toEqual(await store.get(id));
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

  describe("event-driven wakeups", () => {
    it("sends a wakeup for every run a worker could claim, and none for one it could not", async () => {
      const wakes: Wakeup[] = [];
      const subscription = await store.watch((wake) => wakes.push(wake));
      try {
        let now = T0;
        const engine = new Engine({ store, workflows: [wf], now: () => now });

        const id = await engine.start(wf, { n: 21 });
        await until(() => wakes.length === 1);
        await engine.settle(id); // asleep on its timer
        await until(() => wakes.length === 2);
        now += 1_000;
        await engine.settle(id); // waiting for the signal, with a deadline
        await until(() => wakes.length === 3);
        expect((await engine.signal(id, "confirm", true)).status).toBe("completed");
        await until(() => wakes.length === 4);

        expect(wakes).toEqual([
          { runId: id, wakeAt: null }, // created: running, so due now
          { runId: id, wakeAt: T0 + 1_000 }, // the timer it is asleep on
          { runId: id, wakeAt: T0 + 6_000 }, // the deadline on the wait
          { runId: id, wakeAt: null }, // the signal made it due
        ]);

        // The run completed inside that last pass, and a terminal run is not
        // one to claim. Ordering is what proves nothing was sent for it: a
        // session is delivered its notifications in order, so the next wakeup
        // to arrive being the new run's means none came between.
        const other = await engine.start(wf, { n: 1 });
        await until(() => wakes.length === 5);
        expect(wakes[4]).toEqual({ runId: other, wakeAt: null });
      } finally {
        await subscription.close();
      }
    });

    it("picks a new run up at once, on a worker whose poll interval it would wait out", async () => {
      let passes = 0;
      class Counted extends PostgresStore {
        override async claimDue(now: number, leaseMs: number, limit: number): Promise<RunRecord[]> {
          const claimed = await super.claimDue(now, leaseMs, limit);
          passes++;
          return claimed;
        }
      }
      const counted = new Counted(pool, "workflow_runs_test");
      const job = defineWorkflow<{ n: number }, number>("pg-job", async (ctx, input) =>
        ctx.step("double", () => input.n * 2),
      );
      const engine = new Engine({ store: counted, workflows: [job] });

      const handle = engine.worker({ pollMs: 60_000 });
      try {
        await until(() => passes >= 1); // nothing to do; the worker is waiting
        const id = await engine.start(job, { n: 21 });

        await until(async () => (await counted.get(id))?.status === "completed");
        expect((await counted.get(id))?.output).toBe(42);
      } finally {
        await handle.stop();
      }
    });

    it("is there for a timer because the write that set it said when", async () => {
      const napper = defineWorkflow<null, string>("pg-napper", async (ctx) => {
        await ctx.sleep("nap", 200);
        return ctx.step("after", () => "awake");
      });
      const engine = new Engine({ store, workflows: [napper] });
      const id = await engine.start(napper, null);

      // Nothing writes the run when the timer arrives, so the only thing that
      // can bring the worker back inside its poll interval is the wake time it
      // was told when the run went to sleep.
      const handle = engine.worker({ pollMs: 60_000 });
      try {
        await until(async () => (await store.get(id))?.status === "completed", 3_000);
        expect((await store.get(id))?.output).toBe("awake");
      } finally {
        await handle.stop();
      }
    });

    it("replaces a listening session the server dropped", async () => {
      const wakes: Wakeup[] = [];
      const subscription = await store.watch((wake) => wakes.push(wake));
      try {
        const engine = new Engine({ store, workflows: [wf], now: () => T0 });
        await engine.start(wf, { n: 1 }, { id: "before" });
        await until(() => wakes.length === 1);

        // The way a failover or an idle-connection reaper ends it.
        await pool.query(
          `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
            WHERE query LIKE 'LISTEN%' AND pid <> pg_backend_pid()`,
        );

        // Whatever is sent while it is down is lost, which is what the poll
        // interval is for — so wait the reconnect out before sending one that
        // has somewhere to land.
        await new Promise<void>((resolve) => setTimeout(resolve, WAKEUP_RECONNECT_MS + 250));
        await engine.start(wf, { n: 1 }, { id: "after" });

        await until(() => wakes.some((wake) => wake.runId === "after"));
      } finally {
        await subscription.close();
      }
    });
  });
});
