import { describe, expect, it } from "vitest";
import {
  ChildFailedError,
  type ChildHandle,
  ConflictError,
  Engine,
  MAX_REJECTED_SIGNALS,
  MAX_TAGS,
  MAX_TAG_LENGTH,
  MemoryStore,
  NondeterminismError,
  type RunCompletedEvent,
  type RunFailedEvent,
  type RunPage,
  type RunRecord,
  SignalRejectedError,
  StepFailedError,
  type StepFailedEvent,
  StepTimeoutError,
  WaitTimeoutError,
  type Wakeup,
  type WakeupSource,
  type WakeupSubscription,
  defineSignal,
  defineWorkflow,
  historyEvents,
  schedulePeriod,
  scheduleRunId,
  wakeupFor,
} from "../src/index.js";
import { T0, harness, until } from "./helpers.js";

describe("steps", () => {
  it("runs steps in order and completes with the returned output", async () => {
    const calls: string[] = [];
    const wf = defineWorkflow<{ n: number }, number>("sum", async (ctx, input) => {
      const a = await ctx.step("double", () => (calls.push("double"), input.n * 2));
      const b = await ctx.step("inc", () => (calls.push("inc"), a + 1));
      return b;
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, { n: 20 });
    const run = await engine.settle(id);

    expect(run.status).toBe("completed");
    expect(run.output).toBe(41);
    expect(calls).toEqual(["double", "inc"]);
    expect(run.history.map((e) => e.type)).toEqual(["step.completed", "step.completed"]);
  });

  it("never re-executes a completed step, even when the function is replayed many times", async () => {
    let charged = 0;
    const wf = defineWorkflow<null, string>("pay", async (ctx) => {
      const receipt = await ctx.step("charge", () => `rcpt_${++charged}`);
      await ctx.sleep("settle", 1_000); // forces a replay from the top on the next tick
      return receipt;
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    await engine.tick(id); // replays: step returns memoised result
    await engine.tick(id); // harmless extra tick
    const run = await engine.get(id);

    expect(charged).toBe(1);
    expect(run?.output).toBe("rcpt_1");
  });

  it("retries with a persisted backoff, not a timer in memory", async () => {
    let attempts = 0;
    const wf = defineWorkflow<null, string>("flaky", async (ctx) => {
      return ctx.step("call-api", () => {
        attempts++;
        if (attempts < 3) throw new Error(`boom ${attempts}`);
        return "ok";
      });
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    let run = await engine.settle(id);
    expect(run.status).toBe("sleeping");
    expect(run.wakeAt).toBe(T0 + 1_000); // initialDelayMs
    expect(attempts).toBe(1);

    run = await engine.tick(id); // too early — nothing happens
    expect(attempts).toBe(1);

    advance(1_000);
    run = await engine.settle(id);
    expect(run.status).toBe("sleeping");
    expect(run.wakeAt).toBe(T0 + 1_000 + 2_000); // factor 2
    expect(attempts).toBe(2);

    advance(2_000);
    run = await engine.settle(id);
    expect(run.status).toBe("completed");
    expect(run.output).toBe("ok");
    expect(attempts).toBe(3);
    expect(run.history.filter((e) => e.type === "step.failed")).toHaveLength(2);
  });

  it("fails the run after maxAttempts and exposes the step and the last error", async () => {
    const wf = defineWorkflow<null, void>("doomed", async (ctx) => {
      await ctx.step("never", () => { throw new Error("nope"); }, { retry: { maxAttempts: 2 } });
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    const run = await engine.settle(id);

    expect(run.status).toBe("failed");
    expect(run.error).toMatch(/"never" failed after 2 attempt\(s\): nope/);
  });

  it("lets the workflow catch a final step failure and take a different path", async () => {
    const wf = defineWorkflow<null, string>("saga", async (ctx) => {
      try {
        await ctx.step("ship", () => { throw new Error("no courier"); }, { retry: { maxAttempts: 1 } });
        return "shipped";
      } catch (err) {
        if (!(err instanceof StepFailedError)) throw err;
        await ctx.step("refund", () => "refunded");
        return "refunded";
      }
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);
    expect(run.status).toBe("completed");
    expect(run.output).toBe("refunded");
  });
});

describe("signals", () => {
  const approval = defineWorkflow<null, string>("approval", async (ctx) => {
    const { ok } = await ctx.waitFor<{ ok: boolean }>("manager", { timeoutMs: 60_000 });
    return ok ? "approved" : "rejected";
  });

  it("suspends on waitFor and resumes when the signal arrives", async () => {
    const { engine } = harness([approval]);
    const id = await engine.start(approval, null);

    let run = await engine.settle(id);
    expect(run.status).toBe("waiting");
    expect(run.waitingFor).toEqual({ name: "manager", call: 0 });

    run = await engine.signal(id, "manager", { ok: true });
    expect(run.status).toBe("completed");
    expect(run.output).toBe("approved");
  });

  it("buffers a signal that arrives before the workflow waits for it", async () => {
    const wf = defineWorkflow<null, string>("late-wait", async (ctx) => {
      await ctx.sleep("prep", 5_000);
      const v = await ctx.waitFor<string>("go");
      return v;
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    await engine.settle(id); // sleeping
    await engine.signal(id, "go", "early"); // not waiting yet → buffered
    advance(5_000);
    const run = await engine.settle(id);

    expect(run.status).toBe("completed");
    expect(run.output).toBe("early");
  });

  it("times out a wait and throws inside the workflow, where it can be handled", async () => {
    const wf = defineWorkflow<null, string>("patient", async (ctx) => {
      try {
        await ctx.waitFor("reply", { timeoutMs: 1_000 });
        return "got it";
      } catch (err) {
        if (err instanceof WaitTimeoutError) return `gave up on ${err.signal}`;
        throw err;
      }
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    const run = await engine.settle(id);
    expect(run.output).toBe("gave up on reply");
  });

  it("refuses to signal a finished run", async () => {
    const wf = defineWorkflow<null, number>("done", async () => 1);
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    await engine.settle(id);
    await expect(engine.signal(id, "x")).rejects.toThrow(/completed/);
  });
});

describe("timers", () => {
  it("sleeps durably: nothing runs until the wake time, then it continues", async () => {
    const wf = defineWorkflow<null, string>("nap", async (ctx) => {
      await ctx.sleep("cool-down", 10_000);
      return "awake";
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    let run = await engine.settle(id);
    expect(run.status).toBe("sleeping");
    expect(run.wakeAt).toBe(T0 + 10_000);

    advance(9_999);
    expect((await engine.tick(id)).status).toBe("sleeping");

    advance(1);
    run = await engine.settle(id);
    expect(run.status).toBe("completed");
    expect(run.history.some((e) => e.type === "timer.fired")).toBe(true);
  });
});

describe("safety", () => {
  it("detects workflow code that changed under a live run", async () => {
    const v1 = defineWorkflow<null, void>("evolving", async (ctx) => {
      await ctx.step("a", () => 1);
      await ctx.sleep("pause", 1_000);
      await ctx.step("b", () => 2);
    });
    const { engine, store, advance } = harness([v1]);
    const id = await engine.start(v1, null);
    await engine.settle(id);

    // Deploy v2 with a renamed first step while the run is mid-flight.
    const v2 = defineWorkflow<null, void>("evolving", async (ctx) => {
      await ctx.step("a-renamed", () => 1);
      await ctx.sleep("pause", 1_000);
      await ctx.step("b", () => 2);
    });
    const { Engine } = await import("../src/index.js");
    const engine2 = new Engine({ store, workflows: [v2], now: () => T0 + 1_000 });
    advance(1_000);
    const run = await engine2.settle(id);

    expect(run.status).toBe("failed");
    expect(run.error).toMatch(/"a" in history but "a-renamed" now/);
  });

  it("fails loudly when workflow code swallows a suspension", async () => {
    const wf = defineWorkflow<null, string>("swallower", async (ctx) => {
      try {
        await ctx.waitFor("x");
      } catch {
        /* the mistake */
      }
      return "pretend everything is fine";
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);
    expect(run.status).toBe("failed");
    expect(run.error).toMatch(/try\/catch/);
  });

  it("lets only one worker execute a leased run", async () => {
    const wf = defineWorkflow<null, string>("contended", async (ctx) => {
      await ctx.sleep("z", 1);
      return "done";
    });
    const { engine } = harness([wf], { leaseMs: 30_000 });
    const id = await engine.start(wf, null);
    await engine.settle(id); // now sleeping with wakeAt = T0 + 1

    // Simulate worker A holding the lease…
    const run = (await engine.get(id))!;
    run.leaseUntil = T0 + 30_000;
    await engine["store"].save(run, run.version);

    // …and worker B trying to tick the same run once it is due.
    const { advance } = harness([wf]);
    void advance;
    await expect(engine.tick(id)).resolves.toMatchObject({ status: "sleeping" }); // not due yet
  });

  it("rejects a stale write", async () => {
    const wf = defineWorkflow<null, number>("v", async () => 1);
    const { engine, store } = harness([wf]);
    const id = await engine.start(wf, null);
    const a = (await store.get(id))!;
    const b = (await store.get(id))!;
    expect(await store.save(a, a.version)).toBe(true);
    expect(await store.save(b, b.version)).toBe(false);
    await expect(engine.settle(id)).resolves.toMatchObject({ status: "completed" });
    void ConflictError;
  });

  it("cancels a waiting run", async () => {
    const wf = defineWorkflow<null, void>("cancellable", async (ctx) => {
      await ctx.waitFor("never");
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    await engine.settle(id);
    const run = await engine.cancel(id);
    expect(run.status).toBe("canceled");
    await expect(engine.signal(id, "never")).rejects.toThrow(/canceled/);
  });
});

describe("workflow versions", () => {
  const v1 = defineWorkflow<null, string>("greet", async (ctx) => {
    const who = await ctx.step("load", () => "world");
    await ctx.waitFor("go");
    return `hello ${who}`;
  });

  // A v2 that would replay v1's history wrongly: the step at position 0 is
  // named differently, and the greeting it returns is not v1's.
  const v2 = defineWorkflow<null, string>(
    "greet",
    async (ctx) => {
      const who = await ctx.step("load-contact", () => "WORLD");
      await ctx.waitFor("go");
      return `HELLO ${who}`;
    },
    { version: 2 },
  );

  it("replays a live run on the version it started on, not the one just deployed", async () => {
    const { engine, deploy } = harness([v1]);
    const id = await engine.start(v1, null);
    await engine.settle(id); // step done, waiting on "go"

    const deployed = deploy([v1, v2]);
    const run = await deployed.signal(id, "go");

    expect(run.status).toBe("completed");
    expect(run.output).toBe("hello world");
    expect(run.workflowVersion).toBe(1);
  });

  it("starts a run by name on the highest registered version", async () => {
    const { engine } = harness([v1, v2]);
    const id = await engine.start("greet", null);
    await engine.settle(id);

    expect((await engine.get(id))?.workflowVersion).toBe(2);
    expect((await engine.view(id))?.workflowVersion).toBe(2);
    expect((await engine.signal(id, "go")).output).toBe("HELLO WORLD");
  });

  it("starts a run on the version of the definition it was handed", async () => {
    const { engine } = harness([v1, v2]);
    const id = await engine.start(v1, null);
    expect((await engine.get(id))?.workflowVersion).toBe(1);
  });

  it("still detects code that changed under a live run within one version", async () => {
    const { engine, deploy } = harness([v1]);
    const id = await engine.start(v1, null);
    await engine.settle(id);

    // v2's body shipped without the version bump that makes it a new version.
    const forgotToBump = defineWorkflow<null, string>("greet", async (ctx) => {
      const who = await ctx.step("load-contact", () => "WORLD");
      await ctx.waitFor("go");
      return `HELLO ${who}`;
    });
    const run = await deploy([forgotToBump]).signal(id, "go");

    expect(run.status).toBe("failed");
    expect(run.error).toMatch(/"load" in history but "load-contact" now/);
  });

  it("refuses two definitions of one name at the same version", async () => {
    expect(() => harness([v1, v1])).toThrow(/"greet" is registered twice at version 1/);
  });

  it("fails loudly when the version a run started on is no longer registered", async () => {
    const { engine, deploy } = harness([v1]);
    const id = await engine.start(v1, null);

    await expect(deploy([v2]).tick(id)).rejects.toThrow(/"greet" version 1 is not registered/);
  });

  it("replays a run recorded before versions existed on version 1", async () => {
    const { engine, store } = harness([v1, v2]);
    const id = await engine.start(v1, null);

    const legacy = (await store.get(id))!;
    delete legacy.workflowVersion;
    await store.save(legacy, legacy.version);
    await engine.settle(id);

    expect((await engine.get(id))?.history[0]).toMatchObject({ name: "load" });
  });

  describe("children", () => {
    const emitV1 = defineWorkflow<null, string>("emit", async (ctx) => ctx.step("emit", () => "from v1"));
    const emitV2 = defineWorkflow<null, string>("emit", async (ctx) => ctx.step("emit", () => "from v2"), {
      version: 2,
    });
    const byName = defineWorkflow<null, string>("by-name", async (ctx) =>
      ctx.waitForChild(await ctx.startChild("emit", null)),
    );
    const pinned = defineWorkflow<null, string>("pinned", async (ctx) =>
      ctx.waitForChild(await ctx.startChild(emitV1, null)),
    );

    it("starts a child named by string on the latest version, and one named by definition on its own", async () => {
      const { engine, drain } = harness([emitV1, emitV2, byName, pinned]);
      const latest = await engine.start(byName, null, { id: "a" });
      const old = await engine.start(pinned, null, { id: "b" });
      await drain();

      expect((await engine.get(latest))?.output).toBe("from v2");
      expect((await engine.get("a#0"))?.workflowVersion).toBe(2);
      expect((await engine.get(old))?.output).toBe("from v1");
      expect((await engine.get("b#0"))?.workflowVersion).toBe(1);
    });
  });
});

describe("worker", () => {
  it("processDue leases everything due and executes it once", async () => {
    let executions = 0;
    const wf = defineWorkflow<null, void>("job", async (ctx) => {
      await ctx.step("work", () => void executions++);
    });
    const { engine } = harness([wf]);
    await engine.start(wf, null);
    await engine.start(wf, null);
    await engine.start(wf, null);

    expect(await engine.processDue(10)).toBe(3);
    expect(await engine.processDue(10)).toBe(0);
    expect(executions).toBe(3);
  });

  it("the polling worker drains due runs and stops cleanly", async () => {
    const wf = defineWorkflow<null, number>("w", async (ctx) => ctx.step("x", () => 42));
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const handle = engine.worker({ pollMs: 5 });
    await new Promise((r) => setTimeout(r, 30));
    await handle.stop();
    expect((await engine.get(id))?.output).toBe(42);
  });
});

describe("event-driven wakeups", () => {
  const job = defineWorkflow<null, number>("job", async (ctx) => ctx.step("work", () => 42));

  /**
   * A store with wakeups the test sends by hand: MemoryStore plus the one
   * method `worker()` looks for, and a count of the passes it has answered so
   * a test can tell a worker that is waiting from one that has not started.
   */
  class WatchedStore extends MemoryStore implements WakeupSource {
    readonly watchers: ((wake: Wakeup) => void)[] = [];
    passes = 0;
    closed = 0;
    /** Run after a pass has been answered, to write behind a worker's back. */
    afterClaim: (() => Promise<void>) | undefined;

    async watch(onWake: (wake: Wakeup) => void): Promise<WakeupSubscription> {
      this.watchers.push(onWake);
      return {
        close: async () => {
          this.closed++;
        },
      };
    }

    override async claimDue(now: number, leaseMs: number, limit: number): Promise<RunRecord[]> {
      const claimed = await super.claimDue(now, leaseMs, limit);
      this.passes++;
      await this.afterClaim?.();
      return claimed;
    }

    push(wake: Wakeup): void {
      for (const onWake of this.watchers) onWake(wake);
    }
  }

  it("wakes on a wakeup instead of waiting the poll interval out", async () => {
    const store = new WatchedStore();
    const engine = new Engine({ store, workflows: [job] });
    const handle = engine.worker({ pollMs: 60_000 });
    try {
      await until(() => store.passes >= 1); // nothing to do; the worker is waiting
      const id = await engine.start(job, null);
      store.push({ runId: id, wakeAt: null });

      await until(async () => (await engine.get(id))?.status === "completed");
      expect((await engine.get(id))?.output).toBe(42);
    } finally {
      await handle.stop();
    }
    expect(store.closed).toBe(1); // the subscription is the worker's, and ends with it
  });

  it("wakes at a wake time it was told in advance, which nothing writes when it arrives", async () => {
    const napper = defineWorkflow<null, string>("napper", async (ctx) => {
      await ctx.sleep("nap", 150);
      return ctx.step("after", () => "awake");
    });
    const store = new WatchedStore();
    const engine = new Engine({ store, workflows: [napper] });
    const id = await engine.start(napper, null);
    await engine.settle(id);
    const sleeping = await engine.get(id);
    expect(sleeping?.status).toBe("sleeping");

    // The worker is handed the wake time and nothing else. No write happens
    // when it arrives, so a worker that only reacts to wakeups sent in the
    // moment would sit here for the whole poll interval.
    const handle = engine.worker({ pollMs: 60_000 });
    try {
      await until(() => store.passes >= 1);
      store.push({ runId: id, wakeAt: sleeping?.wakeAt ?? null });

      await until(async () => (await engine.get(id))?.status === "completed");
      expect((await engine.get(id))?.output).toBe("awake");
    } finally {
      await handle.stop();
    }
  });

  it("keeps a wakeup that arrives while a pass is already running", async () => {
    const store = new WatchedStore();
    const engine = new Engine({ store, workflows: [job] });
    let id = "";
    // Written after the pass has already claimed, so this pass misses it and
    // the wakeup lands on a worker that is not yet waiting for one.
    store.afterClaim = async () => {
      store.afterClaim = undefined;
      id = await engine.start(job, null);
      store.push({ runId: id, wakeAt: null });
    };

    const handle = engine.worker({ pollMs: 60_000 });
    try {
      await until(async () => id !== "" && (await engine.get(id))?.status === "completed");
    } finally {
      await handle.stop();
    }
  });

  it("still moves a run on the poll interval when no wakeup arrives", async () => {
    const store = new WatchedStore();
    const engine = new Engine({ store, workflows: [job] });
    const handle = engine.worker({ pollMs: 5 });
    try {
      await until(() => store.passes >= 1);
      const id = await engine.start(job, null); // and nothing is pushed

      await until(async () => (await engine.get(id))?.status === "completed");
    } finally {
      await handle.stop();
    }
  });

  it("leaves the store unsubscribed when the worker is told to poll only", async () => {
    const store = new WatchedStore();
    const engine = new Engine({ store, workflows: [job] });
    const handle = engine.worker({ pollMs: 5, events: false });
    try {
      const id = await engine.start(job, null);
      await until(async () => (await engine.get(id))?.status === "completed");
      expect(store.watchers).toEqual([]);
    } finally {
      await handle.stop();
    }
  });

  describe("what a store sends a wakeup for", () => {
    it("is a run nobody holds that is due now or at a time it can name", async () => {
      const waiter = defineWorkflow<null, void>("waiter", async (ctx) => {
        await ctx.waitFor("go");
      });
      const { engine, store } = harness([waiter]);
      const id = await engine.start(waiter, null);

      const fresh = (await store.get(id))!;
      expect(wakeupFor(fresh)).toEqual({ runId: id, wakeAt: null });
      expect(wakeupFor({ ...fresh, leaseUntil: T0 + 30_000 })).toBeNull();
      expect(wakeupFor({ ...fresh, status: "completed" })).toBeNull();

      // Waiting with no deadline: there is no time to name, and calling that
      // "due now" would have every worker claim it on every pass.
      await engine.settle(id);
      const waiting = (await store.get(id))!;
      expect(waiting.status).toBe("waiting");
      expect(wakeupFor(waiting)).toBeNull();
    });

    it("names the wake time of a run that is asleep on a timer", async () => {
      const napper = defineWorkflow<null, void>("napper", async (ctx) => {
        await ctx.sleep("nap", 1_000);
      });
      const { engine, store } = harness([napper]);
      const id = await engine.start(napper, null);
      await engine.settle(id);

      expect(wakeupFor((await store.get(id))!)).toEqual({ runId: id, wakeAt: T0 + 1_000 });
    });
  });
});

describe("listing", () => {
  const counter = defineWorkflow<null, number>("counter", async (ctx) => ctx.step("one", () => 1));
  const waiter = defineWorkflow<null, void>("waiter", async (ctx) => {
    await ctx.waitFor("go");
  });

  it("filters by workflow and by status", async () => {
    const { engine } = harness([counter, waiter]);
    await engine.start(counter, null, { id: "c1" });
    await engine.start(counter, null, { id: "c2" });
    const w = await engine.start(waiter, null, { id: "w1" });
    await engine.settle(w);

    expect((await engine.list({ workflow: "counter" })).runs.map((r) => r.id)).toEqual(["c2", "c1"]);
    expect((await engine.list({ status: "waiting" })).runs.map((r) => r.id)).toEqual(["w1"]);
    expect((await engine.list({ workflow: "counter", status: "waiting" })).runs).toEqual([]);
    expect((await engine.list()).runs).toHaveLength(3);
  });

  it("pages through every run exactly once when they share a creation time", async () => {
    const { engine } = harness([counter]);
    for (const id of ["r1", "r2", "r3", "r4", "r5"]) await engine.start(counter, null, { id });

    const seen: string[] = [];
    let cursor: string | null = null;
    for (let page = 0; page < 5; page++) {
      const result: RunPage = await engine.list(cursor === null ? { limit: 2 } : { limit: 2, cursor });
      seen.push(...result.runs.map((r) => r.id));
      cursor = result.cursor;
      if (cursor === null) break;
    }

    // Identical createdAt, so only the id tiebreak keeps the boundary exact.
    expect(seen).toEqual(["r5", "r4", "r3", "r2", "r1"]);
    expect(cursor).toBeNull();
  });

  it("does not repeat a run when new ones are created mid-page", async () => {
    const { engine } = harness([counter]);
    for (const id of ["r1", "r2", "r3", "r4"]) await engine.start(counter, null, { id });

    const first = await engine.list({ limit: 2 });
    expect(first.runs.map((r) => r.id)).toEqual(["r4", "r3"]);

    // Sorts ahead of the first page: an offset would push r3 down into page two.
    await engine.start(counter, null, { id: "r9" });

    const second = await engine.list({ limit: 2, cursor: first.cursor ?? "" });
    expect(second.runs.map((r) => r.id)).toEqual(["r2", "r1"]);
    expect(second.cursor).toBeNull();
  });

  it("clamps the page size and rejects a cursor it did not issue", async () => {
    const { engine } = harness([counter]);
    await engine.start(counter, null, { id: "r1" });
    await engine.start(counter, null, { id: "r2" });

    expect((await engine.list({ limit: 0 })).runs).toHaveLength(1);
    expect((await engine.list({ limit: 10_000 })).runs).toHaveLength(2);
    await expect(engine.list({ cursor: "not-a-cursor" })).rejects.toThrow(/invalid cursor/);
  });
});

describe("tags", () => {
  const counter = defineWorkflow<null, number>("counter", async (ctx) => ctx.step("one", () => 1));
  const waiter = defineWorkflow<null, void>("waiter", async (ctx) => {
    await ctx.waitFor("go");
  });

  it("finds the run for an order without knowing its id", async () => {
    const { engine } = harness([counter, waiter]);
    await engine.start(counter, null, { id: "c1", tags: ["order:4182", "tenant:acme"] });
    await engine.start(counter, null, { id: "c2", tags: ["order:9001", "tenant:acme"] });
    await engine.start(waiter, null, { id: "w1", tags: ["order:4182"] });

    expect((await engine.list({ tag: "order:4182" })).runs.map((r) => r.id)).toEqual(["w1", "c1"]);
    expect((await engine.list({ tag: "tenant:acme" })).runs.map((r) => r.id)).toEqual(["c2", "c1"]);
    expect((await engine.list({ tag: "order:nope" })).runs).toEqual([]);
  });

  it("narrows by tag alongside workflow and status", async () => {
    const { engine } = harness([counter, waiter]);
    await engine.start(counter, null, { id: "c1", tags: ["order:1"] });
    const w = await engine.start(waiter, null, { id: "w1", tags: ["order:1"] });
    // Same workflow and same status as w1, different order: only the tag parts them.
    const other = await engine.start(waiter, null, { id: "w2", tags: ["order:2"] });
    await engine.settle(w);
    await engine.settle(other);

    expect((await engine.list({ tag: "order:1", workflow: "waiter" })).runs.map((r) => r.id)).toEqual(["w1"]);
    expect((await engine.list({ tag: "order:1", status: "waiting" })).runs.map((r) => r.id)).toEqual(["w1"]);
    expect((await engine.list({ tag: "order:1", workflow: "counter", status: "waiting" })).runs).toEqual([]);
  });

  it("leaves an untagged run out of every tag query but not out of the listing", async () => {
    const { engine } = harness([counter]);
    await engine.start(counter, null, { id: "plain" });
    await engine.start(counter, null, { id: "tagged", tags: ["order:1"] });

    expect((await engine.get("plain"))?.tags).toBeUndefined();
    expect((await engine.list({ tag: "order:1" })).runs.map((r) => r.id)).toEqual(["tagged"]);
    expect((await engine.list()).runs.map((r) => r.id)).toEqual(["tagged", "plain"]);
  });

  it("stores tags trimmed, deduplicated and sorted, and matches a query the same way", async () => {
    const { engine } = harness([counter]);
    await engine.start(counter, null, { id: "r1", tags: ["  tenant:acme ", "order:1", "tenant:acme"] });

    expect((await engine.get("r1"))?.tags).toEqual(["order:1", "tenant:acme"]);
    expect((await engine.list({ tag: " order:1  " })).runs.map((r) => r.id)).toEqual(["r1"]);
  });

  it("narrows to the runs carrying every tag in a set", async () => {
    const { engine } = harness([counter, waiter]);
    await engine.start(counter, null, { id: "c1", tags: ["order:4182", "tenant:acme"] });
    await engine.start(counter, null, { id: "c2", tags: ["order:4182", "tenant:other"] });
    await engine.start(waiter, null, { id: "w1", tags: ["tenant:acme"] });

    // Each tag on its own matches two runs; together they match the one run
    // that carries both, which is the question an operator holding a tenant
    // and an order is actually asking.
    expect((await engine.list({ tag: "order:4182" })).runs.map((r) => r.id)).toEqual(["c2", "c1"]);
    expect((await engine.list({ tag: "tenant:acme" })).runs.map((r) => r.id)).toEqual(["w1", "c1"]);
    expect((await engine.list({ tag: ["order:4182", "tenant:acme"] })).runs.map((r) => r.id)).toEqual(["c1"]);
    // A tag the run does not carry takes it out, however many of the rest match.
    expect((await engine.list({ tag: ["order:4182", "tenant:acme", "nope"] })).runs).toEqual([]);
  });

  it("matches a set alongside workflow, status and the single-tag spelling", async () => {
    const { engine } = harness([counter, waiter]);
    const c = await engine.start(counter, null, { id: "c1", tags: ["order:1", "tenant:acme"] });
    const w = await engine.start(waiter, null, { id: "w1", tags: ["order:1", "tenant:acme"] });
    // Tagged identically, so only workflow and status can part them.
    await engine.settle(c);
    await engine.settle(w);

    expect((await engine.list({ tag: ["order:1", "tenant:acme"], workflow: "waiter" })).runs.map((r) => r.id)).toEqual(
      ["w1"],
    );
    expect((await engine.list({ tag: ["order:1", "tenant:acme"], status: "completed" })).runs.map((r) => r.id)).toEqual(
      ["c1"],
    );
    // One tag in a set is the same query as that tag on its own, and a set
    // is trimmed, deduplicated and order-insensitive like a run's own tags.
    expect((await engine.list({ tag: ["order:1"] })).runs.map((r) => r.id)).toEqual(["w1", "c1"]);
    expect((await engine.list({ tag: [" tenant:acme ", "order:1", "order:1"] })).runs.map((r) => r.id)).toEqual([
      "w1",
      "c1",
    ]);
    // Narrowing by nothing is not narrowing: an empty set is no tag filter,
    // which is what a query assembled from blank operator filters means.
    expect((await engine.list({ tag: [] })).runs.map((r) => r.id)).toEqual(["w1", "c1"]);
  });

  it("pages a set of tags by keyset, without repeating or skipping a run", async () => {
    const { engine } = harness([counter]);
    for (const id of ["r1", "r2", "r3", "r4", "r5"]) {
      // Every run carries "all", so the set is only narrowed by the second
      // tag — and a store filtering it after the page limit rather than
      // inside the query would hand back short pages and a cursor that skips.
      const odd = Number(id.slice(1)) % 2 === 1;
      await engine.start(counter, null, { id, tags: odd ? ["all", "odd"] : ["all", "even"] });
    }

    const first = await engine.list({ tag: ["all", "odd"], limit: 2 });
    expect(first.runs.map((r) => r.id)).toEqual(["r5", "r3"]);
    const second = await engine.list({ tag: ["all", "odd"], limit: 2, cursor: first.cursor ?? "" });
    expect(second.runs.map((r) => r.id)).toEqual(["r1"]);
    expect(second.cursor).toBeNull();
  });

  it("refuses a tag it could not index rather than dropping it", async () => {
    const { engine } = harness([counter]);
    const startWith = (tags: string[]) => engine.start(counter, null, { tags });

    await expect(startWith(["ok", "   "])).rejects.toThrow(/tag cannot be empty/);
    await expect(startWith(["x".repeat(MAX_TAG_LENGTH + 1)])).rejects.toThrow(
      new RegExp(`longer than ${MAX_TAG_LENGTH}`),
    );
    await expect(startWith(Array.from({ length: MAX_TAGS + 1 }, (_, i) => `t${i}`))).rejects.toThrow(/at most/);
    // Refused at the door: nothing was created under a tag that would not match.
    expect((await engine.list()).runs).toEqual([]);
    await expect(engine.list({ tag: " " })).rejects.toThrow(/tag cannot be empty/);
    // A set is held to the same rules, so a query cannot ask for a spelling
    // no run could have been created under.
    await expect(engine.list({ tag: ["ok", " "] })).rejects.toThrow(/tag cannot be empty/);
    await expect(engine.list({ tag: Array.from({ length: MAX_TAGS + 1 }, (_, i) => `t${i}`) })).rejects.toThrow(
      /at most/,
    );
  });

  it("pages a tag query by keyset, without repeating or skipping a run", async () => {
    const { engine } = harness([counter]);
    for (const id of ["r1", "r2", "r3", "r4", "r5"]) {
      // Every other run carries the tag, so a filter applied after the page
      // limit — rather than inside the query — would short the pages.
      await engine.start(counter, null, { id, tags: Number(id.slice(1)) % 2 === 1 ? ["odd"] : ["even"] });
    }

    const first = await engine.list({ tag: "odd", limit: 2 });
    expect(first.runs.map((r) => r.id)).toEqual(["r5", "r3"]);
    const second = await engine.list({ tag: "odd", limit: 2, cursor: first.cursor ?? "" });
    expect(second.runs.map((r) => r.id)).toEqual(["r1"]);
    expect(second.cursor).toBeNull();
  });

  it("carries tags into the generations a continuation hands the work to", async () => {
    const batches = defineWorkflow<number, number>("batches", async (ctx, left) => {
      await ctx.step("batch", () => left);
      return left > 1 ? ctx.continueAsNew(left - 1) : left;
    });
    const { engine } = harness([batches]);
    await engine.start(batches, 3, { id: "r1", tags: ["order:4182"] });
    expect((await engine.settle("r1")).id).toBe("r1~3");

    // The chain is one piece of work, so the tag an operator searches by has to
    // reach the generation actually doing it — not only the one they started.
    expect((await engine.get("r1~3"))?.tags).toEqual(["order:4182"]);
    expect((await engine.list({ tag: "order:4182" })).runs.map((r) => r.id)).toEqual(["r1~3", "r1~2", "r1"]);
  });

  it("rewrites a run's tags, so an operator finds it under the name it now has", async () => {
    const { engine } = harness([counter]);
    await engine.start(counter, null, { id: "r1", tags: ["order:1", "tenant:acme"] });

    // The order turned out to belong to another account.
    const run = await engine.retag("r1", ["tenant:other", "order:1"]);

    expect(run.tags).toEqual(["order:1", "tenant:other"]);
    expect((await engine.get("r1"))?.tags).toEqual(["order:1", "tenant:other"]);
    // The name it no longer carries stops answering for it, which is the half
    // of a retag that a write to the record alone would not do.
    expect((await engine.list({ tag: "tenant:acme" })).runs).toEqual([]);
    expect((await engine.list({ tag: ["order:1", "tenant:other"] })).runs.map((r) => r.id)).toEqual(["r1"]);
  });

  it("leaves a run untagged when a retag names no tags at all", async () => {
    const { engine } = harness([counter]);
    await engine.start(counter, null, { id: "r1", tags: ["order:1"] });

    await engine.retag("r1", []);

    // Absent rather than empty: the shape an untagged run has always had, so
    // no reader ends up with two ways to spell "no tags".
    expect((await engine.get("r1"))?.tags).toBeUndefined();
    expect((await engine.list({ tag: "order:1" })).runs).toEqual([]);
    expect((await engine.list()).runs.map((r) => r.id)).toEqual(["r1"]);
  });

  it("refuses a tag it could not index, leaving the tags the run had", async () => {
    const { engine } = harness([counter]);
    await engine.start(counter, null, { id: "r1", tags: ["order:1"] });

    await expect(engine.retag("r1", ["ok", "   "])).rejects.toThrow(/tag cannot be empty/);
    await expect(engine.retag("r1", ["x".repeat(MAX_TAG_LENGTH + 1)])).rejects.toThrow(/is longer than/);
    await expect(engine.retag("r1", Array.from({ length: MAX_TAGS + 1 }, (_, i) => `t${i}`))).rejects.toThrow(
      /at most 16 tags/,
    );

    // Refused at the door, as at start: a retag that dropped the tag it could
    // not index would leave the run under a name nobody can predict.
    expect((await engine.get("r1"))?.tags).toEqual(["order:1"]);
    expect((await engine.list({ tag: "order:1" })).runs.map((r) => r.id)).toEqual(["r1"]);
  });

  it("writes nothing when a retag names the tags the run already carries", async () => {
    const { engine, advance } = harness([counter]);
    await engine.start(counter, null, { id: "r1", tags: ["order:1", "tenant:acme"] });
    const before = await engine.get("r1");
    advance(1_000);

    // Spelled differently, the same set: a write here would cost whatever
    // worker is holding the run its pass, to rewrite an index into the state
    // it was already in.
    await engine.retag("r1", [" tenant:acme ", "order:1", "order:1"]);

    const after = await engine.get("r1");
    expect(after?.version).toBe(before?.version);
    expect(after?.updatedAt).toBe(before?.updatedAt);
    expect(after?.tags).toEqual(["order:1", "tenant:acme"]);
  });

  it("renames the generation doing the work, not the one that started the chain", async () => {
    const batches = defineWorkflow<number, number>("batches", async (ctx, left) => {
      await ctx.waitFor("go");
      return left > 1 ? ctx.continueAsNew(left - 1) : left;
    });
    const { engine } = harness([batches]);
    await engine.start(batches, 3, { id: "r1", tags: ["order:4182"] });
    await engine.settle("r1");
    await engine.signal("r1", "go");
    expect((await engine.settle("r1")).id).toBe("r1~2");

    // Addressed by the id the operator kept — the one they started — and
    // applied to the generation that is actually waiting on the work.
    await engine.retag("r1", ["order:4182", "tenant:acme"]);

    expect((await engine.get("r1~2"))?.tags).toEqual(["order:4182", "tenant:acme"]);
    // The generation that handed off keeps what it ran under, which is the
    // only account of what the run was called at the time.
    expect((await engine.get("r1"))?.tags).toEqual(["order:4182"]);
    expect((await engine.list({ tag: "tenant:acme" })).runs.map((r) => r.id)).toEqual(["r1~2"]);

    // And the generation after it inherits the names the live one now carries.
    await engine.signal("r1", "go");
    expect((await engine.settle("r1")).id).toBe("r1~3");
    expect((await engine.get("r1~3"))?.tags).toEqual(["order:4182", "tenant:acme"]);
    expect((await engine.list({ tag: "tenant:acme" })).runs.map((r) => r.id)).toEqual(["r1~3", "r1~2"]);
  });

  it("renames a run that has already finished", async () => {
    const { engine } = harness([counter]);
    await engine.start(counter, null, { id: "r1", tags: ["order:1"] });
    expect((await engine.settle("r1")).status).toBe("completed");

    // The names index the record, which outlives the run: a run mislabelled
    // while it ran is still the one an operator goes looking for afterwards.
    await engine.retag("r1", ["order:4182"]);

    expect((await engine.list({ tag: "order:4182" })).runs.map((r) => r.id)).toEqual(["r1"]);
    expect((await engine.list({ tag: "order:1" })).runs).toEqual([]);
  });

  it("shows a found run's tags on its view", async () => {
    const { engine } = harness([counter]);
    await engine.start(counter, null, { id: "r1", tags: ["order:1"] });
    await engine.start(counter, null, { id: "r2" });

    expect((await engine.view("r1"))?.tags).toEqual(["order:1"]);
    expect((await engine.view("r2"))?.tags).toEqual([]);
  });
});

describe("run view", () => {
  const fulfilment = defineWorkflow<{ order: string }, string>("fulfilment", async (ctx) => {
    await ctx.step("charge", () => "rcpt_1");
    await ctx.sleep("cool-off", 1_000);
    const review = await ctx.waitFor<{ approved: boolean }>("review");
    return review.approved ? "shipped" : "held";
  });

  it("renders history as a timeline of offsets from the start", async () => {
    const { engine, advance } = harness([fulfilment]);
    const id = await engine.start(fulfilment, { order: "ord_42" }, { id: "run-1" });
    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);
    advance(500);
    await engine.signal(id, "review", { approved: true });

    const view = await engine.view(id);
    expect(view?.status).toBe("completed");
    expect(view?.output).toBe("shipped");
    expect(view?.input).toEqual({ order: "ord_42" });
    expect(view?.durationMs).toBe(1_500);
    expect(view?.blockedOn).toBeNull();
    expect(view?.timeline.map((e) => [e.seq, e.elapsedMs, e.summary])).toEqual([
      [0, 0, 'step "charge" completed'],
      [1, 1_000, 'timer "cool-off" fired'],
      [2, 1_500, 'signal "review" received'],
    ]);
    expect(JSON.parse(JSON.stringify(view))).toEqual(view); // an admin endpoint can send it as-is
  });

  it("says what a sleeping run is blocked on, and until when", async () => {
    const { engine } = harness([fulfilment]);
    const id = await engine.start(fulfilment, { order: "ord_1" });
    await engine.settle(id);

    expect((await engine.view(id))?.blockedOn).toEqual({ kind: "timer", name: "cool-off", until: T0 + 1_000 });
  });

  it("says which signal a waiting run wants, and its deadline", async () => {
    const wf = defineWorkflow<null, void>("approval", async (ctx) => {
      await ctx.waitFor("sign-off", { timeoutMs: 5_000 });
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    await engine.settle(id);

    expect((await engine.view(id))?.blockedOn).toEqual({ kind: "signal", name: "sign-off", until: T0 + 5_000 });
  });

  it("distinguishes a retry backoff from a sleep, and reports the failure", async () => {
    const wf = defineWorkflow<null, string>("flaky", async (ctx) =>
      ctx.step("call-api", () => {
        throw new Error("boom");
      }),
    );
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    await engine.settle(id);

    const view = await engine.view(id);
    expect(view?.status).toBe("sleeping");
    expect(view?.blockedOn).toEqual({ kind: "retry", name: "call-api", until: T0 + 1_000 });
    expect(view?.timeline[0]?.summary).toBe('step "call-api" failed on attempt 1, retrying: boom');
  });

  it("counts signals buffered ahead of their waitFor", async () => {
    const wf = defineWorkflow<null, void>("inbox", async (ctx) => {
      await ctx.waitFor("mail");
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    await engine.signal(id, "mail", "first");
    await engine.signal(id, "mail", "second");

    expect((await engine.view(id))?.pendingSignals).toEqual({ mail: 2 });
    expect(await engine.view("no-such-run")).toBeNull();
  });
});

describe("child workflows", () => {
  const double = defineWorkflow<{ n: number }, number>("double", async (ctx, input) =>
    ctx.step("multiply", () => input.n * 2),
  );

  const supervisor = defineWorkflow<{ n: number }, number>("supervisor", async (ctx, input) => {
    const child = await ctx.startChild(double, { n: input.n });
    return ctx.waitForChild(child);
  });

  it("runs a child as its own run and hands the output back to the parent", async () => {
    const { engine, drain } = harness([supervisor, double]);
    const id = await engine.start(supervisor, { n: 21 }, { id: "p1" });
    await drain();

    const run = await engine.get(id);
    expect(run?.status).toBe("completed");
    expect(run?.output).toBe(42);

    const child = await engine.get("p1#0");
    expect(child?.workflow).toBe("double");
    expect(child?.parent).toEqual({ runId: "p1", signal: "child:p1#0" });
    expect((await engine.view(id))?.timeline[0]?.summary).toBe('child "double" started as run p1#0');
  });

  it("does not lose a child that finishes before the parent waits for it", async () => {
    const wf = defineWorkflow<null, number>("slow-supervisor", async (ctx) => {
      const child = await ctx.startChild(double, { n: 5 });
      await ctx.sleep("paperwork", 10_000);
      return ctx.waitForChild(child);
    });
    const { engine, drain, advance } = harness([wf, double]);
    const id = await engine.start(wf, null, { id: "p1" });

    await drain(); // the child finishes while the parent is still asleep
    expect((await engine.get(id))?.pendingSignals).toEqual({ "child:p1#0": [{ status: "completed", output: 10 }] });

    advance(10_000);
    await drain();
    expect((await engine.get(id))?.output).toBe(10);
  });

  it("starts the child once when a crash lost the record of the start", async () => {
    let started = 0;
    const counted = defineWorkflow<null, number>("counted", async (ctx) => ctx.step("work", () => ++started));
    const wf = defineWorkflow<null, number>("parent", async (ctx) => {
      const child = await ctx.startChild(counted, null);
      return ctx.waitForChild(child);
    });
    const { engine, store, drain } = harness([wf, counted]);
    const id = await engine.start(wf, null, { id: "p1" });
    await engine.tick(id); // creates the child, records it, then waits

    // Rewind the parent to the instant before that event was persisted: the
    // child exists, the parent has no memory of starting it.
    const parent = (await store.get(id))!;
    parent.history = parent.history.filter((e) => e.type !== "child.started");
    parent.status = "running";
    parent.waitingFor = null;
    await store.save(parent, parent.version);

    await drain();
    expect(store.all().filter((r) => r.workflow === "counted").map((r) => r.id)).toEqual(["p1#0"]);
    expect(started).toBe(1);
    expect((await engine.get(id))?.output).toBe(1);
  });

  it("throws a failed child into the parent, where it can be compensated", async () => {
    const doomed = defineWorkflow<null, void>("doomed", async (ctx) => {
      await ctx.step("explode", () => { throw new Error("kaboom"); }, { retry: { maxAttempts: 1 } });
    });
    const wf = defineWorkflow<null, string>("careful", async (ctx) => {
      const child = await ctx.startChild(doomed, null);
      try {
        await ctx.waitForChild(child);
        return "child succeeded";
      } catch (err) {
        if (!(err instanceof ChildFailedError)) throw err;
        await ctx.step("compensate", () => undefined);
        return `${err.status}: ${err.reason}`;
      }
    });
    const { engine, drain } = harness([wf, doomed]);
    const id = await engine.start(wf, null);
    await drain();

    const run = await engine.get(id);
    expect(run?.status).toBe("completed");
    expect(run?.output).toMatch(/^failed: step "explode" failed after 1 attempt/);
  });

  it("wakes a parent whose child was canceled, rather than leaving it waiting", async () => {
    const patient = defineWorkflow<null, void>("patient-child", async (ctx) => {
      await ctx.waitFor("never");
    });
    const wf = defineWorkflow<null, string>("guardian", async (ctx) => {
      const child = await ctx.startChild(patient, null);
      try {
        await ctx.waitForChild(child);
        return "done";
      } catch (err) {
        if (!(err instanceof ChildFailedError)) throw err;
        return err.status;
      }
    });
    const { engine, drain } = harness([wf, patient]);
    const id = await engine.start(wf, null, { id: "p1" });
    await drain();
    expect((await engine.get(id))?.status).toBe("waiting");

    await engine.cancel("p1#0");
    expect((await engine.get(id))?.output).toBe("canceled");
  });

  it("fans out and collects every child", async () => {
    const fan = defineWorkflow<{ ns: number[] }, number[]>("fan-out", async (ctx, input) => {
      const children: ChildHandle<number>[] = [];
      for (const n of input.ns) children.push(await ctx.startChild(double, { n }));
      const results: number[] = [];
      for (const child of children) results.push(await ctx.waitForChild(child));
      return results;
    });
    const { engine, drain } = harness([fan, double]);
    const id = await engine.start(fan, { ns: [1, 2, 3] }, { id: "p1" });
    await drain();

    const run = await engine.get(id);
    expect(run?.output).toEqual([2, 4, 6]);
    expect(run?.history.filter((e) => e.type === "child.started")).toHaveLength(3);
  });

  it("detects a startChild that names a different workflow than history does", async () => {
    const triple = defineWorkflow<{ n: number }, number>("triple", async (ctx, input) =>
      ctx.step("multiply", () => input.n * 3),
    );
    const { engine, store } = harness([supervisor, double, triple]);
    const id = await engine.start(supervisor, { n: 2 }, { id: "p1" });
    await engine.tick(id); // starts "double", then waits on it

    // Deploy a version that starts a different child at the same position.
    const v2 = defineWorkflow<{ n: number }, number>("supervisor", async (ctx, input) => {
      const child = await ctx.startChild(triple, { n: input.n });
      return ctx.waitForChild(child);
    });
    const { Engine } = await import("../src/index.js");
    const engine2 = new Engine({ store, workflows: [v2, double, triple], now: () => T0 });
    // The child finishes on the new deploy and signals the parent, which replays on v2.
    for (let i = 0; i < 5 && (await engine2.processDue(10)) > 0; i++);

    const run = await engine2.get(id);
    expect(run?.status).toBe("failed");
    expect(run?.error).toMatch(/"double" in history but "triple" now/);
  });
});

describe("history compaction", () => {
  /** Long enough to compact: batches of steps separated by durable sleeps, so it replays often. */
  const batched = defineWorkflow<{ batches: number }, number>("batched", async (ctx, input) => {
    let total = 0;
    for (let batch = 0; batch < input.batches; batch++) {
      for (let i = 0; i < 20; i++) total += await ctx.step(`work-${batch}-${i}`, () => 1);
      await ctx.sleep(`pause-${batch}`, 1_000);
    }
    return total;
  });

  /** Run `batched` to completion, reporting the largest history any replay had to scan. */
  async function drive(batches: number, compactAfter: number) {
    const { engine, advance } = harness([batched], { compactAfter });
    const id = await engine.start(batched, { batches });
    let run = await engine.settle(id);
    let peak = run.history.length;
    for (let i = 0; i < batches + 2 && run.status !== "completed"; i++) {
      advance(1_000);
      run = await engine.settle(id);
      peak = Math.max(peak, run.history.length);
    }
    return { run, peak };
  }

  it("stops the history a replay scans growing with the length of the run", async () => {
    const short = await drive(5, 25);
    const long = await drive(40, 25);

    expect(short.run.output).toBe(100);
    expect(long.run.output).toBe(800);
    // The whole point: eight times the work, the same amount of history per tick.
    expect(long.peak).toBe(short.peak);
    expect(long.run.snapshot?.calls).toBe(840);
  });

  it("leaves the run's outcome identical to the same run uncompacted", async () => {
    const compacted = await drive(6, 25);
    const whole = await drive(6, Infinity);

    expect(compacted.run.output).toBe(whole.run.output);
    expect(compacted.run.status).toBe(whole.run.status);
    expect(whole.run.snapshot).toBeUndefined();
    expect(whole.peak).toBe(126); // 6 × (20 steps + 1 timer), all of it scanned on every tick
  });

  it("never runs a step again once its outcome is folded into the snapshot", async () => {
    const ran: string[] = [];
    const wf = defineWorkflow<null, number>("counted", async (ctx) => {
      let total = 0;
      for (let i = 0; i < 6; i++) total += await ctx.step(`s${i}`, () => (ran.push(`s${i}`), i));
      await ctx.sleep("pause", 1_000);
      for (let i = 6; i < 10; i++) total += await ctx.step(`s${i}`, () => (ran.push(`s${i}`), i));
      return total;
    });
    const { engine, advance } = harness([wf], { compactAfter: 3 });
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    const run = await engine.settle(id);

    expect(run.snapshot?.calls).toBe(7); // six steps and the timer they sat behind
    expect(run.history).toHaveLength(4); // only the steps the last pass added
    expect(run.output).toBe(45);
    expect(ran).toEqual(["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"]);
  });

  it("drops the attempts a step's own success superseded", async () => {
    let attempts = 0;
    const wf = defineWorkflow<null, string>("flaky", async (ctx) => {
      const out = await ctx.step("call-api", () => {
        if (++attempts < 3) throw new Error(`boom ${attempts}`);
        return "ok";
      });
      await ctx.sleep("settle", 1_000);
      return out;
    });
    const { engine, advance } = harness([wf], { compactAfter: 1 });
    const id = await engine.start(wf, null);

    await engine.settle(id); // attempt 1 fails
    advance(1_000);
    await engine.settle(id); // attempt 2 fails
    advance(2_000);
    await engine.settle(id); // attempt 3 succeeds, then sleeps
    advance(1_000);
    const run = await engine.settle(id);

    expect(run.output).toBe("ok");
    expect(attempts).toBe(3);
    expect(run.snapshot?.droppedEvents).toBe(2);
    expect(historyEvents(run).filter((e) => e.type === "step.failed")).toEqual([]);
  });

  it("keeps a step still serving its backoff out of the snapshot", async () => {
    const wf = defineWorkflow<null, void>("mixed", async (ctx) => {
      await ctx.step("ok", () => "done");
      await ctx.step("flaky", () => {
        throw new Error("boom");
      });
    });
    const { engine, advance } = harness([wf], { compactAfter: 1 });
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    const run = await engine.settle(id);

    expect(run.snapshot?.calls).toBe(1); // "ok" only: an unfinished call is not settled
    expect(run.history.map((e) => e.type === "step.failed" && e.attempt)).toEqual([1, 2]);
    expect((await engine.view(id))?.blockedOn).toEqual({ kind: "retry", name: "flaky", until: T0 + 3_000 });
  });

  it("renders folded events on the timeline and says what was dropped", async () => {
    const wf = defineWorkflow<null, string>("audited", async (ctx) => {
      try {
        await ctx.step("charge", () => {
          throw new Error("declined");
        }, { retry: { maxAttempts: 2 } });
        return "charged";
      } catch (err) {
        if (!(err instanceof StepFailedError)) throw err;
        await ctx.sleep("cool-off", 1_000);
        return "declined";
      }
    });
    const { engine, advance } = harness([wf], { compactAfter: 1 });
    const id = await engine.start(wf, null);

    await engine.settle(id); // attempt 1 fails, retrying
    advance(1_000);
    await engine.settle(id); // attempt 2 is the last: the call settles as a failure
    advance(1_000);
    await engine.settle(id); // the timer fires and the settled prefix is folded

    const view = await engine.view(id);
    expect(view?.output).toBe("declined");
    expect(view?.compaction).toEqual({ calls: 2, droppedEvents: 1, at: T0 + 2_000 });
    // Sequence numbers are absolute: the dropped attempt leaves a gap rather than renumbering.
    expect(view?.timeline.map((e) => [e.seq, e.summary])).toEqual([
      [1, 'step "charge" failed on attempt 2, no attempts left: declined'],
      [2, 'timer "cool-off" fired'],
    ]);
    expect(JSON.parse(JSON.stringify(view))).toEqual(view); // an admin endpoint can send it as-is
  });

  it("still detects code that changed under a folded call", async () => {
    const audit = defineWorkflow<null, void>("audit", async (ctx) => {
      await ctx.step("a", () => 1);
      await ctx.sleep("pause", 1_000);
      await ctx.step("b", () => 2);
    });
    const renamed = defineWorkflow<null, void>("audit", async (ctx) => {
      await ctx.step("A", () => 1); // renamed while a run sits in the sleep
      await ctx.sleep("pause", 1_000);
      await ctx.step("b", () => 2);
    });
    const { engine, advance, deploy } = harness([audit], { compactAfter: 1 });
    const id = await engine.start(audit, null);

    await engine.settle(id);
    advance(1_000);
    const run = await deploy([renamed]).settle(id);

    expect(run.status).toBe("failed");
    expect(run.error).toMatch(/step at position 0 was "a" in history but "A" now/);
  });
});

describe("lifecycle hooks", () => {
  it("reports a completed run once, with its output and how long it took", async () => {
    const wf = defineWorkflow<null, string>("greet", async (ctx) => {
      await ctx.sleep("pause", 1_000);
      return ctx.step("say", () => "hi");
    });
    const completed: RunCompletedEvent[] = [];
    const { engine, advance } = harness([wf], { hooks: { onRunCompleted: (e) => void completed.push(e) } });
    const id = await engine.start(wf, null);

    await engine.settle(id);
    expect(completed).toEqual([]); // still sleeping — nothing has completed

    advance(1_000);
    await engine.settle(id);
    await engine.tick(id); // a terminal run is not executed, so not reported twice

    expect(completed).toEqual([
      { runId: id, workflow: "greet", workflowVersion: 1, output: "hi", durationMs: 1_000, at: T0 + 1_000 },
    ]);
  });

  it("reports a failed run with the error it failed on, and never as completed", async () => {
    const wf = defineWorkflow<null, void>("doomed", async (ctx) => {
      await ctx.step("never", () => { throw new Error("nope"); }, { retry: { maxAttempts: 1 } });
    });
    const completed: RunCompletedEvent[] = [];
    const failed: RunFailedEvent[] = [];
    const { engine } = harness([wf], {
      hooks: { onRunCompleted: (e) => void completed.push(e), onRunFailed: (e) => void failed.push(e) },
    });
    const id = await engine.start(wf, null);
    await engine.settle(id);

    expect(completed).toEqual([]);
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({ runId: id, workflow: "doomed", workflowVersion: 1, durationMs: 0 });
    expect(failed[0]?.error).toMatch(/"never" failed after 1 attempt\(s\): nope/);
  });

  it("reports every step attempt, with when the engine will try again", async () => {
    let attempts = 0;
    const wf = defineWorkflow<null, string>("flaky", async (ctx) => {
      return ctx.step("call-api", () => {
        attempts++;
        if (attempts < 3) throw new Error(`boom ${attempts}`);
        return "ok";
      });
    });
    const steps: StepFailedEvent[] = [];
    const { engine, advance } = harness([wf], { hooks: { onStepFailed: (e) => void steps.push(e) } });
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);
    advance(2_000);
    expect((await engine.settle(id)).status).toBe("completed");

    expect(steps.map((e) => [e.step, e.attempt, e.error, e.retryAt])).toEqual([
      ["call-api", 1, "boom 1", T0 + 1_000],
      ["call-api", 2, "boom 2", T0 + 3_000], // factor 2
    ]);
  });

  it("marks the attempt that exhausted the retry policy as having no retry left", async () => {
    const wf = defineWorkflow<null, void>("doomed", async (ctx) => {
      await ctx.step("never", () => { throw new Error("nope"); }, { retry: { maxAttempts: 2 } });
    });
    const steps: StepFailedEvent[] = [];
    const { engine, advance } = harness([wf], { hooks: { onStepFailed: (e) => void steps.push(e) } });
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);

    expect(steps.map((e) => e.retryAt)).toEqual([T0 + 1_000, null]);
  });

  it("does not report an attempt again when replay reads it back from history", async () => {
    // Both settled shapes a later replay passes over: an attempt the retry
    // recovered from, and one the policy gave up on that the workflow caught.
    let attempts = 0;
    const wf = defineWorkflow<null, string>("saga", async (ctx) => {
      const value = await ctx.step("flaky", () => {
        if (++attempts === 1) throw new Error("boom");
        return "ok";
      });
      try {
        await ctx.step("doomed", () => { throw new Error("nope"); }, { retry: { maxAttempts: 1 } });
      } catch (err) {
        if (!(err instanceof StepFailedError)) throw err;
      }
      await ctx.sleep("cool-off", 1_000); // the tick after this replays both of them
      return value;
    });
    const steps: StepFailedEvent[] = [];
    const { engine, advance } = harness([wf], { hooks: { onStepFailed: (e) => void steps.push(e) } });
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);
    advance(1_000);
    expect((await engine.settle(id)).status).toBe("completed");

    expect(steps.map((e) => [e.step, e.attempt])).toEqual([
      ["flaky", 1],
      ["doomed", 1],
    ]);
  });

  it("reports the version a run is pinned to, not the newest registered", async () => {
    const v1 = defineWorkflow<null, string>("pay", async (ctx) => {
      await ctx.waitFor("approved");
      return "v1";
    });
    const v2 = defineWorkflow<null, string>("pay", async () => "v2", { version: 2 });
    const completed: RunCompletedEvent[] = [];
    const { engine, deploy } = harness([v1], { hooks: { onRunCompleted: (e) => void completed.push(e) } });
    const id = await engine.start(v1, null);
    await engine.settle(id);

    await deploy([v1, v2]).signal(id, "approved", null);

    expect(completed.map((e) => [e.workflowVersion, e.output])).toEqual([[1, "v1"]]);
  });

  it("is called after the state it reports is persisted", async () => {
    const wf = defineWorkflow<null, string>("greet", async () => "hi");
    const asStored: RunRecord[] = [];
    const { engine } = harness([wf], {
      hooks: {
        onRunCompleted: async (e) => {
          const stored = await engine.get(e.runId);
          if (stored) asStored.push(stored);
        },
      },
    });
    const id = await engine.start(wf, null);
    await engine.settle(id);

    expect(asStored.map((run) => [run.status, run.output])).toEqual([["completed", "hi"]]);
  });

  it("cannot fail a run: whatever a hook throws is dropped", async () => {
    let attempts = 0;
    const wf = defineWorkflow<null, string>("flaky", async (ctx) => {
      return ctx.step("call-api", () => {
        if (++attempts === 1) throw new Error("boom");
        return "ok";
      });
    });
    const { engine, advance } = harness([wf], {
      hooks: {
        onStepFailed: () => { throw new Error("statsd is down"); },
        onRunCompleted: () => Promise.reject(new Error("pagerduty is down")),
      },
    });
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    const run = await engine.settle(id);

    expect(run.status).toBe("completed");
    expect(run.output).toBe("ok");
  });

  it("says nothing about a canceled run — the caller asked for that itself", async () => {
    const wf = defineWorkflow<null, void>("long", async (ctx) => {
      await ctx.waitFor("never");
    });
    const reported: string[] = [];
    const { engine } = harness([wf], {
      hooks: { onRunCompleted: () => void reported.push("completed"), onRunFailed: () => void reported.push("failed") },
    });
    const id = await engine.start(wf, null);
    await engine.settle(id);

    expect((await engine.cancel(id)).status).toBe("canceled");
    expect(reported).toEqual([]);
  });
});

describe("scheduled starts", () => {
  const tick = defineWorkflow<{ job: string }, string>("nightly", async (ctx, input) => {
    return ctx.step("work", () => `did ${input.job}`);
  });

  it("starts the period's run and reports that it was this call that started it", async () => {
    const { engine, store } = harness([tick]);

    const first = await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });

    expect(first).toEqual({ runId: scheduleRunId("nightly", T0), periodStart: T0, created: true });
    expect(store.all().map((r) => [r.id, r.workflow, r.input])).toEqual([
      [scheduleRunId("nightly", T0), "nightly", { job: "rollup" }],
    ]);
  });

  it("starts nothing on a second call in the same period, however far into it", async () => {
    const { engine, store, advance } = harness([tick]);

    const first = await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });
    advance(59_999);
    const again = await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });

    expect(again).toEqual({ runId: first.runId, periodStart: T0, created: false });
    expect(store.all()).toHaveLength(1);
  });

  it("does not start the period again once its run has finished", async () => {
    // The run is the record that the period fired, so a period whose work is
    // already done must not fire a second time when the loop comes back round.
    const { engine, store, advance, drain } = harness([tick]);

    await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });
    await drain();
    advance(1_000);
    const again = await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });

    expect(store.all().map((r) => [r.status, r.output])).toEqual([["completed", "did rollup"]]);
    expect(again.created).toBe(false);
  });

  it("starts a new run once the period rolls over", async () => {
    const { engine, store, advance } = harness([tick]);

    await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });
    advance(60_000);
    const next = await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });

    expect(next).toEqual({ runId: scheduleRunId("nightly", T0 + 60_000), periodStart: T0 + 60_000, created: true });
    expect(store.all()).toHaveLength(2);
  });

  it("measures periods from the epoch, not from the first call", async () => {
    // Two processes that start minutes apart have to agree on where a period
    // ends, and they never speak: the only thing both of them have is the clock.
    const { engine, advance } = harness([tick]);

    advance(500);
    const late = await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });
    advance(59_500);
    const next = await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });

    expect(late.periodStart).toBe(T0);
    expect(next.periodStart).toBe(T0 + 60_000);
    expect(schedulePeriod(T0 + 500, 60_000)).toBe(T0);
  });

  it("starts one run between two processes that drive the same schedule at once", async () => {
    // The real deployment: every worker runs the loop, so the calls overlap and
    // both see no run before either has written one. The id is the primary key
    // in every store, so the second write loses rather than forking the period.
    const { engine, store, deploy } = harness([tick]);
    const other = deploy([tick]);

    const results = await Promise.all([
      engine.schedule(tick, { job: "rollup" }, { every: 60_000 }),
      other.schedule(tick, { job: "rollup" }, { every: 60_000 }),
    ]);

    expect(results.filter((r) => r.created)).toHaveLength(1);
    expect(results.map((r) => r.runId)).toEqual([scheduleRunId("nightly", T0), scheduleRunId("nightly", T0)]);
    expect(store.all()).toHaveLength(1);
  });

  it("keeps two schedules over one workflow apart when they are named apart", async () => {
    const { engine, store } = harness([tick]);

    await engine.schedule(tick, { job: "rollup" }, { every: 60_000, name: "rollup" });
    await engine.schedule(tick, { job: "digest" }, { every: 60_000, name: "digest" });

    expect(store.all().map((r) => r.id).sort()).toEqual([scheduleRunId("digest", T0), scheduleRunId("rollup", T0)]);
  });

  it("rejects a period that is not a positive number of milliseconds", async () => {
    const { engine } = harness([tick]);

    await expect(engine.schedule(tick, { job: "rollup" }, { every: 0 })).rejects.toThrow(RangeError);
    await expect(engine.schedule(tick, { job: "rollup" }, { every: -1 })).rejects.toThrow(RangeError);
  });

  it("starts an ordinary run: a worker picks it up and it replays like any other", async () => {
    const { engine, drain } = harness([tick]);

    const { runId } = await engine.schedule(tick, { job: "rollup" }, { every: 60_000 });
    await drain();
    const view = await engine.view(runId);

    expect(view?.status).toBe("completed");
    expect(view?.output).toBe("did rollup");
    expect(view?.timeline.map((e) => e.type)).toEqual(["step.completed"]);
  });
});

describe("continuations", () => {
  /** A run that would otherwise grow forever: one step per batch, continuing after each. */
  const batches = defineWorkflow<{ left: number; done: number }, number>("batches", async (ctx, input) => {
    const done = await ctx.step("batch", () => input.done + 1);
    return input.left > 1 ? ctx.continueAsNew({ left: input.left - 1, done }) : done;
  });

  it("ends the run and carries the work on in a successor with an empty history", async () => {
    const { engine } = harness([batches]);
    await engine.start(batches, { left: 3, done: 0 }, { id: "r1" });
    const settled = await engine.settle("r1"); // on through the handovers
    expect(settled.id).toBe("r1~3");

    const first = await engine.get("r1");
    expect(first?.status).toBe("continued");
    expect(first?.continuation).toEqual({ runId: "r1~2" });
    expect(first?.output).toBeUndefined();

    const second = await engine.get("r1~2");
    expect(second?.chain).toEqual({ root: "r1", generation: 2 });
    expect(second?.input).toEqual({ left: 2, done: 1 });

    const last = await engine.get("r1~3");
    expect(last?.status).toBe("completed");
    expect(last?.output).toBe(3);

    // Each generation replays its own history and nothing else, which is the
    // whole point: three batches, one event apiece.
    expect([first, second, last].map((r) => r?.history.length)).toEqual([1, 1, 1]);
    expect((await engine.list({ status: "continued" })).runs.map((r) => r.id)).toEqual(["r1~2", "r1"]);
  });

  it("continues once when a crash lost the record of the handover", async () => {
    const { engine, store, drain } = harness([batches]);
    await engine.start(batches, { left: 2, done: 0 }, { id: "r1" });
    await engine.tick("r1"); // creates the successor, then records the handover

    // Rewind to the instant before that was persisted: the successor exists,
    // the run has no memory of handing over.
    const first = (await store.get("r1"))!;
    first.status = "running";
    delete first.continuation;
    await store.save(first, first.version);

    await drain();
    expect(store.all().map((r) => r.id).sort()).toEqual(["r1", "r1~2"]);
    expect((await engine.get("r1"))?.continuation).toEqual({ runId: "r1~2" });
    expect((await engine.get("r1~2"))?.output).toBe(2);
  });

  it("starts the successor on the newest version, so an endless run can cross a deploy", async () => {
    const v1 = defineWorkflow<number, string>("rolling", async (ctx, n) =>
      n > 0 ? ctx.continueAsNew(n - 1) : "v1",
    );
    const v2 = defineWorkflow<number, string>("rolling", async (ctx, n) => (n > 0 ? ctx.continueAsNew(n - 1) : "v2"), {
      version: 2,
    });
    const { engine, drain } = harness([v1, v2]);
    await engine.start(v1, 1, { id: "r1" }); // pinned to version 1
    await drain();

    expect((await engine.get("r1"))?.workflowVersion).toBe(1);
    expect((await engine.get("r1~2"))?.workflowVersion).toBe(2);
    expect((await engine.get("r1~2"))?.output).toBe("v2");
  });

  it("hands the parent the chain's outcome, not the generation that continued", async () => {
    const relay = defineWorkflow<number, string>("relay", async (ctx, n) => (n > 0 ? ctx.continueAsNew(n - 1) : "relayed"));
    const supervisor = defineWorkflow<null, string>("relay-parent", async (ctx) => {
      const handle = await ctx.startChild(relay, 2);
      return ctx.waitForChild(handle);
    });
    const { engine, drain } = harness([supervisor, relay]);
    await engine.start(supervisor, null, { id: "p1" });
    await drain();

    expect((await engine.get("p1"))?.output).toBe("relayed");
    // The link moves with the work, so the generation that finished reports on
    // the signal the parent has been waiting on since the child started.
    expect((await engine.get("p1#0"))?.status).toBe("continued");
    expect((await engine.get("p1#0~3"))?.parent).toEqual({ runId: "p1", signal: "child:p1#0" });
  });

  it("forwards a signal sent to a run that has since continued", async () => {
    const wf = defineWorkflow<number, string>("shift", async (ctx, n) =>
      n > 0 ? ctx.continueAsNew(n - 1) : ctx.waitFor<string>("handover"),
    );
    const { engine, drain } = harness([wf]);
    await engine.start(wf, 1, { id: "r1" });
    await drain();
    expect((await engine.get("r1~2"))?.status).toBe("waiting");

    await engine.signal("r1", "handover", "baton"); // the id the caller still holds
    expect((await engine.get("r1~2"))?.output).toBe("baton");
  });

  it("carries a signal that arrived before the handover over to the successor", async () => {
    const wf = defineWorkflow<number, string>("mailbox", async (ctx, n) => {
      if (n === 0) return ctx.waitFor<string>("mail");
      await ctx.sleep("gather", 1_000);
      return ctx.continueAsNew(n - 1);
    });
    const { engine, drain, advance } = harness([wf]);
    await engine.start(wf, 1, { id: "r1" });
    await drain(); // generation 1 is asleep
    await engine.signal("r1", "mail", "letter"); // buffered: nothing is waiting for it yet
    expect((await engine.get("r1"))?.pendingSignals).toEqual({ mail: ["letter"] });

    advance(1_000);
    await drain();
    expect((await engine.get("r1"))?.pendingSignals).toEqual({});
    expect((await engine.get("r1~2"))?.output).toBe("letter");
  });

  it("cancels the generation that is running, whichever id the caller has", async () => {
    const wf = defineWorkflow<number, void>("patient", async (ctx, n) => {
      if (n > 0) return ctx.continueAsNew(n - 1);
      await ctx.waitFor("never");
    });
    const { engine, drain } = harness([wf]);
    await engine.start(wf, 1, { id: "r1" });
    await drain();

    const canceled = await engine.cancel("r1");
    expect(canceled.id).toBe("r1~2");
    expect(canceled.status).toBe("canceled");
    expect((await engine.get("r1"))?.status).toBe("continued");
  });

  it("reports one completion for the chain, not one per generation", async () => {
    const completed: RunCompletedEvent[] = [];
    const wf = defineWorkflow<number, string>("counted-chain", async (ctx, n) =>
      n > 0 ? ctx.continueAsNew(n - 1) : "done",
    );
    const { engine, drain } = harness([wf], { hooks: { onRunCompleted: (e) => void completed.push(e) } });
    await engine.start(wf, 2, { id: "r1" });
    await drain();

    expect(completed.map((e) => e.runId)).toEqual(["r1~3"]);
  });

  describe("the chain as one view", () => {
    /** Three generations with a step apiece, a wait at the end, and a tag on the chain. */
    const relay = defineWorkflow<{ left: number }, string>("relayed", async (ctx, input) => {
      await ctx.step(`leg-${input.left}`, () => input.left);
      if (input.left > 1) {
        await ctx.sleep("breathe", 1_000);
        return ctx.continueAsNew({ left: input.left - 1 });
      }
      return ctx.waitFor<string>("baton");
    });

    async function chain() {
      const h = harness([relay]);
      await h.engine.start(relay, { left: 3 }, { id: "r1", tags: ["order:ord_7"] });
      for (let i = 0; i < 2; i++) {
        await h.drain();
        h.advance(1_000);
      }
      await h.drain();
      return h;
    }

    it("stitches every generation's events onto one axis of offsets from the root", async () => {
      const { engine } = await chain();

      const view = await engine.viewChain("r1");
      expect(view?.generations).toBe(3);
      expect(view?.runs.map((r) => r.id)).toEqual(["r1", "r1~2", "r1~3"]);
      expect(view?.timeline.map((e) => [e.generation, e.runId, e.elapsedMs, e.summary])).toEqual([
        [1, "r1", 0, 'step "leg-3" completed'],
        [1, "r1", 1_000, 'timer "breathe" fired'],
        [2, "r1~2", 1_000, 'step "leg-2" completed'],
        [2, "r1~2", 2_000, 'timer "breathe" fired'],
        [3, "r1~3", 2_000, 'step "leg-1" completed'],
      ]);
      // Each generation's own view still reads from its own start, which is
      // what the per-run timeline is for.
      expect(view?.runs.map((r) => r.timeline.map((e) => e.elapsedMs))).toEqual([[0, 1_000], [0, 1_000], [0]]);
      expect(JSON.parse(JSON.stringify(view))).toEqual(view); // an admin endpoint can send it as-is
    });

    it("answers about the work: the chain's input, the live generation and its wait", async () => {
      const { engine } = await chain();

      const view = await engine.viewChain("r1");
      expect(view?.root).toBe("r1");
      expect(view?.live).toBe("r1~3");
      expect(view?.status).toBe("waiting");
      expect(view?.input).toEqual({ left: 3 }); // what the chain was asked to do
      expect(view?.output).toBeUndefined();
      expect(view?.blockedOn).toEqual({ kind: "signal", name: "baton", until: null });
      expect(view?.tags).toEqual(["order:ord_7"]);
      expect(view?.createdAt).toBe(T0);
      expect(view?.durationMs).toBe(2_000); // the root's start to the live generation's last write
    });

    it("answers the same from any id in the chain, including one six generations old", async () => {
      const { engine } = await chain();

      const [fromRoot, fromMiddle, fromLive] = await Promise.all([
        engine.viewChain("r1"),
        engine.viewChain("r1~2"),
        engine.viewChain("r1~3"),
      ]);
      expect(fromMiddle).toEqual(fromRoot);
      expect(fromLive).toEqual(fromRoot);
    });

    it("reports the outcome the last generation produced, once the chain ends", async () => {
      const { engine, advance } = await chain();
      advance(500);
      await engine.signal("r1", "baton", "home"); // the id the caller has held all along

      const view = await engine.viewChain("r1");
      expect(view?.status).toBe("completed");
      expect(view?.output).toBe("home");
      expect(view?.blockedOn).toBeNull();
      expect(view?.durationMs).toBe(2_500);
      expect(view?.timeline.at(-1)).toMatchObject({ generation: 3, summary: 'signal "baton" received' });
    });

    it("renders a run that never continued as a chain of one", async () => {
      const wf = defineWorkflow<null, string>("single", async (ctx) => ctx.step("once", () => "done"));
      const { engine, drain } = harness([wf]);
      await engine.start(wf, null, { id: "r1" });
      await drain();

      const view = await engine.viewChain("r1");
      expect(view).toMatchObject({ root: "r1", live: "r1", generations: 1, status: "completed", output: "done" });
      expect(view?.timeline.map((e) => [e.generation, e.runId])).toEqual([[1, "r1"]]);
      expect(await engine.viewChain("no-such-run")).toBeNull();
    });
  });

  it("shows an operator which generation a run is and where the work went", async () => {
    const { engine, drain } = harness([batches]);
    await engine.start(batches, { left: 2, done: 0 }, { id: "r1" });
    await drain();

    const first = await engine.view("r1");
    expect(first?.chain).toEqual({ root: "r1", generation: 1 });
    expect(first?.continuation).toEqual({ runId: "r1~2" });
    expect(first?.blockedOn).toBeNull();

    const second = await engine.view("r1~2");
    expect(second?.chain).toEqual({ root: "r1", generation: 2 });
    expect(second?.continuation).toBeNull();
  });
});

describe("saga compensation", () => {
  /** The shape the feature exists for: each step registers its undo right after it succeeds. */
  function order(options: {
    release?: () => void;
    refund?: () => void;
    refundRetry?: number;
    ship?: () => string;
  }) {
    return defineWorkflow<null, string>("order", async (ctx) => {
      await ctx.step("reserve", () => "res_1");
      ctx.compensate("release", () => options.release?.());
      await ctx.step("charge", () => "ch_1");
      ctx.compensate("refund", () => options.refund?.(), { retry: { maxAttempts: options.refundRetry ?? 3 } });
      return ctx.step("ship", () => options.ship?.() ?? "shipped", { retry: { maxAttempts: 1 } });
    });
  }

  it("runs the registered undos newest first when a step fails for good", async () => {
    const undone: string[] = [];
    const wf = order({
      release: () => void undone.push("release"),
      refund: () => void undone.push("refund"),
      ship: () => {
        throw new Error("no courier");
      },
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(run.status).toBe("failed");
    expect(undone).toEqual(["refund", "release"]);
    expect(run.error).toMatch(/"ship" failed after 1 attempt\(s\): no courier/);
    // The undos sit above the call the run failed at, in the order they ran.
    expect(historyEvents(run).map((e) => [e.call, e.type, e.name])).toEqual([
      [0, "step.completed", "reserve"],
      [1, "step.completed", "charge"],
      [2, "step.failed", "ship"],
      [3, "compensation.completed", "refund"],
      [4, "compensation.completed", "release"],
    ]);
  });

  it("leaves the undos alone when the workflow handles the failure itself", async () => {
    const undone: string[] = [];
    const wf = defineWorkflow<null, string>("handled", async (ctx) => {
      await ctx.step("charge", () => "ch_1");
      ctx.compensate("refund", () => void undone.push("refund"));
      try {
        return await ctx.step(
          "ship",
          () => {
            throw new Error("no courier");
          },
          { retry: { maxAttempts: 1 } },
        );
      } catch (err) {
        if (!(err instanceof StepFailedError)) throw err;
        return "apologised";
      }
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(run.status).toBe("completed");
    expect(run.output).toBe("apologised");
    expect(undone).toEqual([]);
  });

  it("leaves the undos alone when the run completes", async () => {
    const undone: string[] = [];
    const wf = order({ release: () => void undone.push("release"), refund: () => void undone.push("refund") });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(run.status).toBe("completed");
    expect(run.output).toBe("shipped");
    expect(undone).toEqual([]);
  });

  it("runs each undo at most once, however many passes the failure replays through", async () => {
    const undone: string[] = [];
    let releaseAttempts = 0;
    const wf = order({
      release: () => {
        if (++releaseAttempts === 1) throw new Error("inventory down");
        undone.push("release");
      },
      refund: () => void undone.push("refund"),
      ship: () => {
        throw new Error("no courier");
      },
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    let run = await engine.settle(id);
    expect(run.status).toBe("sleeping"); // "release" is serving out its backoff
    expect(undone).toEqual(["refund"]);

    advance(1_000);
    run = await engine.settle(id);

    expect(run.status).toBe("failed");
    // "refund" is memoised by the replay that carried the phase on.
    expect(undone).toEqual(["refund", "release"]);
    expect(releaseAttempts).toBe(2);
  });

  it("retries an undo on a persisted wake time, under its own policy", async () => {
    let attempts = 0;
    const wf = order({
      refundRetry: 4,
      refund: () => {
        if (++attempts < 3) throw new Error(`gateway ${attempts}`);
      },
      ship: () => {
        throw new Error("no courier");
      },
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    let run = await engine.settle(id);
    expect(run.status).toBe("sleeping");
    expect(run.wakeAt).toBe(T0 + 1_000);
    expect(attempts).toBe(1);

    run = await engine.tick(id); // too early — the backoff is a wake time, not a timer
    expect(attempts).toBe(1);

    advance(1_000);
    run = await engine.settle(id);
    expect(run.wakeAt).toBe(T0 + 1_000 + 2_000); // factor 2, same as a step
    expect(attempts).toBe(2);

    advance(2_000);
    run = await engine.settle(id);

    expect(run.status).toBe("failed");
    expect(attempts).toBe(3);
    expect(historyEvents(run).filter((e) => e.type === "compensation.failed")).toHaveLength(2);
  });

  it("carries on below an undo that exhausted its retries, and names it on the run", async () => {
    const undone: string[] = [];
    const wf = order({
      release: () => void undone.push("release"),
      refundRetry: 1,
      refund: () => {
        throw new Error("gateway down");
      },
      ship: () => {
        throw new Error("no courier");
      },
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(run.status).toBe("failed");
    // Abandoning "release" would leave more of the saga applied, not less.
    expect(undone).toEqual(["release"]);
    expect(run.error).toMatch(/"ship" failed after 1 attempt\(s\): no courier/);
    expect(run.error).toMatch(/compensation did not complete: "refund" \(gateway down\)/);
  });

  it("undoes nothing when replay no longer matches the code", async () => {
    const undone: string[] = [];
    const body = (timer: string) =>
      defineWorkflow<null, string>("drifting", async (ctx) => {
        await ctx.step("charge", () => "ch_1");
        ctx.compensate("refund", () => void undone.push("refund"));
        await ctx.sleep(timer, 1_000);
        return ctx.step("ship", () => "shipped");
      });
    const { engine, advance, deploy } = harness([body("hold")]);
    const id = await engine.start(body("hold"), null);
    await engine.settle(id);

    advance(1_000);
    const run = await deploy([body("wait")]).settle(id);

    expect(run.status).toBe("failed");
    expect(run.error).toMatch(/"hold" in history but "wait" now/);
    // The history and the code disagree, so which undos the run owes is guesswork.
    expect(undone).toEqual([]);
  });

  it("shows an undo on the timeline and reports one serving out its backoff", async () => {
    let attempts = 0;
    const wf = order({
      release: () => {},
      refund: () => {
        if (++attempts === 1) throw new Error("gateway down");
      },
      ship: () => {
        throw new Error("no courier");
      },
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);
    await engine.settle(id);

    const sleeping = await engine.view(id);
    expect(sleeping?.blockedOn).toEqual({ kind: "retry", name: "refund", until: T0 + 1_000 });

    advance(1_000);
    await engine.settle(id);
    const failed = await engine.view(id);

    expect(failed?.blockedOn).toBeNull();
    expect(failed?.timeline.map((e) => e.summary).slice(2)).toEqual([
      'step "ship" failed on attempt 1, no attempts left: no courier',
      'compensation "refund" failed on attempt 1, retrying: gateway down',
      'compensation "refund" completed',
      'compensation "release" completed',
    ]);
  });

  it("reports a failing undo as a compensation rather than as a step", async () => {
    const failures: StepFailedEvent[] = [];
    const wf = order({
      refundRetry: 1,
      refund: () => {
        throw new Error("gateway down");
      },
      ship: () => {
        throw new Error("no courier");
      },
    });
    const { engine } = harness([wf], { hooks: { onStepFailed: (e) => void failures.push(e) } });
    const id = await engine.start(wf, null);
    await engine.settle(id);

    expect(failures.map((e) => [e.kind, e.step, e.attempt, e.retryAt])).toEqual([
      ["step", "ship", 1, null],
      ["compensation", "refund", 1, null],
    ]);
  });

  it("keeps folding history past a registration, because registering records nothing", async () => {
    const wf = defineWorkflow<{ steps: number }, number>("undoable", async (ctx, input) => {
      let total = 0;
      for (let i = 0; i < input.steps; i++) {
        total += await ctx.step(`work-${i}`, () => 1);
        ctx.compensate(`undo-${i}`, () => {});
        await ctx.sleep(`pause-${i}`, 1_000);
      }
      return total;
    });
    const { engine, advance } = harness([wf], { compactAfter: 4 });
    const id = await engine.start(wf, { steps: 10 });

    let run = await engine.settle(id);
    for (let i = 0; i < 12 && run.status !== "completed"; i++) {
      advance(1_000);
      run = await engine.settle(id);
    }

    expect(run.status).toBe("completed");
    expect(run.output).toBe(10);
    // 20 calls, all but the last tick's two folded. A registration that took a
    // call position of its own would leave a hole in the prefix, and the
    // snapshot would have stopped at the first one.
    expect(run.snapshot?.calls).toBe(18);
  });
});

describe("typed signals", () => {
  /**
   * A schema is anything with zod's `parse`, so the suite hand-rolls one rather
   * than take the dependency. The default on `note` is the schema's, which is
   * how a test can tell the parsed value apart from the payload as it arrived.
   */
  const approval = defineSignal("approve", {
    parse(payload: unknown): { ok: boolean; note: string } {
      if (typeof payload !== "object" || payload === null) throw new Error("expected an object");
      const { ok, note } = payload as { ok?: unknown; note?: unknown };
      if (typeof ok !== "boolean") throw new Error("expected `ok` to be a boolean");
      if (note !== undefined && typeof note !== "string") throw new Error("expected `note` to be a string");
      return { ok, note: typeof note === "string" ? note : "" };
    },
  });

  const review = defineWorkflow<null, string>("review", async (ctx) => {
    const { ok, note } = await ctx.waitFor(approval, { timeoutMs: 60_000 });
    return `${ok ? "approved" : "declined"}:${note}`;
  });

  it("hands the workflow what the schema returned, not the payload as it arrived", async () => {
    const { engine } = harness([review]);
    const id = await engine.start(review, null);
    await engine.settle(id);

    const run = await engine.signal(id, approval, { ok: true });

    expect(run.status).toBe("completed");
    // The payload carried no note; the schema's default is what the run saw.
    expect(run.output).toBe("approved:");
  });

  it("refuses a payload the schema rejects before it reaches history", async () => {
    const { engine } = harness([review]);
    const id = await engine.start(review, null);
    await engine.settle(id);

    await expect(engine.signal(id, approval, { ok: "yes" })).rejects.toThrow(SignalRejectedError);

    const run = await engine.get(id);
    expect(run?.status).toBe("waiting");
    expect(run?.waitingFor).toEqual({ name: "approve", call: 0 });
    expect(historyEvents(run as RunRecord)).toEqual([]);
    expect(run?.pendingSignals).toEqual({});
  });

  it("refuses a payload that would otherwise have been buffered ahead of its waitFor", async () => {
    const wf = defineWorkflow<null, string>("late-review", async (ctx) => {
      await ctx.sleep("prep", 5_000);
      const { note } = await ctx.waitFor(approval);
      return note;
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);
    await engine.settle(id); // sleeping, nowhere near the waitFor

    await expect(engine.signal(id, approval, "nope")).rejects.toThrow(/expected an object/);
    expect((await engine.get(id))?.pendingSignals).toEqual({});

    await engine.signal(id, approval, { ok: true, note: "fine" });
    advance(5_000);
    expect((await engine.settle(id)).output).toBe("fine");
  });

  it("records the refusal on the run, where an operator looking at it can see why", async () => {
    const { engine, advance } = harness([review]);
    const id = await engine.start(review, null);
    await engine.settle(id);
    advance(1_000);

    await expect(engine.signal(id, approval, {})).rejects.toThrow(SignalRejectedError);

    const view = await engine.view(id);
    expect(view?.rejectedSignals).toEqual([
      { name: "approve", error: "expected `ok` to be a boolean", at: T0 + 1_000 },
    ]);
    // Nothing was delivered, so the timeline has nothing to say about it.
    expect(view?.timeline).toEqual([]);
    expect(view?.blockedOn).toEqual({ kind: "signal", name: "approve", until: T0 + 60_000 });
  });

  it("keeps the most recent refusals and no more", async () => {
    const { engine } = harness([review]);
    const id = await engine.start(review, null);
    await engine.settle(id);

    for (let i = 0; i < MAX_REJECTED_SIGNALS + 3; i++) {
      await expect(engine.signal(id, approval, { ok: true, note: i })).rejects.toThrow(SignalRejectedError);
    }

    // A caller looping on a bad payload must not grow the record without bound.
    const { rejectedSignals } = (await engine.view(id)) ?? {};
    expect(rejectedSignals).toHaveLength(MAX_REJECTED_SIGNALS);
    expect(rejectedSignals?.[0]?.error).toBe("expected `note` to be a string");
  });

  it("checks a signal named by string once its schema is registered with the engine", async () => {
    const { engine } = harness([review], { signals: [approval] });
    const id = await engine.start(review, null);
    await engine.settle(id);

    // An admin endpoint has a name off the wire, not the definition.
    await expect(engine.signal(id, "approve", { ok: 1 })).rejects.toThrow(SignalRejectedError);
    expect((await engine.signal(id, "approve", { ok: false, note: "thin" })).output).toBe("declined:thin");
  });

  it("checks a definition it was handed even when the engine was not given it", async () => {
    const { engine } = harness([review]);
    const id = await engine.start(review, null);
    await engine.settle(id);

    await expect(engine.signal(id, approval, null)).rejects.toThrow(SignalRejectedError);
  });

  it("leaves a signal with no schema alone", async () => {
    const wf = defineWorkflow<null, unknown>("untyped", async (ctx) => ctx.waitFor("whatever"));
    const { engine } = harness([wf], { signals: [approval] });
    const id = await engine.start(wf, null);
    await engine.settle(id);

    expect((await engine.signal(id, "whatever", 17)).output).toBe(17);
  });

  it("does not refuse the outcome a child reports to its parent", async () => {
    const child = defineWorkflow<{ n: number }, number>("halve", async (ctx, input) =>
      ctx.step("divide", () => input.n / 2),
    );
    const parent = defineWorkflow<{ n: number }, number>("halver", async (ctx, input) =>
      ctx.waitForChild(await ctx.startChild(child, { n: input.n })),
    );
    // The signal a child reports on is derived, so it has no schema to register
    // — and must not be held to anyone else's.
    const { engine, drain } = harness([parent, child], { signals: [approval] });
    const id = await engine.start(parent, { n: 84 });
    await drain();

    expect((await engine.get(id))?.output).toBe(42);
  });

  it("refuses two schemas for one signal name", async () => {
    const other = defineSignal("approve", { parse: (payload: unknown) => payload });
    expect(() => harness([review], { signals: [approval, other] })).toThrow(/registered twice/);
  });
});

describe("step timeouts", () => {
  /**
   * A hang, as a workflow would meet one: a call that never answers. It holds
   * no timer and no socket, so a test can abandon an attempt and still exit.
   */
  const hang = () => new Promise<never>(() => {});

  /**
   * A hang that answers an abort, as a client handed an `AbortSignal` does: it
   * rejects with the reason the attempt was aborted with and lets go.
   */
  const hangUntilAborted = (signal: AbortSignal) =>
    new Promise<never>((_, reject) => {
      signal.addEventListener("abort", () => reject(signal.reason as Error));
    });

  it("turns a hung attempt into a retryable failure with an event of its own", async () => {
    let attempts = 0;
    const wf = defineWorkflow<null, string>("fetch", async (ctx) =>
      ctx.step("call-api", () => (++attempts === 1 ? hang() : "ok"), { timeoutMs: 20 }),
    );
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    // The harness clock never moves during the step, so the attempt was bounded
    // in wall-clock time rather than by anything the engine reads off its clock.
    let run = await engine.settle(id);
    expect(attempts).toBe(1);
    expect(run.status).toBe("sleeping");
    expect(run.wakeAt).toBe(T0 + 1_000); // the step's own backoff, not a lease expiry
    expect(run.history).toMatchObject([
      { type: "step.failed", name: "call-api", attempt: 1, error: "timed out after 20ms", retryAt: T0 + 1_000 },
    ]);

    advance(1_000);
    run = await engine.settle(id);
    expect(run.status).toBe("completed");
    expect(run.output).toBe("ok");
    expect(attempts).toBe(2);
  });

  it("fails the run once the attempts are spent, with both of them on the timeline", async () => {
    const wf = defineWorkflow<null, void>("doomed", async (ctx) => {
      await ctx.step("call-api", hang, { timeoutMs: 20, retry: { maxAttempts: 2 } });
    });
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    const run = await engine.settle(id);

    expect(run.status).toBe("failed");
    expect(run.error).toBe('step "call-api" failed after 2 attempt(s): timed out after 20ms');
    const view = await engine.view(id);
    expect(view?.timeline.map((e) => e.summary)).toEqual([
      'step "call-api" failed on attempt 1, retrying: timed out after 20ms',
      'step "call-api" failed on attempt 2, no attempts left: timed out after 20ms',
    ]);
  });

  it("hands the workflow the timeout as the cause, so a hang is not a refusal", async () => {
    const wf = defineWorkflow<null, string>("supervised", async (ctx) => {
      try {
        return await ctx.step("call-api", hang, { timeoutMs: 20, retry: { maxAttempts: 1 } });
      } catch (err) {
        if (!(err instanceof StepFailedError)) throw err;
        if (!(err.cause instanceof StepTimeoutError)) return "failed";
        return `hung for ${err.cause.timeoutMs}ms`;
      }
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(run.status).toBe("completed");
    expect(run.output).toBe("hung for 20ms");
  });

  it("leaves an attempt that answers inside its timeout alone", async () => {
    const wf = defineWorkflow<null, string>("quick", async (ctx) =>
      ctx.step("call-api", async () => "ok", { timeoutMs: 60_000 }),
    );
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(run.status).toBe("completed");
    expect(run.output).toBe("ok");
    expect(run.history.map((e) => e.type)).toEqual(["step.completed"]);
  });

  it("bounds an undo the same way, so a hung compensation does not strand the phase", async () => {
    const wf = defineWorkflow<null, void>("checkout", async (ctx) => {
      await ctx.step("charge", () => "ok");
      ctx.compensate("refund", hang, { timeoutMs: 20, retry: { maxAttempts: 1 } });
      await ctx.step("ship", () => {
        throw new Error("no courier");
      }, { retry: { maxAttempts: 1 } });
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(run.status).toBe("failed");
    expect(run.error).toBe(
      'step "ship" failed after 1 attempt(s): no courier; compensation did not complete: "refund" (timed out after 20ms)',
    );
    expect(run.history.map((e) => e.type)).toEqual(["step.completed", "step.failed", "compensation.failed"]);
  });

  it("aborts the attempt's signal when the bound elapses, with the timeout as the reason", async () => {
    let reason: unknown;
    const wf = defineWorkflow<null, void>("cancelling", async (ctx) => {
      await ctx.step(
        "call-api",
        (signal) =>
          new Promise<never>((_, reject) => {
            signal.addEventListener("abort", () => {
              reason = signal.reason;
              reject(signal.reason as Error);
            });
          }),
        { timeoutMs: 20, retry: { maxAttempts: 1 } },
      );
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(reason).toBeInstanceOf(StepTimeoutError);
    expect((reason as StepTimeoutError).timeoutMs).toBe(20);
    // The abort is what the step sees; what the run records is still the bound
    // it ran past, not whatever the cancelled client rejected with.
    expect(run.error).toBe('step "call-api" failed after 1 attempt(s): timed out after 20ms');
  });

  it("gives each attempt a signal of its own, so a retry is not born aborted", async () => {
    const seen: boolean[] = [];
    let attempts = 0;
    const wf = defineWorkflow<null, string>("fetch", async (ctx) =>
      ctx.step(
        "call-api",
        (signal) => {
          seen.push(signal.aborted);
          return ++attempts === 1 ? hangUntilAborted(signal) : "ok";
        },
        { timeoutMs: 20 },
      ),
    );
    const { engine, advance } = harness([wf]);
    const id = await engine.start(wf, null);

    await engine.settle(id);
    advance(1_000);
    const run = await engine.settle(id);

    expect(seen).toEqual([false, false]);
    expect(run.status).toBe("completed");
    expect(run.output).toBe("ok");
  });

  it("leaves the signal alone when the attempt answers in time", async () => {
    let aborted: boolean | undefined;
    const wf = defineWorkflow<null, string>("quick", async (ctx) => {
      const signal = await ctx.step("call-api", (s) => Promise.resolve(s), { timeoutMs: 60_000 });
      aborted = signal.aborted;
      return "ok";
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(run.status).toBe("completed");
    expect(aborted).toBe(false);
  });

  it("hands a step with no bound a signal that nothing ever aborts", async () => {
    let signal: AbortSignal | undefined;
    const wf = defineWorkflow<null, void>("unbounded", async (ctx) => {
      await ctx.step("call-api", (s) => {
        signal = s;
      });
      await ctx.step("wait", () => "done");
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    await engine.settle(id);

    expect(signal).toBeInstanceOf(AbortSignal);
    expect(signal?.aborted).toBe(false);
  });

  it("aborts a hung undo too, so the phase does not leave it running", async () => {
    let reason: unknown;
    const wf = defineWorkflow<null, void>("checkout", async (ctx) => {
      await ctx.step("charge", () => "ok");
      ctx.compensate(
        "refund",
        (signal) => {
          signal.addEventListener("abort", () => {
            reason = signal.reason;
          });
          return hangUntilAborted(signal);
        },
        { timeoutMs: 20, retry: { maxAttempts: 1 } },
      );
      await ctx.step(
        "ship",
        () => {
          throw new Error("no courier");
        },
        { retry: { maxAttempts: 1 } },
      );
    });
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null);
    const run = await engine.settle(id);

    expect(reason).toBeInstanceOf(StepTimeoutError);
    expect(run.error).toBe(
      'step "ship" failed after 1 attempt(s): no courier; compensation did not complete: "refund" (timed out after 20ms)',
    );
  });
});
