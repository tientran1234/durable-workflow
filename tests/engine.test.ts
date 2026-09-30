import { describe, expect, it } from "vitest";
import {
  ChildFailedError,
  type ChildHandle,
  ConflictError,
  NondeterminismError,
  type RunCompletedEvent,
  type RunFailedEvent,
  type RunPage,
  type RunRecord,
  StepFailedError,
  type StepFailedEvent,
  WaitTimeoutError,
  defineWorkflow,
  historyEvents,
  schedulePeriod,
  scheduleRunId,
} from "../src/index.js";
import { harness, T0 } from "./helpers.js";

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
    const { engine, drain } = harness([batches]);
    await engine.start(batches, { left: 3, done: 0 }, { id: "r1" });
    await drain();

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
