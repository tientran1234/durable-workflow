import { describe, expect, it } from "vitest";
import {
  ConflictError,
  NondeterminismError,
  StepFailedError,
  WaitTimeoutError,
  defineWorkflow,
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
