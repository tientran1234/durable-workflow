import { Engine, MemoryStore, type WorkflowDefinition } from "../src/index.js";

export const T0 = 1_800_000_000_000; // fixed epoch ms

/** An engine with a clock the test moves by hand. */
export function harness(workflows: WorkflowDefinition<any, any>[], opts: { leaseMs?: number } = {}) {
  let now = T0;
  const store = new MemoryStore();
  const engine = new Engine({
    store,
    workflows,
    now: () => now,
    leaseMs: opts.leaseMs ?? 30_000,
    defaultRetry: { initialDelayMs: 1_000, factor: 2, maxDelayMs: 60_000, maxAttempts: 3 },
  });
  return {
    engine,
    store,
    now: () => now,
    advance: (ms: number) => (now += ms),
    /** Execute everything due, including runs that other runs start. */
    drain: async (passes = 20) => {
      for (let i = 0; i < passes && (await engine.processDue(10)) > 0; i++);
    },
  };
}
