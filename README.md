# durable-workflow

Write a multi-step process as one async function. Run it across crashes,
restarts and days of waiting, with every side effect happening exactly once.

```ts
import { defineWorkflow, Engine, MemoryStore } from "durable-workflow";

const fulfilment = defineWorkflow<{ orderId: string }, string>("fulfilment", async (ctx, { orderId }) => {
  const payment = await ctx.step("charge", () => stripe.charge(orderId), { retry: { maxAttempts: 5 } });

  const review = await ctx.waitFor<{ approved: boolean }>("manual-review", { timeoutMs: 24 * 3600_000 });
  if (!review.approved) {
    await ctx.step("refund", () => stripe.refund(payment.id));
    return "refunded";
  }

  await ctx.sleep("cool-off", 3600_000);
  await ctx.step("ship", () => courier.book(orderId));
  return "shipped";
});

const engine = new Engine({ store: new MemoryStore(), workflows: [fulfilment] });
const runId = await engine.start(fulfilment, { orderId: "ord_42" });
engine.worker();                                        // in as many processes as you like

await engine.signal(runId, "manual-review", { approved: true });   // from an admin UI, hours later
```

The process can die between any two lines of that function. When a worker picks
the run up again, `charge` is not re-run, the review it was waiting for is still
waiting, and the cool-off timer still fires on time.

## How it works

The engine never keeps a workflow "in flight". It keeps a **history** — an
append-only list of what each `ctx.*` call decided — and re-executes the
function from the top whenever the run is due.

Every `ctx.step`, `ctx.waitFor` and `ctx.sleep` counts its position. On replay,
the call at position *n* looks for its own event in history:

- found → return the recorded outcome instantly, no side effect;
- not found → this is the frontier: do the work, append the event, persist.

A step therefore runs its function at most once per run. A workflow that has
completed nine steps and crashes during the tenth replays nine memoised results
in microseconds and resumes exactly where it was.

`waitFor` and `sleep` do not block a worker. They record what the run is
waiting for, persist, and unwind the function by throwing a control-flow
sentinel. The run costs nothing until a signal arrives or the timer is due.

## Design decisions

**Retries are persisted wake times, not `setTimeout`.** A failed step records
`retryAt` in history and puts the run to sleep. The backoff survives a restart;
a process dying mid-wait loses nothing. Attempt counts are derived from history,
so they cannot drift.

**Signals that arrive early are not lost.** `engine.signal()` on a run that has
not reached the matching `waitFor` yet buffers the payload; the `waitFor`
consumes it when it gets there, in order.

**Concurrency is optimistic and enforced by the store.** Every persisted change
bumps `version`; `save()` is `UPDATE … WHERE version = expected`. Two workers
that both try to advance the same run produce one winner and one
`ConflictError`. The Postgres store leases due runs with `FOR UPDATE SKIP
LOCKED`, so *n* workers claim *n* disjoint batches in one statement each,
without blocking one another.

**Nondeterminism is an error, not a silent corruption.** If a deploy renames or
reorders a step while runs are mid-flight, replay finds a history event whose
name does not match the code and fails the run with `NondeterminismError`
naming both. Workflow code must be deterministic: time, randomness and I/O go
inside `ctx.step`.

**Swallowing a suspension is caught.** `try { await ctx.waitFor(…) } catch {}`
would let the function run past a point it never reached. The engine notices
the function returned after a suspension and fails the run with an explanation,
rather than recording a completion that never happened.

**Step failures are ordinary exceptions inside the workflow.** After the last
attempt, `ctx.step` throws `StepFailedError` *into the workflow function*, so
compensation is plain code: catch it, run a `refund` step, return. Sagas need no
extra API.

## Stores

| Store | For | Concurrency |
|---|---|---|
| `MemoryStore` | tests, scripts, single process | version check |
| `PostgresStore` (`durable-workflow/postgres`, needs `pg`) | production | version check + `FOR UPDATE SKIP LOCKED` |

Any `RunStore` implementation with `create / get / save(version) / claimDue`
works. `save` must be conditional on `version` and `claimDue` must lease
atomically — that is the whole contract.

## Layout

```
src/
  types.ts        RunRecord, HistoryEvent, WorkflowContext, RunStore — the model
  context.ts      replay: each ctx call finds its own event or becomes the frontier
  engine.ts       start / tick / signal / cancel / worker; lease + execute
  retry.ts        backoff policy
  due.ts          what "due" means, shared by engine and stores
  stores/memory.ts
  stores/postgres.ts   JSONB record + mirrored query columns + SKIP LOCKED claim
tests/
  engine.test.ts        17 tests with a hand-driven clock: memoisation, durable
                        backoff, early signals, timeouts, nondeterminism, leases
  postgres.integration.test.ts   disjoint claims across concurrent workers, stale writes
```

## Run

```bash
pnpm install
pnpm test                       # unit tests need nothing
pnpm db:up                      # Postgres on :5434
DATABASE_URL=postgresql://postgres:postgres@localhost:5434/workflow pnpm test
```

CI runs the full suite, Postgres included, on every push.

## What is deliberately not here

- **Child workflows and fan-out.** Start them from a step and `waitFor` a
  completion signal; a first-class API would add surface without adding a
  guarantee.
- **Versioned migration of in-flight runs.** Nondeterminism is detected, not
  healed. Drain old runs on the old code, or branch on a version field in input.
- **Very large histories.** A run with tens of thousands of steps replays them
  all on every tick. Snapshotting is the fix and is out of scope here.
- **Scheduling / cron.** Call `engine.start` from whatever already runs on a
  schedule.
