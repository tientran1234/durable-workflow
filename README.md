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

## Child workflows

A child workflow is an ordinary run that reports back to the run that started
it. Written by hand, the pattern is a step that starts the child and a
`waitFor` that the child signals when it is done:

```ts
const parent = defineWorkflow<{ orderId: string }, string>("order", async (ctx, input) => {
  const childId = await ctx.step("start-shipment", () => engine.start(shipment, input));
  return ctx.waitFor<string>(`shipment-done:${childId}`);   // the child signals this on its last line
});
```

`ctx.startChild` and `ctx.waitForChild` are that pattern, with the parts it is
easy to get wrong: the child's id is derived rather than generated, and the
engine sends the signal instead of the child's own code.

```ts
const fulfilment = defineWorkflow<{ orderId: string }, string>("fulfilment", async (ctx, { orderId }) => {
  const shipment = await ctx.startChild(shipWorkflow, { orderId });
  const invoice = await ctx.startChild(invoiceWorkflow, { orderId });   // both are running now

  try {
    const tracking = await ctx.waitForChild(shipment, { timeoutMs: 24 * 3600_000 });
    await ctx.waitForChild(invoice);
    return tracking;
  } catch (err) {
    if (!(err instanceof ChildFailedError)) throw err;   // never swallow: suspension is control flow
    await ctx.step("apologise", () => mail.send(orderId));
    return "apologised";
  }
});
```

A child is a run like any other: a worker claims it, its steps retry on their
own, and it shows up in `engine.list` and `engine.view` with its own history.
`startChild` returns immediately, so starting several and waiting for each in
turn is fan-out.

The child's id is `${parentRunId}#${call}` and the signal it answers on is
`child:${childRunId}`, both derived from where the `startChild` call sits in the
parent. That is what makes starting one exactly-once: a parent that dies between
creating the child and recording the event replays into the same id, finds the
child already there, and does not start a second one.

Waiting is `ctx.waitFor` on that signal and nothing more, so it inherits what
signals already guarantee — a child that finishes before the parent gets to
`waitForChild` is buffered, not lost — and the engine delivers the outcome when
the child reaches a terminal state, including when it is canceled. A child that
failed or was canceled throws `ChildFailedError` into the parent, where
compensation is plain code, exactly like a failed step.

One caveat: telling the parent means writing to a second run, and two runs are
not written atomically. If a worker dies between the child's last write and that
signal, the parent stays blocked on it. Give the wait a `timeoutMs`, or
re-deliver the outcome by hand — the run view names the signal it is waiting on:

```ts
await engine.signal(parentId, childSignal(childRunId), { status: "completed", output });
```

## Inspecting runs

`engine.list` pages over runs, newest first; `engine.view` renders one run for
an admin screen.

```ts
const { runs, cursor } = await engine.list({ workflow: "fulfilment", status: "waiting", limit: 20 });
const next = cursor ? await engine.list({ workflow: "fulfilment", cursor }) : null;

const view = await engine.view(runId);
// {
//   id, workflow: "fulfilment", status: "sleeping", durationMs: 1200,
//   input: { orderId: "ord_42" }, output: undefined, error: null,
//   blockedOn: { kind: "timer", name: "cool-off", until: 1800000003601200 },
//   pendingSignals: {},
//   timeline: [
//     { seq: 0, elapsedMs: 0,    type: "step.completed",  name: "charge",        summary: 'step "charge" completed' },
//     { seq: 1, elapsedMs: 1200, type: "signal.received", name: "manual-review", summary: 'signal "manual-review" received' },
//   ],
// }
```

`blockedOn` is the question an operator actually has — is this run waiting on a
signal, a timer, or a retry backoff, and until when — which otherwise has to be
pieced together from `status`, `waitingFor`, `pendingTimer` and history. The
view is plain JSON: an admin endpoint can return it unchanged.

Paging is keyset, not offset. Runs are ordered by `(createdAt, id)` descending
and `cursor` names one exact row, so a page boundary stays correct while new
runs are being created — an offset would skip or repeat rows. Cursors are
opaque; pass back what the previous page returned. `limit` defaults to 50 and
is capped at 500.

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

Any `RunStore` implementation with `create / get / save(version) / claimDue /
list` works. `save` must be conditional on `version`, `claimDue` must lease
atomically, and `list` must order by `(createdAt, id)` descending — that is the
whole contract.

## Layout

```
src/
  types.ts        RunRecord, HistoryEvent, WorkflowContext, RunStore — the model
  context.ts      replay: each ctx call finds its own event or becomes the frontier
  engine.ts       start / tick / signal / cancel / worker; lease + execute
  retry.ts        backoff policy
  due.ts          what "due" means, shared by engine and stores
  list.ts         listing order and cursor codec, shared by engine and stores
  view.ts         a run rendered for an admin screen: blockedOn + history as a timeline
  children.ts     how a parent names its child and the signal the engine answers on
  stores/memory.ts
  stores/postgres.ts   JSONB record + mirrored query columns + SKIP LOCKED claim
tests/
  engine.test.ts        33 tests with a hand-driven clock: memoisation, durable
                        backoff, early signals, timeouts, nondeterminism, leases,
                        listing, the run view and child runs
  postgres.integration.test.ts   disjoint claims across concurrent workers, stale
                        writes, keyset paging
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

- **Versioned migration of in-flight runs.** Nondeterminism is detected, not
  healed. Drain old runs on the old code, or branch on a version field in input.
- **Very large histories.** A run with tens of thousands of steps replays them
  all on every tick. Snapshotting is the fix and is out of scope here.
- **Scheduling / cron.** Call `engine.start` from whatever already runs on a
  schedule.
