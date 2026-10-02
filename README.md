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

## Typed signals

A signal is the one input to a run that comes from outside TypeScript — an
admin endpoint, a webhook, somebody's `curl`. `ctx.waitFor<{ approved: boolean }>`
is a cast on trust: nothing checks that the payload is that, so
`{ approved: "yes" }` is recorded, handed to the workflow, and surfaces as a
failure somewhere below the wait with the history already carrying the cause.

`defineSignal` puts the name and the schema in one value, which both sides read:

```ts
import { z } from "zod";
import { defineSignal } from "durable-workflow";

const approval = defineSignal("manual-review", z.object({ approved: z.boolean(), note: z.string().default("") }));

const fulfilment = defineWorkflow<{ orderId: string }, string>("fulfilment", async (ctx, { orderId }) => {
  const review = await ctx.waitFor(approval, { timeoutMs: 24 * 3600_000 });   // { approved: boolean; note: string }
  return review.approved ? "shipped" : "refunded";
});

await engine.signal(runId, approval, await request.json());   // throws SignalRejectedError on anything else
```

A schema is anything with a `parse(payload)` method, which is zod's shape and
nothing more, so the library a caller already uses stays theirs and none of
them is a dependency here. A hand-written `{ parse }` works just as well.

**The check happens on the way in, once.** `engine.signal` is where a payload
crosses into a run, so a payload that fails is refused there: nothing is
appended to history, nothing is buffered, and the run is left exactly as the
wait found it. Everything in `pendingSignals` and in history below is therefore
a value some schema approved, which is what lets `ctx.waitFor` return the
schema's output type without parsing it again — a second check could only
disagree with the record it was reading, and a schema tightened under a live run
would fail that run rather than the deploy that tightened it.

**A schema is a gate, not a codec.** What gets recorded is what the schema
returned, so defaults and coercions reach the workflow. What history holds is
JSON, so a schema whose output is not — a `Date`, a `Map`, a class instance —
comes back as the JSON of it on the next replay, typed as the thing it no
longer is. Keep the output JSON-shaped and parse the rest inside a step.

**An endpoint has a name, not a definition.** Register the signals with the
engine and `engine.signal(runId, "manual-review", payload)` is checked too:

```ts
const engine = new Engine({ store, workflows, signals: [approval] });
```

A name with no registered schema is unvalidated rather than refused. That is
what keeps an untyped `ctx.waitFor("whatever")` working, and it is also load
bearing: the signal a child reports its outcome on is derived from its run id,
so there is no schema for the engine to hold it to.

**A refusal is recorded on the run, and it is not history.** Not touching
history is the guarantee, so the record carries a capped list of refusals
beside it, which `engine.view` reports:

```ts
const view = await engine.view(runId);
// rejectedSignals: [{ name: "manual-review", error: "approved: expected boolean, received string", at: 1800000001000 }]
```

That is the answer to the one question a refusal creates — this run is still
waiting for a signal somebody insists they sent — and the timeline cannot give
it, because the payload never got that far. The payload itself is deliberately
not kept: it came from outside and has just been established not to be a shape
anything here understands. Only the most recent ten are, so a caller retrying a
malformed payload in a loop cannot grow the record without bound.

## Sagas

A step that fails for good throws `StepFailedError` *into* the workflow
function, so the smallest saga is a `try`/`catch` that runs the undo itself:

```ts
try {
  await ctx.step("ship", () => courier.book(orderId));
} catch (err) {
  if (!(err instanceof StepFailedError)) throw err;
  await ctx.step("refund", () => stripe.refund(payment.id));
  return "refunded";
}
```

That stops scaling at the second thing to undo. Four steps in, the catch block
has to know which of them got as far as happening, undo them in the right order,
give each undo its own retries — and do all of it again from the top when the
worker running the catch block dies halfway through.

`ctx.compensate` puts the undo next to the work it reverses instead:

```ts
const fulfilment = defineWorkflow<{ orderId: string }, string>("fulfilment", async (ctx, { orderId }) => {
  const hold = await ctx.step("reserve", () => inventory.hold(orderId));
  ctx.compensate("release", () => inventory.release(hold.id));

  const payment = await ctx.step("charge", () => stripe.charge(orderId));
  ctx.compensate("refund", () => stripe.refund(payment.id), { retry: { maxAttempts: 10 } });

  await ctx.step("ship", () => courier.book(orderId));    // throws after its last attempt
  return "shipped";
});
```

If `ship` fails for good and nothing catches it, the engine runs `refund` and
then `release`, and only then fails the run. Each undo is a durable step of its
own: it happens at most once, retries under its own policy, and its backoff is a
persisted wake time — so a worker that dies between `refund` and `release`
replays into the phase and carries on rather than refunding twice.

Newest first because a saga is a stack: the later work was done on top of the
earlier, so undoing outwards is the only order in which each undo finds the
state it was registered against.

**Registering records nothing.** `ctx.compensate` does not suspend and writes no
history event, which is why there is nothing to await: replay re-registers by
re-executing, so the undos that exist are exactly the ones the run reached. A
registration that took a call position of its own would be a position that never
settles, and compaction would stop folding at the first one.

**The failure has to escape the function.** A `try`/`catch` that handles a step
failure is the workflow saying it has another path, so nothing is undone — the
two styles do not fight, and the hand-written one above still works unchanged.
What runs the undos is the run failing.

**`fn` is not a workflow.** It runs inside a durable unit, so it must not call
`ctx.*`: those calls have no position of their own, and the phase is past the
point where the function could suspend.

**An undo that gives up does not stop the rest.** After its last attempt the
phase moves to the next registration: the work below it is still done, and
abandoning that would leave more of the saga applied rather than less. The run
then fails naming the cause and every undo that did not happen.

```
step "ship" failed after 3 attempt(s): courier down; compensation did not complete: "refund" (gateway down)
```

**Nothing is undone when replay and the code disagree.** A `NondeterminismError`
means the history and the function no longer describe the same run, so which
undos it owes is precisely what neither can say. The run fails without
compensating.

Undos are their own events rather than steps, so `engine.view` shows them as
such — `compensation "refund" completed` — and a run sleeping on an undo's
backoff says so:

```ts
// blockedOn: { kind: "retry", name: "refund", until: 1800000001000 }
```

Registrations do not cross a handover: `ctx.continueAsNew` starts a run with an
empty history, so what the successor should undo is part of what you hand it.

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

## Versions

Changing a workflow that has runs in flight is the awkward case: the code the
engine replays is no longer the code those runs recorded. A version says which
revision a run belongs to.

```ts
// The fulfilment workflow above is v1. v2 drops the manual review — a change
// that would replay a live run straight past a wait it is sitting in.
const fulfilmentV2 = defineWorkflow<{ orderId: string }, string>("fulfilment", async (ctx, { orderId }) => {
  await ctx.step("charge", () => stripe.charge(orderId));
  await ctx.step("ship", () => courier.book(orderId));
  return "shipped";
}, { version: 2 });

const engine = new Engine({ store, workflows: [fulfilment, fulfilmentV2] });   // both stay registered
```

A run records the version it started on and replays on that version's code for
its whole life. Runs waiting for a review on v1 still get one; runs started
after the deploy ship without it. `engine.start(fulfilment, input)` starts on
the version of the definition you hand it, `engine.start("fulfilment", input)`
on the highest registered — so the default is the newest and pinning a run to
an older version is deliberate.

Keep the old definition registered until its runs drain. `engine.list` and
`engine.view` report `workflowVersion`, which is how you tell when that is:

```ts
const { runs } = await engine.list({ workflow: "fulfilment", status: "waiting" });
const stragglers = runs.filter((run) => runVersion(run) === 1);
```

A version is one registration, not a label: registering the same name twice at
one version throws at startup, because two bodies under one version are
indistinguishable to a run. Omitted, `version` is 1.

Bump it when a change would make a live run replay differently — a renamed or
reordered `ctx` call, a branch that skips one. Changing what happens *inside* a
step does not need a bump; the step's recorded result is what replay uses.
Getting that judgement wrong is not silent: shipping a changed body under the
same version still fails the run with `NondeterminismError` naming both names.

## Schedules

Something has to start a run every hour. The hard part is not the timer: it is
that the loop holding it has no memory. A retried tick, a process that restarts
and fires its interval early, or a second worker running the same loop each
start another run for an hour that already ran.

```ts
const rollup = defineWorkflow<{ job: string }, void>("rollup", async (ctx) => {
  const rows = await ctx.step("collect", () => warehouse.scan());
  await ctx.step("publish", () => warehouse.publish(rows));
});

// In every worker process, in whatever loop already runs there.
setInterval(() => void engine.schedule(rollup, { job: "daily" }, { every: 3600_000 }), 30_000);
```

That is one run an hour between all of them, at any call frequency, because
the run's id is `${name}@${periodStart}` — derived from the schedule and the
period rather than generated. A call that finds a run already under that id has
its answer without writing anything, which is what makes the loop disposable:
an interval, a cron entry, every worker at once, none of them remembering the
last tick. The return value says which run the period got and whether this call
is the one that started it:

```ts
const { runId, periodStart, created } = await engine.schedule(rollup, input, { every: 3600_000 });
```

Periods are measured from the epoch, not from the first call, so processes that
never speak agree on the boundaries: `every: 3600_000` is the top of each hour
and `every: 86_400_000` is UTC midnight, on a worker that started yesterday and
one that started a second ago. `name` separates two schedules over one
workflow; without it they share the workflow's name, and therefore a run.

A scheduled run is an ordinary run — it retries, waits for signals, and shows
up in `engine.list` and `engine.view` — and the schedule does not wait for it.
The next period starts a new run whether or not this one has finished. Where
two must not overlap, the workflow is what knows: start it with a step that
checks.

Nothing about the schedule itself is stored. The period's run *is* the record
that the period fired, which is what a call reads to decide, so changing
`every` or removing the schedule is a deploy rather than a migration — and a
period nobody called during is skipped, not backfilled.

## Inspecting runs

`engine.list` pages over runs, newest first; `engine.view` renders one run for
an admin screen.

```ts
const { runs, cursor } = await engine.list({ workflow: "fulfilment", status: "waiting", limit: 20 });
const next = cursor ? await engine.list({ workflow: "fulfilment", cursor }) : null;

const view = await engine.view(runId);
// {
//   id, workflow: "fulfilment", workflowVersion: 1, status: "sleeping", durationMs: 1200,
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

## Hooks

`engine.list` and `engine.view` answer a question when you ask it. Hooks are the
other direction: the engine tells you as runs pass the three points worth
counting.

```ts
const engine = new Engine({
  store,
  workflows,
  hooks: {
    onRunCompleted: ({ workflow, durationMs }) => metrics.timing(`workflow.${workflow}.duration`, durationMs),
    onRunFailed: ({ workflow, runId, error }) => pager.alert(`${workflow} run ${runId} failed: ${error}`),
    onStepFailed: ({ workflow, step, attempt, retryAt }) =>
      retryAt === null
        ? pager.alert(`${workflow} step "${step}" gave up after ${attempt} attempt(s)`)
        : metrics.increment(`workflow.${workflow}.step.${step}.retry`),
  },
});
```

`onStepFailed` fires once per failed attempt, and `retryAt` is the distinction
alerting actually cares about: a number is the time the engine will try again,
`null` means the policy is spent and the failure is about to land in the
workflow function as `StepFailedError`. Every payload carries `runId`,
`workflow` and the `workflowVersion` the run is pinned to, so a metric stays
attributable to the code that produced it while a deploy drains.

Three properties, all deliberate.

**A hook cannot change what a run does.** It is called after the state it
describes is persisted, and whatever it throws is dropped: a statsd socket that
is down must not fail a run that has already succeeded. Anything that needs to
influence the workflow belongs in the workflow, as a step.

**Delivery is in-process and at-most-once.** The call happens in the worker that
finished the run, straight after the write. A process that dies in between emits
nothing and nothing re-emits it — the history is the audit trail, a hook is not.
A notification that must not be lost is `ctx.step("notify", …)`, retried and
recorded like any other side effect.

**Hooks are awaited.** A slow hook slows the worker that called it, which is the
honest default rather than an unhandled rejection later; hand the I/O to a queue
yourself if you would rather it did not.

There is no hook for a canceled run. `engine.cancel` returns to the caller that
asked for the cancellation, which is the only party a hook would be telling.

## Long runs

Replay re-executes the workflow function from the top, and each `ctx` call has
to find its own event. Searching the whole history for every call costs the
length of that history per call, so a run with tens of thousands of steps gets
slower on every tick — the work grows with what the run has already done.

Compaction folds the **settled prefix** of a history into a snapshot: one event
per call, indexed by position, which replay reads directly.

```ts
const engine = new Engine({ store, workflows, compactAfter: 1000 });   // the default
```

A call is settled when replay neither does work nor suspends at it: a completed
step, a step that failed for the last time, a signal received or timed out, a
fired timer, a started child. Only a prefix is ever foldable, and that comes
free: a pass suspends at the frontier, so nothing above an unsettled call has
events yet. A step serving out its backoff is *not* settled, so its attempt
count is untouched.

Two things this does not do. It does not make the function itself cheaper —
nine thousand memoised steps are still nine thousand calls — it stops each one
paying for the history behind it. And it does not shrink the record: replay
needs the recorded results, so they stay. What it drops are the attempts a step
made before it succeeded, which is why a retry-heavy run stops carrying them.

That drop is the one cost. `engine.view` reports it rather than hiding it:

```ts
const view = await engine.view(runId);
// compaction: { calls: 9_412, droppedEvents: 118, at: 1800000900000 }
```

The timeline still lists every settled call; the 118 superseded attempts within
the first 9,412 positions are gone. A run whose full audit trail matters more
than its tick cost sets `compactAfter: Infinity` and keeps all of it.

## Continuations

Compaction stops replay paying for a history it has already settled, but the
settled results themselves stay — replay needs them — so a run that never ends
still grows. `ctx.continueAsNew` is where it ends: the run stops, and the work
carries on in a fresh run of the same workflow with an empty history.

```ts
const sweep = defineWorkflow<{ cursor: string | null; swept: number }, number>("sweep", async (ctx, state) => {
  const batch = await ctx.step("page", () => db.page(state.cursor));
  await ctx.step("archive", () => archive(batch.rows));

  const swept = state.swept + batch.rows.length;
  if (batch.next === null) return swept;

  await ctx.sleep("breathe", 60_000);
  return ctx.continueAsNew({ cursor: batch.next, swept });   // never returns
});
```

It unwinds the function the way `waitFor` does, so it is the last thing the
call reaches — `return ctx.continueAsNew(…)` rather than a call whose result
gets used. What the successor knows is what you hand it: its history is empty,
so a memoised step is not memoised there, which is the point.

The successor's id is `${root}~${generation}` — `run-1`, `run-1~2`, `run-1~3` —
derived from the chain rather than from the run it follows. Derived, so
starting it is exactly-once: a pass that dies between creating the successor
and recording the handover replays into the same run instead of forking a
second chain. From the chain rather than its predecessor, so a run that
continues a thousand times does not carry a thousand suffixes.

A handover is not a completion. The run ends as `continued` with a pointer to
the successor, and `onRunCompleted` fires once per chain rather than once per
generation, because a generation that continued has produced no output — and a
parent waiting on it is owed the chain's outcome, so the link moves on with the
work and the last generation is what reports.

```ts
const view = await engine.view("run-1");
// status: "continued", chain: { root: "run-1", generation: 1 }, continuation: { runId: "run-1~2" }
```

**The successor starts on the newest registered version.** A run pinned to v1
continues onto v2 if v2 is registered. Nothing else about versions changes —
within a generation a run still replays on the version it started on — but a
run that continues forever would otherwise never reach new code, and a handover
is the one point in its life where no history has to survive the change. It is
the place a deploy drains through.

**A chain is one piece of work from the outside.** `engine.signal`, `cancel`
and `settle` walk to the generation that is running, so an admin screen or a
child holding an id from six generations ago still addresses the work rather
than a run that is done. Signals the predecessor had buffered and never
consumed move over too: whether a signal survives should not depend on which
side of a handover it landed on. Reading stays per-run — `get`, `view` and
`list` answer about the record you asked for, which is what makes each
generation's history inspectable on its own.

**When to stop is the workflow's business.** Nothing here caps a chain; the
`if` that returns instead of continuing is the only thing that ends one. A
workflow that continues unconditionally is an infinite loop that survives
restarts.

## Design decisions

**Retries are persisted wake times, not `setTimeout`.** A failed step records
`retryAt` in history and puts the run to sleep. The backoff survives a restart;
a process dying mid-wait loses nothing. Attempt counts are derived from history,
so they cannot drift.

**Signals that arrive early are not lost.** `engine.signal()` on a run that has
not reached the matching `waitFor` yet buffers the payload; the `waitFor`
consumes it when it gets there, in order. A buffered payload has been through
its schema already — the check is on the way in, not on the way out — so what
is waiting there is never a shape the workflow cannot read.

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
compensation can be plain code: catch it, run a `refund` step, return. What
`ctx.compensate` adds is only the part of that which stops scaling — reverse
order, one durable undo per registration, retries per undo — for the case where
the failure is left to escape.

## Stores

| Store | For | Concurrency |
|---|---|---|
| `MemoryStore` | tests, scripts, single process | version check |
| `SqliteStore` (`durable-workflow/sqlite`, needs `better-sqlite3`) | one node, and tests that want a real file | version check + one writer at a time |
| `PostgresStore` (`durable-workflow/postgres`, needs `pg`) | production | version check + `FOR UPDATE SKIP LOCKED` |

`SqliteStore` is the Postgres store's schema on a file: the record as JSON, the
columns a worker queries by mirrored beside it.

```ts
import Database from "better-sqlite3";
import { SqliteStore } from "durable-workflow/sqlite";

const store = new SqliteStore(new Database("runs.db"));
store.ensureSchema();          // also sets WAL and a busy timeout
```

It runs several workers in one process, and several processes over one file.
What Postgres needs `SKIP LOCKED` for, SQLite gets from admitting one writer at
a time: `claimDue` is a single `UPDATE … RETURNING`, so the runs it selects are
the runs it leases, and the next worker's claim runs after that one commits and
sees the leases it took. The cost is the other side of the same coin — writers
queue rather than proceed in parallel, which is why a busy timeout is set and
why many workers still want Postgres.

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
  versions.ts     the registry: definitions by name and version, and a run's pin
  list.ts         listing order and cursor codec, shared by engine and stores
  compaction.ts   folding a settled history prefix into a snapshot replay indexes
  view.ts         a run rendered for an admin screen: blockedOn + history as a timeline
  children.ts     how a parent names its child and the signal the engine answers on
  saga.ts         the undos a failing run owes, newest first, and how it reports them
  signals.ts      a signal's name and schema in one value, and the refusals a run keeps
  hooks.ts        the lifecycle payloads, and the call that cannot fail a run
  schedule.ts     the period a clock reading falls in, and the id that period's run takes
  continuation.ts a chain's root, a run's generation, and the id the next one takes
  stores/memory.ts
  stores/sqlite.ts     the same record on a file: one writer, so claiming is one statement
  stores/postgres.ts   JSONB record + mirrored query columns + SKIP LOCKED claim
tests/
  engine.test.ts        95 tests with a hand-driven clock: memoisation, durable
                        backoff, early signals, timeouts, nondeterminism, leases,
                        listing, the run view, child runs, compaction, the
                        lifecycle hooks, scheduled starts, continuations,
                        saga compensation and typed signals
  sqlite.test.ts        9 tests on a real file: a run resumed after the process
                        that started it is gone, stale writes, leases across two
                        connections, keyset paging
  postgres.integration.test.ts   disjoint claims across concurrent workers, stale
                        writes, keyset paging
```

## Run

```bash
pnpm install
pnpm test                       # unit tests and the SQLite store need nothing
pnpm db:up                      # Postgres on :5434
DATABASE_URL=postgresql://postgres:postgres@localhost:5434/workflow pnpm test
```

CI runs the full suite, Postgres included, on every push.

## What is deliberately not here

- **Migrating an in-flight run between versions.** A run finishes on the
  version it started on; there is no hook to rewrite its history onto the next
  one. Keep the old definition registered until those runs drain.
- **A view over a whole continuation chain.** `engine.list` and `engine.view`
  answer about one run, and a chain is a run per generation, so an operator
  following one walks it by id — `run-1`, `run-1~2` — rather than reading it as
  a single timeline. Stitching them is a query over `chain.root`, which is the
  store's job and not this library's.
- **Continuing a run from outside it.** `ctx.continueAsNew` is a decision the
  workflow makes about its own state, and only the workflow knows what the next
  generation needs to be handed. There is no `engine.continueAsNew`; cancelling
  a run and starting another is that, without the pretence that the two are one
  piece of work.
- **Compensating a canceled run.** `engine.cancel` marks a run canceled without
  executing it, and the undos a run owes only exist inside a pass that replays
  its function, so cancelling undoes nothing. Unwinding a saga on purpose is a
  decision about state that the workflow has to make: signal it, and let the
  function take the path that ends the run itself.
- **Re-checking a payload that is already in the run.** A schema is applied
  where a payload enters, so one that was buffered before its signal had a
  schema — or sent by a name the engine had no registration for — stays as it
  arrived, and tightening a schema does not refuse the runs already holding the
  old shape. Validating on the way out instead would put the engine in the
  position of failing a live run over a deploy it cannot see, which is what
  `version` is for.
- **Durable hook delivery.** A lifecycle hook is an in-process call made after
  the write it reports; a worker that dies in between emits nothing, and nothing
  replays it. A side effect that must not be lost goes in the workflow as a step.
- **Calling `engine.schedule` on time, and calendar schedules.** A schedule is
  a fixed period measured from the epoch plus the run id derived from it; the
  loop that calls it on time is still whatever you already run. What a period
  cannot say, a cron expression can — the last weekday of a month, a local
  timezone that observes DST — and that arithmetic stays on your side of
  `every`.
