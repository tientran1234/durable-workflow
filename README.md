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

## Step timeouts

A step that fails says so. A step that *hangs* — a socket with no read timeout,
a lock nobody releases — says nothing: the worker sits inside `fn`, the run's
lease runs out, and because a `running` run with no live lease is due again,
another worker replays it and calls the same function. The run moves when one of
those laps happens to return, and not at all if none of them does — with one
attempt on its history the whole time.

`timeoutMs` bounds one attempt:

```ts
await ctx.step("charge", () => stripe.charge(orderId), {
  timeoutMs: 10_000,
  retry: { maxAttempts: 5 },
});
```

An attempt still running after 10s is abandoned and recorded as a failed
attempt — `step.failed` with `timed out after 10000ms`, the attempt counted,
the backoff persisted, `onStepFailed` fired. From there it is an ordinary
failure: four more attempts, then `StepFailedError` into the workflow. The
timeout is the `cause` of that error, so code that treats a hang differently
from a refusal can ask:

```ts
catch (err) {
  if (err instanceof StepFailedError && err.cause instanceof StepTimeoutError) { … }
}
```

The bound is per attempt, not per step, and it applies to an undo too — a
`ctx.compensate` takes the same options, and a hung undo is the worse hang,
since the run is already failing and the phase is waiting on it.

**Keep it well under the engine's `leaseMs`** (30s by default). The timeout is
what makes a hang the step's problem rather than the lease's; set it longer than
the lease and the lease still expires first, which is the behaviour it was there
to replace.

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

## Tags

A run id is the engine's name for a run. An operator arrives holding the
application's — order 4182 — so tag the run with it at start and find it by
that later.

```ts
await engine.start(fulfilment, { orderId: "ord_4182" }, { tags: ["order:ord_4182", "tenant:acme"] });

const { runs } = await engine.list({ tag: "order:ord_4182" });     // which run is handling this?
await engine.list({ tag: "tenant:acme", status: "waiting", limit: 20 });   // and what is stuck
await engine.list({ tag: ["tenant:acme", "order:ord_4182"] });     // the run that is both
```

A tag query narrows alongside `workflow` and `status` and pages by the same
cursor as any other listing. Tags are plain strings with no structure the
engine knows about; `order:` above is a convention, not syntax.

`tag` takes one tag or a set of them, and a set matches only the runs carrying
every tag in it — an operator who knows the tenant and the order is holding two
names for one run. The set narrows, so adding a tag can only remove runs from
the answer, and an empty set narrows nothing, which is what a query assembled
from filters the operator left blank means. A set is trimmed, deduplicated and
order-insensitive like a run's own tags, so `["b", " a "]` and `["a", "b"]` are
the same query.

Tags are fixed when the run starts. They say what the run is about, which is
settled before the first step, and both durable stores mirror them into an
index at that point and never have to revisit it. The index is keyed
`(tag, created_at DESC, run_id DESC)` — the tag, then the order listings come
out in — so a tag query seeks once and reads the page it returns, whether the
tag matches one run or a million. A set of tags enters the table the same way,
through one of them, and tests the rest as a key probe per row the seek already
named: the page still arrives in listing order and the query still stops at
`limit`, where intersecting every tag's runs would mean collecting and sorting
all of them before a page could be ordered at all. Tags also follow
`ctx.continueAsNew` into the next generation, since the chain is one piece of work under one set of names; a
child run is its own work and starts untagged.

A tag that could not be indexed is refused at `engine.start` rather than
dropped: empty, longer than 128 characters, or more than 16 on one run. The
failure mode worth avoiding is an operator searching for a run they cannot
find. Duplicates collapse, and whitespace around a tag is trimmed both
where it is stored and where it is matched, so `" order:1 "` and `"order:1"`
are the same tag.

## Dashboard

`engine.view` is JSON. `engine.dashboard()` is the screen over it, as one
fetch handler:

```ts
const admin = engine.dashboard({ title: "Acme ops" });

Bun.serve({ fetch: admin });                                  // or Deno.serve, or a Worker
app.all("/ops/workflows/*", (c) => admin(c.req.raw));         // or a route in whatever you have
```

What it serves is three screens and one action: the run list, filtered by
workflow, status and tag and paged by the same cursor `engine.list` returns;
one run, with its facts, its input and output, and its history as the timeline
`engine.view` renders; and a form that sends a signal to the run you are
looking at, pre-filled with the name it is waiting for.

Each response is one self-contained HTML document — styles inline, no scripts,
no assets to host, every action a link or a form. There is nothing to build and
nothing to serve beside it, which is the point: a dashboard you have to deploy
is a dashboard nobody has during the incident.

It never has to be told where it is mounted. Routing is entirely in the query
string — `?run=<id>` is one run, no query is the list, a POST sends a signal —
so the links it writes are relative to the path the request arrived on, and the
same handler works at `/ops/workflows`, at the root, or behind a proxy that
rewrites neither.

The payload box is read as JSON, and an empty one as `null` — the untyped
signal whose arrival is the whole message. Anything that is not JSON comes back
as a message on the form rather than reaching the engine as the string somebody
typed, because a schema is waiting on a shape and would refuse it somewhere the
operator cannot see. A schema that does refuse a payload says so on the form
too, and the run page lists every refusal `engine.view` kept — which is the
only place a run still waiting for a signal somebody insists they sent explains
itself.

```ts
// What the dashboard asks of an engine, and all it can do with one.
export interface DashboardEngine {
  list(query: RunQuery): Promise<RunPage>;
  view(id: string): Promise<RunView | null>;
  signal(id: string, name: string, payload?: unknown): Promise<RunRecord>;
}
```

It takes that interface rather than an `Engine`, so the screen can only do what
an operator should: there is no `cancel` on it and no store behind it. Mount it
on a path your admin surface already authenticates — **it authenticates nobody
itself**, it reads every run's input and output, and it can resume a run. See
below for why the CSRF token is that layer's too.

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
side of a handover it landed on. `get`, `view` and `list` still answer about
the record you asked for, which is what makes each generation's history
inspectable on its own; `engine.viewChain` below is the same chain read the way
`signal` addresses it.

```ts
const chain = await engine.viewChain("run-1");      // or "run-1~3"; either answers
// {
//   root: "run-1", live: "run-1~3", generations: 3, status: "sleeping",
//   input: { cursor: null, swept: 0 },              // what the chain was asked to do
//   output: undefined, durationMs: 121_400,
//   blockedOn: { kind: "timer", name: "breathe", until: 1800000181400 },
//   runs: [ /* each generation as engine.view renders it, oldest first */ ],
//   timeline: [
//     { generation: 1, runId: "run-1",   elapsedMs: 0,       summary: 'step "page" completed' },
//     { generation: 1, runId: "run-1",   elapsedMs: 400,     summary: 'step "archive" completed' },
//     { generation: 1, runId: "run-1",   elapsedMs: 60_400,  summary: 'timer "breathe" fired' },
//     { generation: 2, runId: "run-1~2", elapsedMs: 60_700,  summary: 'step "page" completed' },
//     // …
//   ],
// }
```

The chain is found from the root the run names, not from the id you passed, so
an id from six generations ago reads the same as the live one — asking about the
work rather than about a record is the whole difference from `view`. The fields a
chain shares with a run answer about the chain: the root's input, the outcome the
last generation reported, elapsed time across every handover. Offsets on that
timeline are from the root for the same reason, since a per-run offset restarts
at zero at each handover; each generation's own view keeps its own.

A handover leaves no history event — nothing replays it — so the only thing that
marks one on the timeline is `generation` changing. Reading a chain costs one
store read per generation, the same walk `signal` already does; a chain long
enough for that to matter is one to page over with `list({ tag })` instead.

**When to stop is the workflow's business.** Nothing here caps a chain; the
`if` that returns instead of continuing is the only thing that ends one. A
workflow that continues unconditionally is an infinite loop that survives
restarts.

## Event-driven wakeups

A worker polls, and between two passes nothing is happening. A run created
somewhere else — by a web request, by `engine.schedule`, by a parent's
`ctx.startChild` — sits there until some worker's next pass, and so does a run
whose timer or retry backoff has just come due. On average that is half the poll
interval, and the way to shorten it is to poll harder, in every worker, against
the same table.

(`engine.signal` is the case that was never waiting: it executes the run it
resumed in the calling process. It leaves the run for a worker only when another
one holds the lease.)

The Postgres store closes the rest of that gap by saying when a run becomes
due. Every write that leaves a run claimable sends a `NOTIFY` carrying the run's
id and its wake time, and a worker holds a session `LISTEN`ing for them:

```ts
const engine = new Engine({ store: new PostgresStore(pool), workflows });
engine.worker({ pollMs: 30_000 });   // the interval is now the fallback, not the latency
engine.worker({ pollMs: 500, events: false });   // or opt out and poll only
```

A worker reacts to two different things, and the second is why a wakeup carries
a time rather than just an id:

- **a run is due now** — it was just created, or a signal left it runnable for
  somebody else to pick up. The write happens and the notification follows it;
- **a run will be due at `wakeAt`** — it is asleep on a timer or a retry
  backoff. Nothing writes the run when that moment arrives, so the notification
  has to be the one that put it to sleep: the worker keeps the time and sets a
  timer of its own for it.

So a worker holds the wake times it has been told about, soonest first, and one
timer for the earliest of them — `MAX_PENDING_WAKEUPS` of them at most, because
a store may hold millions of sleeping runs and being on time for the next one is
the point.

**Polling stays, and stays load-bearing.** A notification is sent after the
write it reports, in a separate statement, so a process that dies in between
sends nothing. A lease that expires with the worker holding it announces nothing
either — nothing writes the run. A worker that was not listening yet, or whose
session had just dropped, hears nothing. In every one of those cases the run is
claimed on the next pass, which is why the interval is a fallback and not an
optimisation that can be removed: a wakeup is only ever a reason to look early.

`watch` is the whole contract, and it is optional:

```ts
interface WakeupSource {
  watch(onWake: (wake: { runId: string; wakeAt: number | null }) => void): Promise<WakeupSubscription>;
}
```

`MemoryStore` and `SqliteStore` do not implement it and are polled; a worker
checks for the method and does the right thing either way. A store that does
implement it decides which runs are worth a wakeup with `wakeupFor`, so that
what a notification means does not depend on which store sent it: not a terminal
run, not one somebody already holds a lease on, and not a run `waiting` with no
deadline — that one is waiting indefinitely rather than due now, and calling it
due would have every worker claim it on every pass.

A dropped `LISTEN` session is replaced behind the subscription. Notifications
sent while it was gone are not redelivered — Postgres does not queue them for a
session that is not there — which is the same gap as any other, and covered the
same way.

## Design decisions

**Retries are persisted wake times, not `setTimeout`.** A failed step records
`retryAt` in history and puts the run to sleep. The backoff survives a restart;
a process dying mid-wait loses nothing. Attempt counts are derived from history,
so they cannot drift.

**A step timeout is the one thing here that is not durable.** Everything else
that waits is a persisted wake time; `timeoutMs` is a real `setTimeout` in the
worker. A durable deadline can only be noticed on a later tick, and the tick is
what is stuck — so the instrument has to live in the process that is hung, and
nothing of it needs to survive a restart, because a restart already ends the
attempt it was bounding.

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

**A wakeup is a reason to look early, never the reason a run moves.** The
Postgres store notifies listening workers when a run becomes claimable, but it
does so after the write rather than inside it, and Postgres delivers nothing to
a session that is not there. So every wakeup is allowed to be lost, and the
poll interval — not the notification — is what guarantees the run is picked up.
Making the notification reliable instead would mean a durable outbox beside the
run, which is a second thing to write, drain and reason about for a result the
poll already has.

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

| Store | For | Concurrency | Wakeups |
|---|---|---|---|
| `MemoryStore` | tests, scripts, single process | version check | polled |
| `SqliteStore` (`durable-workflow/sqlite`, needs `better-sqlite3`) | one node, and tests that want a real file | version check + one writer at a time | polled |
| `PostgresStore` (`durable-workflow/postgres`, needs `pg`) | production | version check + `FOR UPDATE SKIP LOCKED` | `LISTEN`/`NOTIFY` |

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
atomically, `list` must order by `(createdAt, id)` descending, and a
`list({ tag })` must be answered from an index rather than by reading runs,
one tag or a set of them — that is the whole contract. A `watch` on top of it
makes the store a `WakeupSource`; see **Event-driven wakeups** for what a
worker does with one, and for why it is latency rather than part of the
contract.

## Layout

```
src/
  types.ts        RunRecord, HistoryEvent, WorkflowContext, RunStore — the model
  context.ts      replay: each ctx call finds its own event or becomes the frontier
  engine.ts       start / tick / signal / cancel / worker; lease + execute
  retry.ts        backoff policy
  timeout.ts      the wall-clock bound on one attempt, and why it is not the engine's clock
  due.ts          what "due" means, shared by engine and stores
  versions.ts     the registry: definitions by name and version, and a run's pin
  list.ts         listing order and cursor codec, shared by engine and stores
  tags.ts         what a tag may be, the set a query means, and why a store indexes them at create
  wakeups.ts      what a store may tell a worker, and the wait a wakeup cuts short
  compaction.ts   folding a settled history prefix into a snapshot replay indexes
  view.ts         a run rendered for an admin screen: blockedOn + history as a
                  timeline, and a chain's generations stitched onto one axis
  dashboard.ts    that view as HTML: one fetch handler, routed by query string
  children.ts     how a parent names its child and the signal the engine answers on
  saga.ts         the undos a failing run owes, newest first, and how it reports them
  signals.ts      a signal's name and schema in one value, and the refusals a run keeps
  hooks.ts        the lifecycle payloads, and the call that cannot fail a run
  schedule.ts     the period a clock reading falls in, and the id that period's run takes
  continuation.ts a chain's root, a run's generation, and the id the next one takes
  stores/memory.ts
  stores/sqlite.ts     the same record on a file: one writer, so claiming is one statement
  stores/postgres.ts   JSONB record + mirrored query columns + SKIP LOCKED claim + LISTEN/NOTIFY
                       both keep a (tag, run) table beside the record, indexed in listing order
tests/
  engine.test.ts        120 tests with a hand-driven clock: memoisation, durable
                        backoff, early signals, timeouts, nondeterminism, leases,
                        listing, the run view, child runs, compaction, the
                        lifecycle hooks, scheduled starts, continuations read
                        per generation and as one chain,
                        saga compensation, typed signals, step timeouts, tags,
                        and a worker reacting to wakeups a test sends by hand
  dashboard.test.ts     16 tests driving the handler the way a browser does:
                        follow the link the list writes, read the timeline, post
                        the form, follow the redirect — and a `<script>` tag
                        carried in through an id, a tag, an input and the query
  sqlite.test.ts        11 tests on a real file: a run resumed after the process
                        that started it is gone, stale writes, leases across two
                        connections, keyset paging, and the plan a tag query gets
  postgres.integration.test.ts   disjoint claims across concurrent workers, stale
                        writes, keyset paging, the plan a tag query gets, and
                        the wakeups a run's life sends over LISTEN/NOTIFY
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
- **A chain on the run list, and on the dashboard.** `engine.viewChain` reads a
  chain as one piece of work, but `engine.list` and the screen over it are still
  a page of runs: a chain of nine generations is nine rows, and the dashboard
  links to one run's timeline. Collapsing them would mean a listing that reads
  every row's chain to decide whether to show it, against an index that orders
  runs and knows nothing about generations. A tag names every generation at
  once, which is how to get a chain's runs back in one query.
- **Retagging a run.** `engine.start` is the only place tags are set. Tags are
  the run's identity in the application's terms, which does not change while it
  runs; a mutable set of them is a labelling system, with a second write path
  into the index and a question about what a tag meant at the time. A query may
  name several tags at once, which is the half of this that reads rather than
  writes.
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
- **Cancelling the work a step timeout abandoned.** `timeoutMs` stops waiting
  for the call; it cannot stop the call, because a promise is not interruptible
  and nothing here is handed an `AbortSignal` to pass on. The abandoned work may
  still finish, and the retry may do the same thing a second time — so a step
  with a timeout wants the same idempotency key a step that retries already
  wants. Threading cancellation through would mean a second signature for `fn`
  and a cooperating client on the other end of it; the client's own request
  timeout is that, where it exists.
- **Authentication, and the CSRF token that goes with it.** `engine.dashboard()`
  serves whoever reaches it, and a cross-site form can post to it. Both belong
  to the layer you mount it behind: a token has to be bound to a session, and
  the session is the one thing a handler with no user model cannot have. The
  consequence is a rule rather than a setting — do not route to it from the
  public internet.
- **Cancelling, retrying or editing a run from the dashboard.** The screen can
  send a signal and nothing else. A signal is a message the workflow already
  asked for and handles on a path it defines; a cancel button is an operator
  ending a run from outside its own logic, and a retry button is an operator
  deciding a step's policy was wrong after the fact. Both are `Engine` calls
  with consequences worth writing down at the call site, which is why the
  dashboard takes `DashboardEngine` and not an engine.
- **A dashboard that updates itself.** Each response is a document, so a run
  moving on is a refresh. Live state means a socket or a poll loop, which means
  scripts, which means the one thing this is not: something to build and host
  before it is any use.
- **A wakeup for a lease that expired, and redelivery of one that was missed.**
  A worker that dies holding a run writes nothing, so nothing announces that
  the run is claimable again; the same is true of every notification sent while
  a `LISTEN` session was down. Both are found by the next poll. Closing either
  gap means something watching for leases that lapse, or an outbox the store
  drains — a second source of truth about what is due, when the table already
  answers that exactly.

- **Durable hook delivery.** A lifecycle hook is an in-process call made after
  the write it reports; a worker that dies in between emits nothing, and nothing
  replays it. A side effect that must not be lost goes in the workflow as a step.
- **Calling `engine.schedule` on time, and calendar schedules.** A schedule is
  a fixed period measured from the epoch plus the run id derived from it; the
  loop that calls it on time is still whatever you already run. What a period
  cannot say, a cron expression can — the last weekday of a month, a local
  timezone that observes DST — and that arithmetic stays on your side of
  `every`.
