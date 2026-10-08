# Roadmap

One item per pull request, in order.

- [x] `engine.list({ workflow?, status?, limit, cursor })` on both stores, plus a tiny JSON admin view of a run (history rendered as a timeline).
- [x] Child workflows: `ctx.startChild(workflow, input)` returning a handle, and `ctx.waitForChild(handle)` built on signals. Document the pattern first, then the helper.
- [x] Versioned workflows: `defineWorkflow("name", run, { version: 2 })`; the run records the version it started on and replays with that version's code. Nondeterminism stays detected within a version.
- [x] History compaction for long runs: snapshot the completed prefix so replay cost stops growing.
- [x] SQLite store (`better-sqlite3`, optional peer) for single-node deployments and tests without Postgres.
- [x] Lifecycle hooks: `onRunCompleted`, `onRunFailed`, `onStepFailed` for metrics and alerting.
- [x] Scheduled starts: `engine.schedule(workflow, input, { every })` with idempotent run ids per period.
- [x] Continuations: `ctx.continueAsNew(input)` ends a long run and hands the work to a fresh one, with derived ids per generation.

## Batch 2 — set by the owner, 30 Sep 2026

Same rule: one item per change, in order.

- [x] Saga compensation: `ctx.compensate(name, fn)` registers an undo for the step just completed; when a later step fails for good, compensations run in reverse order, each as a durable step with its own retries.
- [x] Typed signals: `defineSignal("approve", zodSchema)` so `waitFor` returns the parsed type and `engine.signal` rejects invalid payloads before they touch history; the rejection is recorded on the run.
- [x] Step timeouts: `ctx.step(name, fn, { timeoutMs })` — a hung step becomes a retryable failure with its own history event instead of a lease that expires silently.
- [x] Tags and search: `engine.start(wf, input, { tags })` and `engine.list({ tag })`, indexed on both stores, so an operator finds the run for order X without knowing its id.
- [x] Event-driven wakeups: Postgres `LISTEN/NOTIFY` on run changes so a worker wakes immediately on a signal or a due timer instead of waiting for the poll interval; polling stays as the fallback.
- [x] Dashboard: `engine.dashboard()` returns a single-file HTML handler (fetch-compatible) listing runs, rendering the timeline from `engine.view`, with a form to send a signal.
- [x] A view over a whole continuation chain: `engine.viewChain(id)` answers from any id in the chain, with every generation's view and their timelines on one axis.
- [ ] Matching several tags at once: `engine.list({ tag: [...] })` returns the runs carrying every tag in the set, answered from the same index both durable stores already keep.
