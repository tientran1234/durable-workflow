# Changelog

## 2026-10-02

- Typed signals: `defineSignal("approve", zodSchema)` so `waitFor` returns the parsed type and `engine.signal` rejects invalid payloads before they touch history, and the rejection is recorded on the run — so a payload from an admin endpoint or a webhook is checked where it enters the run rather than surfacing as a failure somewhere below the wait, and a run still waiting for a signal somebody insists they sent says why on `engine.view`.

## 2026-09-30

- Saga compensation: `ctx.compensate(name, fn)` registers an undo for the step just completed, and when a later step fails for good the registrations run in reverse order, each as a durable step with its own retries — so unwinding a half-applied saga no longer means a catch block that has to know which steps got as far as happening, and a worker that dies mid-undo replays into the phase instead of undoing twice.
- Continuations: `ctx.continueAsNew(input)` ends a long run and hands the work to a fresh one with an empty history, with derived ids per generation — so a run that never ends stops carrying the results replay has already settled, and a signal, a cancellation or a waiting parent still reaches the generation that is running.

## 2026-09-29

- Scheduled starts: `engine.schedule(workflow, input, { every })` with idempotent run ids per period, so the loop that starts a run every hour no longer has to remember the last tick — the run id is derived from the schedule and the period, and every worker can drive the same schedule at once for one run per period.

## 2026-09-28

- Lifecycle hooks: `onRunCompleted`, `onRunFailed`, `onStepFailed` for metrics and alerting, so counting durations and paging on a failure no longer means polling `engine.list` — each fires after the write it reports, once per terminal run and once per failed step attempt, and cannot fail the run it describes.

## 2026-09-27

- SQLite store (`better-sqlite3`, optional peer) for single-node deployments and tests without Postgres: the Postgres store's schema on a file, where claiming due runs is a single `UPDATE … RETURNING` because SQLite admits one writer at a time.

## 2026-09-26

- History compaction for long runs: the engine snapshots the completed prefix of a run's history — one event per settled call, indexed by position — so replay stops scanning the whole history for every call and a long run's tick cost stops growing with what it has already done.

## 2026-09-25

- Versioned workflows: `defineWorkflow("name", run, { version: 2 })`; the run records the version it started on and replays with that version's code, so a deploy no longer changes what a run already in flight means, and nondeterminism stays detected within a version.

## 2026-09-24

- Child workflows: `ctx.startChild(workflow, input)` returning a handle, and `ctx.waitForChild(handle)` built on signals, so a parent can fan work out to runs that retry and are inspected on their own without hand-rolling the start-and-signal pattern.
