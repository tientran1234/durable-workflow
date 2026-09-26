# Roadmap

One item per pull request, in order.

- [x] `engine.list({ workflow?, status?, limit, cursor })` on both stores, plus a tiny JSON admin view of a run (history rendered as a timeline).
- [x] Child workflows: `ctx.startChild(workflow, input)` returning a handle, and `ctx.waitForChild(handle)` built on signals. Document the pattern first, then the helper.
- [x] Versioned workflows: `defineWorkflow("name", run, { version: 2 })`; the run records the version it started on and replays with that version's code. Nondeterminism stays detected within a version.
- [x] History compaction for long runs: snapshot the completed prefix so replay cost stops growing.
- [ ] SQLite store (`better-sqlite3`, optional peer) for single-node deployments and tests without Postgres.
- [ ] Lifecycle hooks: `onRunCompleted`, `onRunFailed`, `onStepFailed` for metrics and alerting.
- [ ] Scheduled starts: `engine.schedule(workflow, input, { every })` with idempotent run ids per period.
