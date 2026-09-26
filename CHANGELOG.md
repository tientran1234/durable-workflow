# Changelog

## 2026-09-26

- History compaction for long runs: the engine snapshots the completed prefix of a run's history — one event per settled call, indexed by position — so replay stops scanning the whole history for every call and a long run's tick cost stops growing with what it has already done.

## 2026-09-25

- Versioned workflows: `defineWorkflow("name", run, { version: 2 })`; the run records the version it started on and replays with that version's code, so a deploy no longer changes what a run already in flight means, and nondeterminism stays detected within a version.

## 2026-09-24

- Child workflows: `ctx.startChild(workflow, input)` returning a handle, and `ctx.waitForChild(handle)` built on signals, so a parent can fan work out to runs that retry and are inspected on their own without hand-rolling the start-and-signal pattern.
