import { WorkflowNotFoundError } from "./errors.js";
import type { RunRecord, WorkflowDefinition } from "./types.js";

/**
 * The version a workflow defined without one belongs to, and the version a run
 * created before versions existed replays on.
 */
export const DEFAULT_VERSION = 1;

/**
 * The version a run is pinned to for the rest of its life. Records written
 * before versions existed carry no version and are version 1: they were started
 * on the only code there was.
 */
export function runVersion(run: Pick<RunRecord, "workflowVersion">): number {
  return run.workflowVersion ?? DEFAULT_VERSION;
}

/**
 * The version a `list({ version })` query means, checked where every store
 * reads it — the way each of them calls `pageLimit` and `queryTags` — so one
 * spelling of the question cannot be answered differently depending on which
 * store is holding the runs.
 *
 * A version is a registration rather than a label, so a query has to name one
 * a run could have started on: a fraction, a zero or a NaN names no
 * definition, and answering it with no runs would read as "the runs on that
 * version have drained", which is the one answer this must not invent.
 */
export function queryVersion(version: number | undefined): number | undefined {
  if (version === undefined) return undefined;
  if (!Number.isInteger(version) || version < DEFAULT_VERSION) {
    throw new Error(`a version query names a whole version from ${DEFAULT_VERSION}, got ${version}`);
  }
  return version;
}

/**
 * The workflows an engine knows, keyed by name *and* version, so several
 * versions of one name live here side by side. That is what lets a deploy add
 * v2 without touching runs that started on v1: a run replays on the version it
 * recorded, and only new runs get the latest.
 */
export class WorkflowRegistry {
  private readonly byName = new Map<string, Map<number, WorkflowDefinition>>();

  add(definition: WorkflowDefinition): void {
    const versions = this.byName.get(definition.name) ?? new Map<number, WorkflowDefinition>();
    if (versions.has(definition.version)) {
      // Nearly always a forgotten version bump. Two different functions under
      // one version are indistinguishable to a run, which is exactly the
      // corruption versions exist to prevent — so refuse at startup.
      throw new Error(`workflow "${definition.name}" is registered twice at version ${definition.version}`);
    }
    versions.set(definition.version, definition);
    this.byName.set(definition.name, versions);
  }

  has(name: string): boolean {
    return this.byName.has(name);
  }

  /** The version a new run starts on: the highest registered. */
  latest(name: string): WorkflowDefinition {
    const versions = this.byName.get(name);
    if (!versions) throw new WorkflowNotFoundError(name);
    const highest = Math.max(...versions.keys());
    return versions.get(highest) as WorkflowDefinition;
  }

  /** The exact version a run is pinned to. */
  get(name: string, version: number): WorkflowDefinition {
    const definition = this.byName.get(name)?.get(version);
    if (!definition) throw new WorkflowNotFoundError(name, version);
    return definition;
  }
}
