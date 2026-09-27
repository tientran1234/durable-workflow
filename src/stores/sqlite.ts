import type { Database } from "better-sqlite3";
import type { RunPage, RunQuery, RunRecord, RunStore } from "../types.js";

interface Row {
  data: string;
  status: RunRecord["status"];
  wake_at: number | null;
  lease_until: number | null;
  version: number;
}

/**
 * SQLite-backed store for a single node: the same shape as the Postgres one —
 * the full record as JSON, the columns a worker queries by mirrored beside it —
 * on a file instead of a server.
 *
 * `better-sqlite3` is an optional peer dependency — install it only if you use
 * this store.
 */
export class SqliteStore implements RunStore {
  constructor(
    private readonly db: Database,
    private readonly table = "workflow_runs",
  ) {}

  /**
   * Idempotent. Run once at startup. It also sets the two pragmas the
   * concurrency story rests on: WAL, so a worker reading runs is not blocked by
   * one writing, and a busy timeout, so a second writer waits its turn instead
   * of failing with SQLITE_BUSY.
   */
  ensureSchema(): void {
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("busy_timeout = 5000");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id          TEXT PRIMARY KEY,
        workflow    TEXT NOT NULL,
        status      TEXT NOT NULL,
        wake_at     INTEGER,
        lease_until INTEGER,
        version     INTEGER NOT NULL,
        data        TEXT NOT NULL,
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL
      )`);
    this.db.exec(
      `CREATE INDEX IF NOT EXISTS ${this.table}_due ON ${this.table} (status, wake_at) WHERE status IN ('running','sleeping','waiting')`,
    );
    this.db.exec(
      `CREATE INDEX IF NOT EXISTS ${this.table}_recent ON ${this.table} (created_at DESC, id DESC)`,
    );
  }

  async create(_run: RunRecord): Promise<void> {
    throw new Error("SqliteStore.create is not implemented");
  }

  async get(_id: string): Promise<RunRecord | null> {
    throw new Error("SqliteStore.get is not implemented");
  }

  async save(_run: RunRecord, _expectedVersion: number): Promise<boolean> {
    throw new Error("SqliteStore.save is not implemented");
  }

  async claimDue(_now: number, _leaseMs: number, _limit: number): Promise<RunRecord[]> {
    throw new Error("SqliteStore.claimDue is not implemented");
  }

  async list(_query: RunQuery): Promise<RunPage> {
    throw new Error("SqliteStore.list is not implemented");
  }

  private hydrate(row: Row): RunRecord {
    return {
      ...(JSON.parse(row.data) as RunRecord),
      status: row.status,
      wakeAt: row.wake_at,
      leaseUntil: row.lease_until,
      version: row.version,
    };
  }
}
