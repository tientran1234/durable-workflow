import type { Database } from "better-sqlite3";
import { decodeCursor, encodeCursor, pageLimit } from "../list.js";
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

  async create(run: RunRecord): Promise<void> {
    this.db
      .prepare(
        `INSERT INTO ${this.table} (id, workflow, status, wake_at, lease_until, version, data, created_at, updated_at)
         VALUES (@id, @workflow, @status, @wakeAt, @leaseUntil, @version, @data, @createdAt, @updatedAt)`,
      )
      .run({
        id: run.id,
        workflow: run.workflow,
        status: run.status,
        wakeAt: run.wakeAt,
        leaseUntil: run.leaseUntil,
        version: run.version,
        data: JSON.stringify(run),
        createdAt: run.createdAt,
        updatedAt: run.updatedAt,
      });
  }

  async get(id: string): Promise<RunRecord | null> {
    const row = this.db
      .prepare<[string], Row>(`SELECT data, status, wake_at, lease_until, version FROM ${this.table} WHERE id = ?`)
      .get(id);
    return row ? this.hydrate(row) : null;
  }

  async save(run: RunRecord, expectedVersion: number): Promise<boolean> {
    const next = expectedVersion + 1;
    const { changes } = this.db
      .prepare(
        `UPDATE ${this.table}
            SET data = @data, status = @status, wake_at = @wakeAt, lease_until = @leaseUntil,
                version = @version, updated_at = @updatedAt
          WHERE id = @id AND version = @expected`,
      )
      .run({
        id: run.id,
        data: JSON.stringify({ ...run, version: next }),
        status: run.status,
        wakeAt: run.wakeAt,
        leaseUntil: run.leaseUntil,
        version: next,
        updatedAt: run.updatedAt,
        expected: expectedVersion,
      });
    if (changes !== 1) return false;
    run.version = next;
    return true;
  }

  /**
   * One statement, so it is one implicit transaction: the rows it selects are
   * the rows it leases. Postgres needs SKIP LOCKED to keep workers from
   * blocking one another over the same candidates; SQLite admits one writer at
   * a time, so a second worker's claim runs after this one has committed and
   * sees the leases it took.
   */
  async claimDue(now: number, leaseMs: number, limit: number): Promise<RunRecord[]> {
    const rows = this.db
      .prepare<{ now: number; leaseMs: number; limit: number }, Row>(
        `UPDATE ${this.table}
            SET lease_until = @now + @leaseMs, version = version + 1, updated_at = @now
          WHERE id IN (
            SELECT id FROM ${this.table}
             WHERE (status = 'running'
                    OR (status IN ('sleeping','waiting') AND wake_at IS NOT NULL AND wake_at <= @now))
               AND (lease_until IS NULL OR lease_until < @now)
             ORDER BY wake_at NULLS FIRST, created_at
             LIMIT @limit
          )
        RETURNING data, status, wake_at, lease_until, version`,
      )
      .all({ now, leaseMs, limit });
    return rows.map((row) => this.hydrate(row));
  }

  /**
   * Keyset pagination, the same row comparison as the Postgres store:
   * `(created_at, id) < (cursor)`, which the (created_at DESC, id DESC) index
   * answers directly and which stays exact while new runs are being created.
   */
  async list(query: RunQuery): Promise<RunPage> {
    const limit = pageLimit(query.limit);
    const cursor = query.cursor === undefined ? null : decodeCursor(query.cursor);
    const rows = this.db
      .prepare<
        { workflow: string | null; status: string | null; createdAt: number | null; id: string | null; limit: number },
        Row
      >(
        `SELECT data, status, wake_at, lease_until, version FROM ${this.table}
          WHERE (@workflow IS NULL OR workflow = @workflow)
            AND (@status IS NULL OR status = @status)
            AND (@createdAt IS NULL OR (created_at, id) < (@createdAt, @id))
          ORDER BY created_at DESC, id DESC
          LIMIT @limit`,
      )
      .all({
        workflow: query.workflow ?? null,
        status: query.status ?? null,
        createdAt: cursor?.createdAt ?? null,
        id: cursor?.id ?? null,
        limit: limit + 1,
      });
    // One extra row tells us whether a next page exists without a second query.
    const runs = rows.slice(0, limit).map((row) => this.hydrate(row));
    const last = runs[runs.length - 1];
    return { runs, cursor: rows.length > limit && last ? encodeCursor(last) : null };
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
