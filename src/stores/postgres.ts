import type { Pool } from "pg";
import { decodeCursor, encodeCursor, pageLimit } from "../list.js";
import type { RunPage, RunQuery, RunRecord, RunStore } from "../types.js";

interface Row {
  data: RunRecord;
  status: RunRecord["status"];
  wake_at: string | number | null;
  lease_until: string | number | null;
  version: number;
}

const num = (v: string | number | null): number | null => (v === null ? null : Number(v));

/**
 * Postgres-backed store. The full record lives in a JSONB column; the columns
 * a worker queries by (status, wake_at, lease_until, version) are mirrored so
 * claimDue is one indexed statement.
 *
 * `pg` is an optional peer dependency — install it only if you use this store.
 */
export class PostgresStore implements RunStore {
  constructor(
    private readonly pool: Pool,
    private readonly table = "workflow_runs",
  ) {}

  /** Idempotent. Run once at startup or in a migration. */
  async ensureSchema(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        id          TEXT PRIMARY KEY,
        workflow    TEXT NOT NULL,
        status      TEXT NOT NULL,
        wake_at     BIGINT,
        lease_until BIGINT,
        version     INTEGER NOT NULL,
        data        JSONB NOT NULL,
        created_at  BIGINT NOT NULL,
        updated_at  BIGINT NOT NULL
      )`);
    await this.pool.query(
      `CREATE INDEX IF NOT EXISTS ${this.table}_due ON ${this.table} (status, wake_at) WHERE status IN ('running','sleeping','waiting')`,
    );
    await this.pool.query(
      `CREATE INDEX IF NOT EXISTS ${this.table}_recent ON ${this.table} (created_at DESC, id DESC)`,
    );
  }

  async create(run: RunRecord): Promise<void> {
    await this.pool.query(
      `INSERT INTO ${this.table} (id, workflow, status, wake_at, lease_until, version, data, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)`,
      [run.id, run.workflow, run.status, run.wakeAt, run.leaseUntil, run.version, JSON.stringify(run), run.createdAt, run.updatedAt],
    );
  }

  async get(id: string): Promise<RunRecord | null> {
    const { rows } = await this.pool.query<Row>(
      `SELECT data, status, wake_at, lease_until, version FROM ${this.table} WHERE id = $1`,
      [id],
    );
    const row = rows[0];
    return row ? this.hydrate(row) : null;
  }

  async save(run: RunRecord, expectedVersion: number): Promise<boolean> {
    const next = expectedVersion + 1;
    const { rowCount } = await this.pool.query(
      `UPDATE ${this.table}
         SET data = $2::jsonb, status = $3, wake_at = $4, lease_until = $5, version = $6, updated_at = $7
       WHERE id = $1 AND version = $8`,
      [run.id, JSON.stringify({ ...run, version: next }), run.status, run.wakeAt, run.leaseUntil, next, run.updatedAt, expectedVersion],
    );
    if (rowCount !== 1) return false;
    run.version = next;
    return true;
  }

  /**
   * SKIP LOCKED is what makes many workers safe: each one leases a disjoint
   * set of due runs in a single statement, without blocking on the others.
   */
  async claimDue(now: number, leaseMs: number, limit: number): Promise<RunRecord[]> {
    const { rows } = await this.pool.query<Row>(
      `WITH due AS (
         SELECT id FROM ${this.table}
          WHERE (status = 'running'
                 OR (status IN ('sleeping','waiting') AND wake_at IS NOT NULL AND wake_at <= $1))
            AND (lease_until IS NULL OR lease_until < $1)
          ORDER BY wake_at NULLS FIRST, created_at
          LIMIT $3
          FOR UPDATE SKIP LOCKED
       )
       UPDATE ${this.table} r
          SET lease_until = $1 + $2, version = r.version + 1, updated_at = $1
         FROM due
        WHERE r.id = due.id
       RETURNING r.data, r.status, r.wake_at, r.lease_until, r.version`,
      [now, leaseMs, limit],
    );
    return rows.map((row) => this.hydrate(row));
  }

  /**
   * Keyset pagination: the cursor is compared as a row, `(created_at, id) <
   * (cursor)`, which the (created_at DESC, id DESC) index answers directly and
   * which stays exact while new runs are being created.
   */
  async list(query: RunQuery): Promise<RunPage> {
    const limit = pageLimit(query.limit);
    const cursor = query.cursor === undefined ? null : decodeCursor(query.cursor);
    const { rows } = await this.pool.query<Row>(
      `SELECT data, status, wake_at, lease_until, version FROM ${this.table}
        WHERE ($1::text IS NULL OR workflow = $1)
          AND ($2::text IS NULL OR status = $2)
          AND ($3::bigint IS NULL OR (created_at, id) < ($3::bigint, $4::text))
        ORDER BY created_at DESC, id DESC
        LIMIT $5`,
      [query.workflow ?? null, query.status ?? null, cursor?.createdAt ?? null, cursor?.id ?? null, limit + 1],
    );
    // One extra row tells us whether a next page exists without a second query.
    const runs = rows.slice(0, limit).map((row) => this.hydrate(row));
    const last = runs[runs.length - 1];
    return { runs, cursor: rows.length > limit && last ? encodeCursor(last) : null };
  }

  private hydrate(row: Row): RunRecord {
    return {
      ...row.data,
      status: row.status,
      wakeAt: num(row.wake_at),
      leaseUntil: num(row.lease_until),
      version: row.version,
    };
  }
}
