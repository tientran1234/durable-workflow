import type { Notification, Pool, PoolClient } from "pg";
import { decodeCursor, encodeCursor, pageLimit } from "../list.js";
import { queryTags } from "../tags.js";
import type { RunPage, RunQuery, RunRecord, RunStore } from "../types.js";
import { type Wakeup, type WakeupSource, type WakeupSubscription, decodeWakeup, encodeWakeup, wakeupFor } from "../wakeups.js";

interface Row {
  data: RunRecord;
  status: RunRecord["status"];
  wake_at: string | number | null;
  lease_until: string | number | null;
  version: number;
}

const num = (v: string | number | null): number | null => (v === null ? null : Number(v));

/**
 * How long a dropped LISTEN connection is left before it is replaced. Short,
 * because the gap is a gap in wakeups and nothing more: polling moves every
 * run throughout it.
 */
export const WAKEUP_RECONNECT_MS = 1_000;

/** The channel as LISTEN must spell it, so it matches the name pg_notify is given. */
const quoted = (channel: string): string => `"${channel.replace(/"/g, '""')}"`;

/**
 * Postgres-backed store. The full record lives in a JSONB column; the columns
 * a worker queries by (status, wake_at, lease_until, version) are mirrored so
 * claimDue is one indexed statement, and a run's tags are mirrored into a
 * table beside it so a tag query — one tag or a set of them — is one indexed
 * statement too.
 *
 * It is also the one store that can tell a worker a run became due instead of
 * waiting to be asked: every write that leaves a run claimable sends a
 * NOTIFY, and `watch` is a session LISTENing for them.
 *
 * `pg` is an optional peer dependency — install it only if you use this store.
 */
export class PostgresStore implements RunStore, WakeupSource {
  /**
   * Where a run's tags are indexed: a row per (tag, run), carrying the run's
   * `created_at` so the index can hand a tag's runs over already in listing
   * order. Only `create` and `retag` write it, and each writes it with the
   * record, so the copy cannot drift from the run.
   */
  private readonly tagTable: string;
  /** Where wakeups for this table are sent. Named after it, so two tables do not share them. */
  private readonly channel: string;

  constructor(
    private readonly pool: Pool,
    private readonly table = "workflow_runs",
  ) {
    this.tagTable = `${table}_tags`;
    this.channel = `${table}_wake`;
  }

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
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ${this.tagTable} (
        tag        TEXT NOT NULL,
        run_id     TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        PRIMARY KEY (tag, run_id)
      )`);
    // The tag first, then the listing order: one seek to the tag and the rows
    // come out newest-first, so a tag query reads a page and stops. Indexing
    // the tag alone would still leave the ordering to a sort over every run
    // carrying it.
    await this.pool.query(
      `CREATE INDEX IF NOT EXISTS ${this.tagTable}_recent ON ${this.tagTable} (tag, created_at DESC, run_id DESC)`,
    );
  }

  /**
   * The record and its index rows in one statement, so they are one
   * transaction: a run that exists without its tags is a run an operator
   * cannot find. A run with no tags unnests an empty array and inserts
   * nothing, and a duplicate id still fails on the primary key below — which
   * is what engine.schedule reads as "this period already started".
   */
  async create(run: RunRecord): Promise<void> {
    await this.pool.query(
      `WITH inserted AS (
         INSERT INTO ${this.table} (id, workflow, status, wake_at, lease_until, version, data, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
         RETURNING id, created_at
       )
       INSERT INTO ${this.tagTable} (tag, run_id, created_at)
       SELECT tag, inserted.id, inserted.created_at FROM inserted, unnest($10::text[]) AS tag`,
      [
        run.id,
        run.workflow,
        run.status,
        run.wakeAt,
        run.leaseUntil,
        run.version,
        JSON.stringify(run),
        run.createdAt,
        run.updatedAt,
        run.tags ?? [],
      ],
    );
    await this.wake(run);
  }

  async get(id: string): Promise<RunRecord | null> {
    const { rows } = await this.pool.query<Row>(
      `SELECT data, status, wake_at, lease_until, version FROM ${this.table} WHERE id = $1`,
      [id],
    );
    const row = rows[0];
    return row ? this.hydrate(row) : null;
  }

  /** The tag table is not written here: only a retag moves a run's tags. See tags.ts. */
  async save(run: RunRecord, expectedVersion: number): Promise<boolean> {
    if (!(await this.updateRecord(this.pool, run, expectedVersion))) return false;
    run.version = expectedVersion + 1;
    await this.wake(run);
    return true;
  }

  /**
   * The record and its index rows together, for the reason `create` writes
   * them in one statement: a run indexed under a name it no longer carries is
   * one an operator reaches by the stale name and misses by the current one.
   *
   * A transaction on one connection rather than that single statement, because
   * this one deletes the rows before it writes them. Data-modifying CTEs all
   * read the same snapshot, so an insert of a tag the run already had would
   * collide on `(tag, run_id)` with the row the delete in the same statement
   * had removed — a retag refused for leaving a tag alone.
   *
   * No wakeup is sent: a retag touches neither the status nor the wake time,
   * so it cannot leave a run claimable that the write before it did not.
   */
  async retag(run: RunRecord, expectedVersion: number): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const written = await this.updateRecord(client, run, expectedVersion);
      if (written) {
        await client.query(`DELETE FROM ${this.tagTable} WHERE run_id = $1`, [run.id]);
        // The run's created_at, not now: see RunStore.retag.
        await client.query(
          `INSERT INTO ${this.tagTable} (tag, run_id, created_at)
           SELECT tag, $1, $2 FROM unnest($3::text[]) AS tag`,
          [run.id, run.createdAt, run.tags ?? []],
        );
      }
      await client.query(written ? "COMMIT" : "ROLLBACK");
      client.release();
      if (written) run.version = expectedVersion + 1;
      return written;
    } catch (err) {
      // The transaction is open and the session is in whatever state the
      // failure left it: give the connection up rather than hand it back.
      client.release(true);
      throw err;
    }
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
   *
   * A tag is a second statement rather than another predicate, because it is a
   * different way into the table: the tag index names the runs and their
   * order, so the query reads the page it returns. As a predicate on the
   * statement below it would instead walk runs newest-first, discarding the
   * ones without the tag, until it had collected a page — work proportional to
   * the whole table for the one lookup tags exist to make cheap.
   */
  async list(query: RunQuery): Promise<RunPage> {
    const limit = pageLimit(query.limit);
    const cursor = query.cursor === undefined ? null : decodeCursor(query.cursor);
    const [tag, ...rest] = queryTags(query.tag);
    // Every tag past the first is a predicate on a candidate the tag index has
    // already named, not a second way into the table: the set still enters
    // through one tag's range scan, so the rows arrive in listing order and the
    // query stops at LIMIT. Each extra tag is then one primary-key probe per
    // candidate row — cheaper than intersecting every tag's runs, which the
    // planner would have to collect and sort in full before it could order
    // anything, for a page it then throws most of away. The anchor is whichever
    // tag queryTags sorted first: nothing here knows which of them is the rare
    // one, and making the caller say would make them answer for the shape of
    // their own table.
    const carriesRest =
      rest.length === 0
        ? ""
        : `AND (SELECT count(*) FROM ${this.tagTable} o
                 WHERE o.run_id = t.run_id AND o.tag = ANY($7::text[])) = cardinality($7::text[])`;
    const filters = [
      query.workflow ?? null,
      query.status ?? null,
      cursor?.createdAt ?? null,
      cursor?.id ?? null,
      limit + 1,
    ];
    const { rows } =
      tag === undefined
        ? await this.pool.query<Row>(
            `SELECT r.data, r.status, r.wake_at, r.lease_until, r.version FROM ${this.table} r
              WHERE ($1::text IS NULL OR r.workflow = $1)
                AND ($2::text IS NULL OR r.status = $2)
                AND ($3::bigint IS NULL OR (r.created_at, r.id) < ($3::bigint, $4::text))
              ORDER BY r.created_at DESC, r.id DESC
              LIMIT $5`,
            filters,
          )
        : await this.pool.query<Row>(
            // The cursor is compared against the tag table's own columns, which
            // is what keeps the whole page one range scan of its index.
            `SELECT r.data, r.status, r.wake_at, r.lease_until, r.version
               FROM ${this.tagTable} t
               JOIN ${this.table} r ON r.id = t.run_id
              WHERE t.tag = $6
                AND ($1::text IS NULL OR r.workflow = $1)
                AND ($2::text IS NULL OR r.status = $2)
                AND ($3::bigint IS NULL OR (t.created_at, t.run_id) < ($3::bigint, $4::text))
                ${carriesRest}
              ORDER BY t.created_at DESC, t.run_id DESC
              LIMIT $5`,
            rest.length === 0 ? [...filters, tag] : [...filters, tag, rest],
          );
    // One extra row tells us whether a next page exists without a second query.
    const runs = rows.slice(0, limit).map((row) => this.hydrate(row));
    const last = runs[runs.length - 1];
    return { runs, cursor: rows.length > limit && last ? encodeCursor(last) : null };
  }

  /**
   * Listen for this table's wakeups on a connection of its own — a
   * notification is delivered to the session that subscribed, so this is the
   * one thing in the store that cannot be a pooled round trip.
   *
   * The first connection is awaited, so a worker starting against a database
   * it cannot LISTEN on says so. Losing it later is handled here instead: the
   * session is replaced after WAKEUP_RECONNECT_MS, and whatever was sent while
   * it was gone is simply not delivered, which is the gap polling covers.
   */
  async watch(onWake: (wake: Wakeup) => void): Promise<WakeupSubscription> {
    let current: { release: () => void } | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let closed = false;

    const lost = (): void => {
      current = null;
      if (closed) return;
      timer = setTimeout(() => {
        // Another failure lands back here, so this retries for as long as the
        // subscription is open: giving up would be a worker that polls
        // forever, which nothing would ever say.
        void this.listen(onWake, lost).then((next) => {
          if (closed) next.release();
          else current = next;
        }, lost);
      }, WAKEUP_RECONNECT_MS);
      timer.unref();
    };

    try {
      current = await this.listen(onWake, lost);
    } catch (err) {
      closed = true;
      throw err;
    }

    return {
      close: async () => {
        closed = true;
        if (timer !== null) clearTimeout(timer);
        const held = current;
        current = null;
        held?.release();
      },
    };
  }

  /**
   * Tell whoever is listening that this run is theirs to claim, if it is.
   *
   * Sent after the write it reports rather than inside it, so a process that
   * dies in between sends nothing — a wakeup lost, not a run lost. That is the
   * whole reason a worker keeps polling, and it is why this is one more round
   * trip on the writes that leave a run claimable rather than a condition
   * folded into each statement, where it would have to be read back out of the
   * row count that already answers something else.
   */
  private async wake(run: RunRecord): Promise<void> {
    const wakeup = wakeupFor(run);
    if (wakeup === null) return;
    await this.pool.query("SELECT pg_notify($1, $2)", [this.channel, encodeWakeup(wakeup)]);
  }

  /**
   * The version-guarded UPDATE of the record itself, which `save` is and
   * `retag` runs on its own connection inside a transaction. Shared so the two
   * cannot drift into writing a run's columns differently.
   */
  private async updateRecord(q: Pool | PoolClient, run: RunRecord, expectedVersion: number): Promise<boolean> {
    const version = expectedVersion + 1;
    const { rowCount } = await q.query(
      `UPDATE ${this.table}
         SET data = $2::jsonb, status = $3, wake_at = $4, lease_until = $5, version = $6, updated_at = $7
       WHERE id = $1 AND version = $8`,
      [run.id, JSON.stringify({ ...run, version }), run.status, run.wakeAt, run.leaseUntil, version, run.updatedAt, expectedVersion],
    );
    return rowCount === 1;
  }

  /** One LISTENing session, with the handlers that give it up when it breaks. */
  private async listen(onWake: (wake: Wakeup) => void, lost: () => void): Promise<{ release: () => void }> {
    const client = await this.pool.connect();
    let held = true;
    // Always given back as broken, even on a clean close: the session carries
    // a LISTEN and these handlers, and neither of them survives being handed
    // to the next caller as a fresh connection.
    const give = (): boolean => {
      if (!held) return false;
      held = false;
      client.release(true);
      return true;
    };
    const drop = (): void => {
      if (give()) lost();
    };

    client.on("error", drop);
    client.on("end", drop);
    client.on("notification", (message: Notification) => {
      if (message.channel !== this.channel) return;
      const wake = decodeWakeup(message.payload);
      if (wake !== null) onWake(wake);
    });

    try {
      await client.query(`LISTEN ${quoted(this.channel)}`);
    } catch (err) {
      give();
      throw err;
    }
    return { release: give };
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
