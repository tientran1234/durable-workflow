import { pageLimit } from "./list.js";
import type { RunPage, RunQuery, RunRecord, RunStatus } from "./types.js";
import type { RunBlockedOn, RunView, TimelineEntry } from "./view.js";

/**
 * A handler in the shape every modern server already accepts: Node's
 * `createServer` via its fetch adapters, Deno, Bun, Workers, Hono, Nitro. The
 * dashboard is one function of Request to Response so mounting it is a route
 * and not an integration.
 */
export type DashboardHandler = (request: Request) => Promise<Response>;

/**
 * What the dashboard asks of an engine: the two reads and the one write. It
 * takes this rather than an `Engine` so the screen stays testable against a
 * stand-in, and so it cannot reach past what an operator is allowed to do —
 * there is no `cancel` here, and no store.
 */
export interface DashboardEngine {
  list(query: RunQuery): Promise<RunPage>;
  view(id: string): Promise<RunView | null>;
  signal(id: string, name: string, payload?: unknown): Promise<RunRecord>;
}

export interface DashboardOptions {
  /** Runs per page. Defaults to DASHBOARD_LIMIT; capped like any other listing. */
  limit?: number;
  /** Heading, and the document title. Defaults to "durable-workflow". */
  title?: string;
}

/** A page small enough to read on one screen, which is what this is for. */
export const DASHBOARD_LIMIT = 25;

const STATUSES: RunStatus[] = ["running", "sleeping", "waiting", "completed", "failed", "canceled", "continued"];

/**
 * An admin screen over `list` and `view`, served as one self-contained HTML
 * document per request: inline styles, no scripts, no assets to host, and
 * every action a link or a form. There is nothing to build and nothing to
 * serve beside it, which is the whole point — a dashboard you have to deploy
 * is a dashboard nobody has during the incident.
 *
 * Routing is entirely in the query string: `?run=<id>` is one run, no query is
 * the list, a POST sends a signal. So the handler never has to be told where
 * it was mounted, and the links it writes are relative to whatever path the
 * request came in on — `/admin`, `/_internal/workflows`, the root.
 *
 * It is unauthenticated and unprotected against cross-site posts by design:
 * it reads every run's input and output and can resume a run, so it belongs
 * behind whatever already guards the rest of your admin surface, and that is
 * the layer that owns the session a CSRF token would be bound to.
 */
export function dashboard(engine: DashboardEngine, options: DashboardOptions = {}): DashboardHandler {
  const limit = pageLimit(options.limit ?? DASHBOARD_LIMIT);
  const title = options.title ?? "durable-workflow";

  return async (request: Request): Promise<Response> => {
    const url = new URL(request.url);

    if (request.method === "POST") return send(engine, request, url);
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("method not allowed", { status: 405, headers: { allow: "GET, HEAD, POST" } });
    }

    const runId = url.searchParams.get("run");
    if (runId === null) return html(await listPage(engine, url, { limit, title }));

    const view = await engine.view(runId);
    if (!view) return html(notFound(runId, url, title), 404);
    return html(runPage(view, url, title));
  };
}

// ---------------------------------------------------------------------------
// Sending a signal

/**
 * Deliver the signal form, then redirect to the run it was sent to.
 *
 * Post/redirect/get, so the operator refreshing the page they land on — which
 * is exactly what somebody watching a run they just resumed does — re-reads
 * the run instead of sending the signal a second time.
 */
async function send(engine: DashboardEngine, request: Request, url: URL): Promise<Response> {
  const form = await request.formData();
  const runId = String(form.get("run") ?? "");
  const name = String(form.get("signal") ?? "").trim();
  const body = String(form.get("payload") ?? "").trim();

  const back = new URL(url);
  back.search = "";
  back.searchParams.set("run", runId);

  if (!runId || !name) {
    back.searchParams.set("error", "a run id and a signal name are required");
    return seeOther(back);
  }

  let payload: unknown;
  try {
    // An empty box is the untyped signal whose arrival is the whole message.
    // Anything else is read as JSON and reported when it is not: a schema is
    // waiting on a shape, and quietly sending `{"approved":true}` as the
    // string an operator typed would be refused somewhere they cannot see.
    payload = body === "" ? null : JSON.parse(body);
  } catch {
    back.searchParams.set("error", `payload is not valid JSON: ${body}`);
    return seeOther(back);
  }

  try {
    await engine.signal(runId, name, payload);
    back.searchParams.set("sent", name);
  } catch (err) {
    back.searchParams.set("error", err instanceof Error ? err.message : String(err));
  }
  return seeOther(back);
}

// ---------------------------------------------------------------------------
// The list

async function listPage(engine: DashboardEngine, url: URL, chrome: { limit: number; title: string }): Promise<string> {
  const workflow = url.searchParams.get("workflow")?.trim() ?? "";
  const status = url.searchParams.get("status")?.trim() ?? "";
  const tag = url.searchParams.get("tag")?.trim() ?? "";
  const cursor = url.searchParams.get("cursor") ?? "";

  const query: RunQuery = { limit: chrome.limit };
  if (workflow) query.workflow = workflow;
  if (isStatus(status)) query.status = status;
  if (tag) query.tag = tag;
  if (cursor) query.cursor = cursor;

  let page: RunPage;
  try {
    page = await engine.list(query);
  } catch (err) {
    // A cursor is opaque and arrives in a URL somebody may have edited or
    // kept, so a listing that cannot be answered is a message on the filter
    // form rather than a 500 over the whole screen.
    return document(chrome.title, [
      heading(chrome.title, url),
      filters(url, { workflow, status, tag }),
      banner("error", err instanceof Error ? err.message : String(err)),
    ]);
  }

  const rows = page.runs.map((run) => {
    const href = `${link(url)}?run=${encodeURIComponent(run.id)}`;
    return `<tr>
      <td><a href="${text(href)}"><code>${text(run.id)}</code></a></td>
      <td>${text(run.workflow)} <span class="dim">v${run.workflowVersion ?? 1}</span></td>
      <td>${badge(run.status)}</td>
      <td class="num">${text(stamp(run.createdAt))}</td>
      <td class="num">${text(elapsed(run.updatedAt - run.createdAt))}</td>
      <td>${run.tags?.length ? run.tags.map((t) => `<span class="tag">${text(t)}</span>`).join(" ") : "<span class=\"dim\">—</span>"}</td>
    </tr>`;
  });

  const next = page.cursor === null ? "" : nextPageLink(url, page.cursor);

  return document(chrome.title, [
    heading(chrome.title, url),
    filters(url, { workflow, status, tag }),
    rows.length === 0
      ? `<p class="dim">No runs match.</p>`
      : `<table>
          <thead><tr><th>run</th><th>workflow</th><th>status</th><th>started</th><th>duration</th><th>tags</th></tr></thead>
          <tbody>${rows.join("")}</tbody>
        </table>${next}`,
  ]);
}

function filters(url: URL, current: { workflow: string; status: string; tag: string }): string {
  const options = ["", ...STATUSES]
    .map((s) => `<option value="${text(s)}"${s === current.status ? " selected" : ""}>${text(s || "any status")}</option>`)
    .join("");
  // GET, and no hidden cursor: changing a filter starts the paging over,
  // because a cursor names a row in the order the old query returned.
  return `<form class="filters" method="get" action="${text(link(url))}">
    <input type="text" name="workflow" value="${text(current.workflow)}" placeholder="workflow" />
    <select name="status">${options}</select>
    <input type="text" name="tag" value="${text(current.tag)}" placeholder="tag" />
    <button type="submit">Filter</button>
  </form>`;
}

function nextPageLink(url: URL, cursor: string): string {
  const next = new URL(url);
  next.searchParams.set("cursor", cursor);
  return `<p class="pager"><a href="${text(link(next) + next.search)}">Next page →</a></p>`;
}

// ---------------------------------------------------------------------------
// One run

function runPage(view: RunView, url: URL, title: string): string {
  const sent = url.searchParams.get("sent");
  const error = url.searchParams.get("error");

  const facts: [string, string][] = [
    ["workflow", `${text(view.workflow)} <span class="dim">v${view.workflowVersion}</span>`],
    ["status", badge(view.status)],
    ["started", text(stamp(view.createdAt))],
    ["duration", text(elapsed(view.durationMs))],
    ["blocked on", blocked(view.blockedOn)],
    ["tags", view.tags.length ? view.tags.map((t) => `<span class="tag">${text(t)}</span>`).join(" ") : dim("—")],
  ];
  // Generations count from 1, so a chain is only worth a row once the work has
  // been handed on at least once — and then the root is the id an operator
  // most likely arrived holding.
  if (view.chain.generation > 1) {
    facts.push(["chain", `${runLink(url, view.chain.root)} <span class="dim">generation ${view.chain.generation}</span>`]);
  }
  if (view.continuation) facts.push(["continued as", runLink(url, view.continuation.runId)]);
  if (view.error !== null) facts.push(["error", `<span class="failed">${text(view.error)}</span>`]);

  const pending = Object.entries(view.pendingSignals);
  if (pending.length > 0) {
    facts.push(["buffered signals", pending.map(([n, count]) => `${text(n)} <span class="dim">×${count}</span>`).join(", ")]);
  }

  const rows = view.timeline.map(
    (entry) => `<tr>
      <td class="num">${entry.seq}</td>
      <td class="num">${text(elapsed(entry.elapsedMs))}</td>
      <td class="num">${entry.call}</td>
      <td><code>${text(entry.type)}</code></td>
      <td>${text(entry.summary)}</td>
    </tr>`,
  );

  return document(`${view.workflow} · ${view.id}`, [
    `<p class="pager"><a href="${text(link(url))}">← all runs</a></p>`,
    `<h1>${text(view.workflow)} <code class="dim">${text(view.id)}</code></h1>`,
    sent === null ? "" : banner("ok", `signal "${sent}" sent`),
    error === null ? "" : banner("error", error),
    `<dl>${facts.map(([k, v]) => `<dt>${text(k)}</dt><dd>${v}</dd>`).join("")}</dl>`,
    section("Input", json(view.input)),
    view.status === "completed" ? section("Output", json(view.output)) : "",
    rejected(view.rejectedSignals),
    timeline(view, rows),
    signalForm(view, url),
    `<footer class="dim">${text(title)}</footer>`,
  ]);
}

function timeline(view: RunView, rows: string[]): string {
  // The caveat belongs with the timeline, not in the facts above it: a
  // compacted run's early retries are the entries an operator would otherwise
  // go looking for and conclude never happened.
  const caveat = view.compaction
    ? `<p class="dim">History was compacted at ${text(stamp(view.compaction.at))}: ${view.compaction.droppedEvents}
       superseded attempt(s) within the first ${view.compaction.calls} calls are no longer on record.</p>`
    : "";
  const body =
    rows.length === 0
      ? `<p class="dim">Nothing has happened yet.</p>`
      : `<table>
          <thead><tr><th>seq</th><th>at</th><th>call</th><th>type</th><th>what happened</th></tr></thead>
          <tbody>${rows.join("")}</tbody>
        </table>`;
  return `<h2>Timeline</h2>${caveat}${body}`;
}

function rejected(signals: RunView["rejectedSignals"]): string {
  if (signals.length === 0) return "";
  // A refused payload never reached history, so the timeline cannot show it.
  // This is the only place a run that looks stuck on a signal somebody insists
  // they sent explains itself.
  const items = signals
    .map((s) => `<li><code>${text(s.name)}</code> at ${text(stamp(s.at))}: ${text(s.error)}</li>`)
    .join("");
  return `<h2>Rejected signals</h2><ul class="rejected">${items}</ul>`;
}

function signalForm(view: RunView, url: URL): string {
  if (["completed", "failed", "canceled", "continued"].includes(view.status)) {
    return `<h2>Send a signal</h2><p class="dim">This run is ${text(view.status)}; it cannot be signalled.</p>`;
  }
  const waiting = view.blockedOn?.kind === "signal" ? view.blockedOn.name : "";
  return `<h2>Send a signal</h2>
    <form class="signal" method="post" action="${text(link(url))}">
      <input type="hidden" name="run" value="${text(view.id)}" />
      <label>name <input type="text" name="signal" value="${text(waiting)}" placeholder="manual-review" required /></label>
      <label>payload <textarea name="payload" rows="4" placeholder="JSON, or leave empty for null"></textarea></label>
      <button type="submit">Send</button>
    </form>`;
}

// ---------------------------------------------------------------------------
// HTML

function notFound(runId: string, url: URL, title: string): string {
  return document(title, [
    `<p class="pager"><a href="${text(link(url))}">← all runs</a></p>`,
    `<h1>No such run</h1>`,
    `<p>Nothing is stored under <code>${text(runId)}</code>.</p>`,
  ]);
}

/**
 * Everything the browser gets: one document, styles included. `no-store`
 * because every screen here is live state — a run an operator is watching is
 * the one thing a cached page must not be.
 */
function html(body: string, status = 200): Response {
  return new Response(body, {
    status,
    headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
  });
}

function seeOther(location: URL): Response {
  return new Response(null, { status: 303, headers: { location: link(location) + location.search } });
}

/** The path the request arrived on, which is where this handler is mounted. */
function link(url: URL): string {
  return url.pathname;
}

function document(title: string, parts: string[]): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>${text(title)}</title>
<style>${STYLE}</style>
</head>
<body>
<main>
${parts.filter((p) => p !== "").join("\n")}
</main>
</body>
</html>
`;
}

function heading(title: string, url: URL): string {
  return `<h1><a href="${text(link(url))}">${text(title)}</a></h1>`;
}

function section(label: string, body: string): string {
  return `<h2>${text(label)}</h2>${body}`;
}

function banner(kind: "ok" | "error", message: string): string {
  return `<p class="banner ${kind}">${text(message)}</p>`;
}

function blocked(on: RunBlockedOn | null): string {
  if (!on) return dim("—");
  const until = on.until === null ? "no deadline" : `until ${stamp(on.until)}`;
  return `${text(on.kind)} <code>${text(on.name)}</code> <span class="dim">${text(until)}</span>`;
}

function runLink(url: URL, runId: string): string {
  return `<a href="${text(`${link(url)}?run=${encodeURIComponent(runId)}`)}"><code>${text(runId)}</code></a>`;
}

function json(value: unknown): string {
  if (value === undefined) return dim("undefined");
  return `<pre>${text(JSON.stringify(value, null, 2) ?? "undefined")}</pre>`;
}

function badge(status: RunStatus): string {
  return `<span class="badge ${status}">${text(status)}</span>`;
}

function dim(value: string): string {
  return `<span class="dim">${text(value)}</span>`;
}

function stamp(at: number): string {
  return new Date(at).toISOString().replace("T", " ").replace(".000Z", "Z");
}

/** Offsets, not epochs: a timeline is read by how long after the start it was. */
function elapsed(ms: number): string {
  if (ms < 1_000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1_000)}s`;
  const hours = Math.floor(ms / 3_600_000);
  return hours < 48 ? `${hours}h ${Math.round((ms % 3_600_000) / 60_000)}m` : `${Math.floor(hours / 24)}d`;
}

function isStatus(value: string): value is RunStatus {
  return (STATUSES as string[]).includes(value);
}

const ENTITIES: Record<string, string> = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
};

/**
 * Everything written into this page — text node or attribute value — goes
 * through here.
 *
 * A run's id, tags, input, output and errors are application data and a signal
 * payload came off the wire, so this is a screen that renders untrusted text by
 * definition, to an operator who can also resume runs. Quotes are escaped along
 * with the markup characters, which is what makes one escaper enough for
 * attribute values too — every one of them is written inside double quotes.
 */
function text(value: string): string {
  return value.replace(/[&<>"']/g, (c) => ENTITIES[c] ?? c);
}

const STYLE = `
:root { color-scheme: light dark; --fg: #1a1a1a; --dim: #6b7280; --line: #e5e7eb; --bg: #fff; --accent: #1d4ed8; }
@media (prefers-color-scheme: dark) {
  :root { --fg: #e8e8e8; --dim: #9ca3af; --line: #30363d; --bg: #0d1117; --accent: #7aa2f7; }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--fg);
  font: 14px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width: 68rem; margin: 0 auto; padding: 2rem 1rem 4rem; }
h1 { font-size: 1.35rem; margin: 0 0 1rem; font-weight: 600; }
h2 { font-size: 1rem; margin: 2rem 0 .5rem; font-weight: 600; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .85em; }
pre { background: color-mix(in srgb, var(--fg) 5%, transparent); border: 1px solid var(--line);
  border-radius: 6px; padding: .6rem .8rem; overflow-x: auto; margin: 0; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: .45rem .6rem; border-bottom: 1px solid var(--line); vertical-align: top; }
th { font-size: .75rem; text-transform: uppercase; letter-spacing: .04em; color: var(--dim); font-weight: 600; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
dl { display: grid; grid-template-columns: max-content 1fr; gap: .35rem 1rem; margin: 0; }
dt { color: var(--dim); }
dd { margin: 0; }
.dim { color: var(--dim); }
.failed { color: #dc2626; }
.badge { display: inline-block; padding: .05rem .45rem; border-radius: 999px; font-size: .75rem;
  border: 1px solid var(--line); background: color-mix(in srgb, var(--fg) 6%, transparent); }
.badge.running, .badge.completed { color: #15803d; border-color: #15803d55; }
.badge.failed { color: #dc2626; border-color: #dc262655; }
.badge.waiting, .badge.sleeping { color: #b45309; border-color: #b4530955; }
.tag { display: inline-block; padding: .05rem .4rem; border-radius: 4px; font-size: .75rem;
  border: 1px solid var(--line); }
.filters, .signal { display: flex; flex-wrap: wrap; gap: .5rem; align-items: flex-end; margin: 0 0 1.5rem; }
.signal { flex-direction: column; align-items: stretch; max-width: 32rem; }
label { display: flex; flex-direction: column; gap: .25rem; color: var(--dim); font-size: .8rem; }
input, select, textarea, button { font: inherit; padding: .35rem .5rem; border-radius: 6px;
  border: 1px solid var(--line); background: var(--bg); color: var(--fg); }
textarea { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; resize: vertical; }
button { cursor: pointer; background: var(--accent); border-color: var(--accent); color: #fff; align-self: flex-start; }
.banner { padding: .5rem .75rem; border-radius: 6px; border: 1px solid var(--line); }
.banner.ok { border-color: #15803d55; color: #15803d; }
.banner.error { border-color: #dc262655; color: #dc2626; }
.rejected { padding-left: 1.2rem; }
.pager { margin: 1rem 0 0; }
footer { margin-top: 3rem; font-size: .8rem; }
`;
