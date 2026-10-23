import { TERMINAL } from "./due.js";
import { pageLimit } from "./list.js";
import type { RunPage, RunQuery, RunRecord, RunStatus } from "./types.js";
import type { ChainView, RunBlockedOn, RunView, TimelineEntry } from "./view.js";

/**
 * A handler in the shape every modern server already accepts: Node's
 * `createServer` via its fetch adapters, Deno, Bun, Workers, Hono, Nitro. The
 * dashboard is one function of Request to Response so mounting it is a route
 * and not an integration.
 */
export type DashboardHandler = (request: Request) => Promise<Response>;

/**
 * What the dashboard asks of an engine: the three reads and the one write. It
 * takes this rather than an `Engine` so the screen stays testable against a
 * stand-in, and so it cannot reach past what an operator is allowed to do —
 * there is no `cancel` here, and no store.
 */
export interface DashboardEngine {
  list(query: RunQuery): Promise<RunPage>;
  view(id: string): Promise<RunView | null>;
  /**
   * The chain a run belongs to, read from any generation's id. A read of the
   * same class as `view` — it reaches no further than the records that one
   * already serves, one per generation.
   */
  viewChain(id: string): Promise<ChainView | null>;
  signal(id: string, name: string, payload?: unknown): Promise<RunRecord>;
}

export interface DashboardOptions {
  /** Runs per page. Defaults to DASHBOARD_LIMIT; capped like any other listing. */
  limit?: number;
  /** Heading, and the document title. Defaults to "durable-workflow". */
  title?: string;
  /**
   * Bind the signal form to the session the layer in front of this one holds.
   * Absent, a post is taken from wherever it came from, which is where this
   * screen started: mounted behind something that already authenticates, with
   * a rule not to route to it from the public internet.
   */
  csrf?: DashboardCsrf;
  /**
   * Let the screen end a run. Absent, it cannot: there is no button and a
   * post asking for one is refused, which is where this screen started.
   */
  cancel?: DashboardCancel;
  /**
   * Re-read the screens that only read, this often, through a
   * `<meta http-equiv="refresh">` and nothing else — no socket, no poll loop,
   * no script, and the same document a refresh has always served. Absent, a
   * run moving on is the operator pressing F5, which is where this started.
   *
   * It reaches the run list and the chain. Not the run page: that is the one
   * screen with a box an operator types a signal payload into, and a document
   * that replaces itself takes what is half-typed in it with it.
   */
  refreshSeconds?: number;
}

/**
 * The two halves of a CSRF check, supplied by whoever mounts the dashboard.
 *
 * A token has to be bound to a session, and a handler with no user model has
 * no session to bind one to — so the token is not minted here, it is asked
 * for. The layer that authenticated the request hands one over per request
 * and says, on the way back, whether the token that returned is the one that
 * session was given. All this screen owns is the plumbing: the field in the
 * form, and the refusal of a post without it.
 */
export interface DashboardCsrf {
  /**
   * The token to write into the signal form for the request being rendered.
   * Per request, because the token is the session's and the session is what
   * differs between two operators reading the same run.
   */
  token(request: Request): string | Promise<string>;
  /** Whether the token that came back with a post is the one its session was given. */
  verify(request: Request, token: string): boolean | Promise<boolean>;
}

/**
 * Ending a run, handed over by whoever mounts the dashboard.
 *
 * It is not on `DashboardEngine` with the reads and the signal, because it is
 * not the same kind of act: a signal is a message the workflow already asked
 * for and handles on a path it defines, and a cancel is an operator ending the
 * run from outside its own logic. So the screen cannot reach for it — the
 * layer that decided this operator may do that passes it in, at the call site
 * where the decision is written down and greppable.
 *
 * The result is awaited and otherwise ignored: what the operator is told is
 * the run, re-read on the page they land back on. Throwing is how this says no
 * — the message goes to them as it came.
 */
export type DashboardCancel = (id: string) => unknown;

/** The form field the token travels in, and the only name a host has to know. */
const CSRF_FIELD = "_csrf";

/**
 * Which post this is. Absent means a signal, so the form that has always been
 * on this page posts exactly the fields it always did.
 */
const ACTION_FIELD = "action";
const CANCEL_ACTION = "cancel";

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
 * It authenticates nobody: it reads every run's input and output and can
 * resume a run, so it belongs behind whatever already guards the rest of your
 * admin surface. That layer owns the session a CSRF token is bound to, which
 * is why `csrf` takes the token from it rather than minting one here — and
 * without `csrf` the screen is as open to a cross-site post as it has always
 * been.
 */
export function dashboard(engine: DashboardEngine, options: DashboardOptions = {}): DashboardHandler {
  const limit = pageLimit(options.limit ?? DASHBOARD_LIMIT);
  const title = options.title ?? "durable-workflow";
  const csrf = options.csrf;

  return async (request: Request): Promise<Response> => {
    const url = new URL(request.url);

    if (request.method === "POST") return posted(engine, request, url, { csrf, title, cancel: options.cancel });
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("method not allowed", { status: 405, headers: { allow: "GET, HEAD, POST" } });
    }

    const runId = url.searchParams.get("run");
    if (runId !== null) {
      const view = await engine.view(runId);
      if (!view) return html(notFound(url, title, "run", runId), 404);
      // Only the run page carries a form that changes anything, so it is the
      // only request a token is asked for: the filters are a GET, and the
      // chain below is a read.
      return html(runPage(view, url, title, csrf ? await csrf.token(request) : null, options.cancel !== undefined));
    }

    const chainId = url.searchParams.get("chain");
    if (chainId !== null) {
      const view = await engine.viewChain(chainId);
      if (!view) return html(notFound(url, title, "chain", chainId), 404);
      return html(chainPage(view, url, title));
    }

    return html(await listPage(engine, url, { limit, title }));
  };
}

// ---------------------------------------------------------------------------
// The two posts

/**
 * Every post this screen takes, through one door: the form is read once and
 * the token checked once, so a write added here cannot be the one that forgot
 * to ask.
 *
 * The check comes before anything is read out of the form, and is answered as
 * a page rather than the redirect an operator's own mistakes get: a post this
 * session did not make has no run page to be sent back to, and the browser
 * that made it is not the one looking.
 */
async function posted(
  engine: DashboardEngine,
  request: Request,
  url: URL,
  mount: { csrf: DashboardCsrf | undefined; title: string; cancel: DashboardCancel | undefined },
): Promise<Response> {
  const form = await request.formData();

  if (mount.csrf && !(await accepted(mount.csrf, request, String(form.get(CSRF_FIELD) ?? "")))) {
    return html(refused(url, mount.title), 403);
  }

  return String(form.get(ACTION_FIELD) ?? "") === CANCEL_ACTION
    ? end(form, url, mount.cancel)
    : send(engine, form, url);
}

/**
 * Deliver the signal form, then redirect to the run it was sent to.
 *
 * Post/redirect/get, so the operator refreshing the page they land on — which
 * is exactly what somebody watching a run they just resumed does — re-reads
 * the run instead of sending the signal a second time.
 */
async function send(engine: DashboardEngine, form: FormData, url: URL): Promise<Response> {
  const runId = String(form.get("run") ?? "");
  const name = String(form.get("signal") ?? "").trim();
  const body = String(form.get("payload") ?? "").trim();

  const back = runPageUrl(url, runId);

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

/**
 * End the run the form names, if ending one is something this screen was
 * handed. Post/redirect/get like a signal, so the operator reads the outcome
 * off the run itself rather than off this response.
 */
async function end(form: FormData, url: URL, cancel: DashboardCancel | undefined): Promise<Response> {
  const runId = String(form.get("run") ?? "");
  const back = runPageUrl(url, runId);

  if (!cancel) {
    // No page here offers this, so a post that arrives asking for it was
    // written by hand against a screen that was never given the call. Said as
    // a message on the run rather than silently: somebody is holding a form
    // that looks like it works, and the honest answer is that this mount
    // cannot do it, not that nothing happened.
    back.searchParams.set("error", "this dashboard cannot cancel runs");
    return seeOther(back);
  }
  if (!runId) {
    back.searchParams.set("error", "a run id is required");
    return seeOther(back);
  }

  try {
    await cancel(runId);
    back.searchParams.set("canceled", "1");
  } catch (err) {
    // The layer that handed the call over is also the one entitled to refuse
    // it for this operator, so its message is theirs to read.
    back.searchParams.set("error", err instanceof Error ? err.message : String(err));
  }
  return seeOther(back);
}

/** The run's own page, which is where every post lands the operator back on. */
function runPageUrl(url: URL, runId: string): URL {
  const back = new URL(url);
  back.search = "";
  back.searchParams.set("run", runId);
  return back;
}

/**
 * Whether the post may go on to the engine. Fails closed: an empty field is
 * no token at all, and a check that throws has not said yes — a session store
 * that is down must not be the thing that lets a cross-site post through.
 */
async function accepted(csrf: DashboardCsrf, request: Request, token: string): Promise<boolean> {
  if (token === "") return false;
  try {
    return await csrf.verify(request, token);
  } catch {
    return false;
  }
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

function runPage(view: RunView, url: URL, title: string, csrfToken: string | null, canCancel: boolean): string {
  const sent = url.searchParams.get("sent");
  const error = url.searchParams.get("error");
  const canceled = url.searchParams.get("canceled") !== null;

  const facts: [string, string][] = [
    ["workflow", `${text(view.workflow)} <span class="dim">v${view.workflowVersion}</span>`],
    ["status", badge(view.status)],
    ["started", text(stamp(view.createdAt))],
    ["duration", text(elapsed(view.durationMs))],
    ["blocked on", blocked(view.blockedOn)],
    ["tags", tags(view)],
  ];
  // Generations count from 1, so the work has been handed on at least once if
  // this is not the first — or, on the root, if it names a successor. Below
  // that there is no second run to read as one piece of work, and the page in
  // front of the operator is the whole of it.
  //
  // The id is the root's, as the one an operator most likely arrived holding,
  // but the link is the chain rather than the root's own record: a generation
  // is rarely the question, and the root's history is one row on from there
  // along with every other generation's.
  if (view.chain.generation > 1 || view.continuation) {
    facts.push(["chain", `${chainLink(url, view.chain.root)} <span class="dim">generation ${view.chain.generation}</span>`]);
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
    canceled ? banner("ok", "This run was canceled; nothing it had done has been undone.") : "",
    error === null ? "" : banner("error", error),
    `<dl>${facts.map(([k, v]) => `<dt>${text(k)}</dt><dd>${v}</dd>`).join("")}</dl>`,
    section("Input", json(view.input)),
    view.status === "completed" ? section("Output", json(view.output)) : "",
    rejected(view.rejectedSignals),
    formerTags(view.formerTags),
    timeline(view, rows),
    signalForm(view, url, csrfToken),
    canCancel ? cancelForm(view, url, csrfToken) : "",
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

function tags(view: RunView): string {
  const names = view.tags.length ? view.tags.map((t) => `<span class="tag">${text(t)}</span>`).join(" ") : dim("—");
  // Only once the run has been renamed: on every other run "since" would date
  // the tags to the run's own start, which the row above it already says.
  if (view.formerTags.length === 0) return names;
  return `${names} <span class="dim">since ${text(stamp(view.tagsSince))}</span>`;
}

function formerTags(former: RunView["formerTags"]): string {
  if (former.length === 0) return "";
  // The operator who needs this arrived by a name the filter above no longer
  // finds, so the point of each line is the window: the run is theirs, and
  // this is when it stopped being the one they are describing.
  const items = former
    .map((entry) => {
      const names = entry.tags.length
        ? entry.tags.map((t) => `<span class="tag">${text(t)}</span>`).join(" ")
        : dim("untagged");
      return `<li>${names} <span class="dim">${text(`${stamp(entry.from)} → ${stamp(entry.until)}`)}</span></li>`;
    })
    .join("");
  return `<h2>Former tags</h2><ul class="names">${items}</ul>`;
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

function signalForm(view: RunView, url: URL, csrfToken: string | null): string {
  if (TERMINAL.has(view.status)) {
    return `<h2>Send a signal</h2><p class="dim">This run is ${text(view.status)}; it cannot be signalled.</p>`;
  }
  const waiting = view.blockedOn?.kind === "signal" ? view.blockedOn.name : "";
  const token = csrfToken === null ? "" : hidden(CSRF_FIELD, csrfToken);
  return `<h2>Send a signal</h2>
    <form class="signal" method="post" action="${text(link(url))}">
      ${token}
      <input type="hidden" name="run" value="${text(view.id)}" />
      <label>name <input type="text" name="signal" value="${text(waiting)}" placeholder="manual-review" required /></label>
      <label>payload <textarea name="payload" rows="4" placeholder="JSON, or leave empty for null"></textarea></label>
      <button type="submit">Send</button>
    </form>`;
}

/**
 * The button, on a dashboard that was given the call and a run there is still
 * something to end.
 *
 * It says what cancelling does not do, because the only thing an operator can
 * conclude from a saga's steps in the timeline above is the opposite: a
 * canceled run executes nothing further, and the undos it owes only exist
 * inside a pass that replays its function, so this unwinds none of them. A run
 * that has to come apart cleanly is signalled, and takes that path itself.
 */
function cancelForm(view: RunView, url: URL, csrfToken: string | null): string {
  if (TERMINAL.has(view.status)) return "";
  return `<h2>Cancel this run</h2>
    <p class="dim">The run stops where it is and runs nothing further. Steps it has already
    completed stay done: compensations do not run, so a run that has to come apart cleanly
    wants a signal and the path the workflow takes on it.</p>
    <form class="cancel" method="post" action="${text(link(url))}">
      ${csrfToken === null ? "" : hidden(CSRF_FIELD, csrfToken)}
      ${hidden(ACTION_FIELD, CANCEL_ACTION)}
      ${hidden("run", view.id)}
      <button class="danger" type="submit">Cancel run</button>
    </form>`;
}

// ---------------------------------------------------------------------------
// One chain

/**
 * A continuation chain as the one piece of work it is: the chain's own facts,
 * every generation as a row into its own history, and all their events on a
 * single axis of offsets from the root.
 *
 * The fields it shares with a run answer about the chain — the root's input,
 * the outcome the last generation reported, the elapsed time across every
 * handover — because an operator who arrived holding an order id is asking
 * what happened to the work, not what happened before the first handover.
 *
 * It writes nothing. `engine.signal` walks to the generation doing the work,
 * so a form here would post exactly what that generation's own page posts —
 * and that page is one click away, named in the facts. Keeping the only write
 * on one screen keeps the CSRF token on one screen.
 */
function chainPage(view: ChainView, url: URL, title: string): string {
  const facts: [string, string][] = [
    ["workflow", text(view.workflow)],
    ["status", badge(view.status)],
    ["generations", String(view.generations)],
    // The generation `signal` and `cancel` reach, which is the run an operator
    // reading a chain has come here to act on.
    ["live", runLink(url, view.live)],
    ["started", text(stamp(view.createdAt))],
    ["duration", text(elapsed(view.durationMs))],
    ["blocked on", blocked(view.blockedOn)],
    ["tags", view.tags.length ? view.tags.map((t) => `<span class="tag">${text(t)}</span>`).join(" ") : dim("—")],
  ];
  if (view.error !== null) facts.push(["error", `<span class="failed">${text(view.error)}</span>`]);

  const generations = view.runs.map(
    (run) => `<tr>
      <td class="num">${run.chain.generation}</td>
      <td>${runLink(url, run.id)}</td>
      <td>${badge(run.status)}</td>
      <td class="num">${text(stamp(run.createdAt))}</td>
      <td class="num">${text(elapsed(run.durationMs))}</td>
    </tr>`,
  );

  const events = view.timeline.map(
    (entry) => `<tr>
      <td class="num">${entry.generation}</td>
      <td class="num">${text(elapsed(entry.elapsedMs))}</td>
      <td class="num">${entry.call}</td>
      <td><code>${text(entry.type)}</code></td>
      <td>${text(entry.summary)}</td>
    </tr>`,
  );

  return document(`${view.workflow} · ${view.root}`, [
    `<p class="pager"><a href="${text(link(url))}">← all runs</a></p>`,
    `<h1>${text(view.workflow)} <code class="dim">${text(view.root)}</code></h1>`,
    `<dl>${facts.map(([k, v]) => `<dt>${text(k)}</dt><dd>${v}</dd>`).join("")}</dl>`,
    section("Input", json(view.input)),
    view.status === "completed" ? section("Output", json(view.output)) : "",
    `<h2>Generations</h2><table>
      <thead><tr><th class="num">gen</th><th>run</th><th>status</th><th>started</th><th>duration</th></tr></thead>
      <tbody>${generations.join("")}</tbody>
    </table>`,
    chainTimeline(view, events),
    `<footer class="dim">${text(title)}</footer>`,
  ]);
}

/**
 * Every generation's events in order, each saying which one it is recorded on.
 * A handover leaves no event of its own — nothing replays it — so the only
 * thing that marks one here is the generation changing.
 */
function chainTimeline(view: ChainView, rows: string[]): string {
  // The run page's caveat, said per generation it applies to: on one axis a
  // compacted generation's missing attempts read as a quiet stretch of the
  // work rather than of a run, which is the wrong thing to conclude from them.
  const compacted = view.runs.filter((run) => run.compaction !== null);
  const which = compacted.map((run) => run.chain.generation).join(", ");
  const dropped = compacted.reduce((total, run) => total + (run.compaction?.droppedEvents ?? 0), 0);
  const caveat =
    compacted.length === 0
      ? ""
      : `<p class="dim">History was compacted on ${compacted.length === 1 ? "generation" : "generations"}
         ${text(which)}: ${dropped} superseded attempt(s) are no longer on record.</p>`;
  const body =
    rows.length === 0
      ? `<p class="dim">Nothing has happened yet.</p>`
      : `<table>
          <thead><tr><th class="num">gen</th><th>at</th><th>call</th><th>type</th><th>what happened</th></tr></thead>
          <tbody>${rows.join("")}</tbody>
        </table>`;
  return `<h2>Timeline</h2>${caveat}${body}`;
}

// ---------------------------------------------------------------------------
// HTML

/**
 * A post the mounting layer did not vouch for. It names no run and offers no
 * form: whatever made this request, the page it should be reading is the one
 * it reached this screen through.
 */
function refused(url: URL, title: string): string {
  return document(title, [
    `<h1>Refused</h1>`,
    `<p>That form did not come from this session, or it has been open long enough to expire.</p>`,
    `<p class="pager"><a href="${text(link(url))}">← all runs</a></p>`,
  ]);
}

function notFound(url: URL, title: string, what: "run" | "chain", id: string): string {
  return document(title, [
    `<p class="pager"><a href="${text(link(url))}">← all runs</a></p>`,
    `<h1>No such ${text(what)}</h1>`,
    `<p>Nothing is stored under <code>${text(id)}</code>.</p>`,
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

/** A field the browser carries back untouched, escaped like any other value. */
function hidden(name: string, value: string): string {
  return `<input type="hidden" name="${text(name)}" value="${text(value)}" />`;
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

/** The chain a run belongs to, named by its root — which is how `?chain=` reads it. */
function chainLink(url: URL, rootId: string): string {
  return `<a href="${text(`${link(url)}?chain=${encodeURIComponent(rootId)}`)}"><code>${text(rootId)}</code></a>`;
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
button.danger { background: #dc2626; border-color: #dc2626; }
.cancel { margin: 0 0 1.5rem; }
.banner { padding: .5rem .75rem; border-radius: 6px; border: 1px solid var(--line); }
.banner.ok { border-color: #15803d55; color: #15803d; }
.banner.error { border-color: #dc262655; color: #dc2626; }
.rejected, .names { padding-left: 1.2rem; }
.pager { margin: 1rem 0 0; }
footer { margin-top: 3rem; font-size: .8rem; }
`;
