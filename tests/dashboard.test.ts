import { describe, expect, it } from "vitest";
import { type DashboardCsrf, type DashboardEngine, type RunQuery, dashboard, defineSignal, defineWorkflow } from "../src/index.js";
import { harness } from "./helpers.js";

const fulfilment = defineWorkflow<{ order: string }, string>("fulfilment", async (ctx) => {
  await ctx.step("charge", () => "rcpt_1");
  await ctx.sleep("cool-off", 1_000);
  const review = await ctx.waitFor<{ approved: boolean }>("review");
  return review.approved ? "shipped" : "held";
});

/** The dashboard is mounted somewhere; nothing here may depend on where. */
const MOUNT = "https://admin.internal/ops/workflows";

function get(path = ""): Request {
  return new Request(MOUNT + path);
}

function post(fields: Record<string, string>): Request {
  const body = new FormData();
  for (const [k, v] of Object.entries(fields)) body.set(k, v);
  return new Request(MOUNT, { method: "POST", body });
}

describe("dashboard", () => {
  it("lists runs as links to their own page", async () => {
    const { engine } = harness([fulfilment]);
    await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1", tags: ["order:ord_1"] });
    await engine.start(fulfilment, { order: "ord_2" }, { id: "run-2" });
    const handler = engine.dashboard();

    const response = await handler(get());
    const body = await response.text();

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/html; charset=utf-8");
    // A run an operator is watching is the one page a cache must not serve.
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(body.startsWith("<!doctype html>")).toBe(true);
    expect(body).toContain("fulfilment");
    expect(body).toContain("order:ord_1");
    // Links are relative to the path the request arrived on, so the handler
    // works mounted anywhere without being told where.
    expect(body).toContain('href="/ops/workflows?run=run-1"');
    expect(body).toContain('href="/ops/workflows?run=run-2"');
    expect(body).not.toContain("http://");
  });

  it("is self-contained: no scripts, no requests out", async () => {
    const { engine } = harness([fulfilment]);
    await engine.start(fulfilment, { order: "ord_1" });
    const body = await (await engine.dashboard()(get())).text();

    expect(body).toContain("<style>");
    expect(body).not.toContain("<script");
    expect(body).not.toContain("<link");
    expect(body).not.toContain("<img");
  });

  it("passes the filter form's fields to engine.list, and ignores a status it does not know", async () => {
    const { engine } = harness([fulfilment]);
    const queries: RunQuery[] = [];
    // Through the DashboardEngine seam the screen is documented against, which
    // is also the cheapest way to see what it asked the store for.
    const recording: DashboardEngine = {
      list: (query) => (queries.push(query), engine.list(query)),
      view: (id) => engine.view(id),
      viewChain: (id) => engine.viewChain(id),
      signal: (id, name, payload) => engine.signal(id, name, payload),
    };
    const handler = dashboard(recording);

    await handler(get("?workflow=fulfilment&status=waiting&tag=order:ord_1"));
    await handler(get("?status=not-a-status&workflow="));

    expect(queries[0]).toEqual({ limit: 25, workflow: "fulfilment", status: "waiting", tag: "order:ord_1" });
    expect(queries[1]).toEqual({ limit: 25 });
  });

  it("offers the next page, carrying the cursor and the filters", async () => {
    const { engine } = harness([fulfilment]);
    for (let i = 0; i < 3; i++) await engine.start(fulfilment, { order: `ord_${i}` }, { id: `run-${i}` });
    const handler = engine.dashboard({ limit: 2 });

    const first = await (await handler(get("?workflow=fulfilment"))).text();
    // Followed the way a browser follows it: out of the escaped attribute.
    const href = /href="([^"]*cursor=[^"]*)"/.exec(first)?.[1]?.replaceAll("&amp;", "&");
    expect(href).toBeDefined();
    expect(href).toContain("workflow=fulfilment");

    const second = await (await handler(get(href!.slice("/ops/workflows".length)))).text();
    expect(first).toContain("run-2"); // newest first
    expect(first).not.toContain("?run=run-0");
    expect(second).toContain("?run=run-0");
  });

  it("reports a cursor it cannot read instead of failing the whole screen", async () => {
    const { engine } = harness([fulfilment]);
    const response = await engine.dashboard()(get("?cursor=not-a-cursor"));

    expect(response.status).toBe(200);
    expect(await response.text()).toContain("invalid cursor");
  });

  it("renders the run's timeline and what it is blocked on", async () => {
    const { engine, advance } = harness([fulfilment]);
    const id = await engine.start(fulfilment, { order: "ord_42" }, { id: "run-1" });
    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);

    const body = await (await engine.dashboard()(get("?run=run-1"))).text();

    expect(body).toContain('step &quot;charge&quot; completed');
    expect(body).toContain('timer &quot;cool-off&quot; fired');
    expect(body).toContain("1.0s"); // the timer fired a second in, shown as an offset
    expect(body).toContain("<code>review</code>"); // blocked on that signal
    expect(body).toContain("ord_42");
  });

  it("names the compaction that dropped attempts the timeline would have shown", async () => {
    // Compaction folds at the start of a pass, so the run needs a second one.
    const wf = defineWorkflow<null, number>("long", async (ctx) => {
      let total = 0;
      for (let i = 0; i < 6; i++) total += await ctx.step(`s${i}`, () => i);
      await ctx.sleep("pause", 1_000);
      return total;
    });
    const { engine, advance } = harness([wf], { compactAfter: 3 });
    const id = await engine.start(wf, null, { id: "run-1" });
    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);

    const body = await (await engine.dashboard()(get("?run=run-1"))).text();
    expect(body).toContain("History was compacted at");
  });

  it("sends the form's signal and redirects back to the run", async () => {
    const { engine } = harness([fulfilment]);
    const id = await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1" });
    await engine.settle(id); // asleep on cool-off, so the signal is buffered
    const handler = engine.dashboard();

    const response = await handler(post({ run: "run-1", signal: "review", payload: '{"approved":true}' }));

    expect(response.status).toBe(303);
    // Post/redirect/get: refreshing the page an operator lands on re-reads the
    // run rather than sending the signal a second time.
    expect(response.headers.get("location")).toBe("/ops/workflows?run=run-1&sent=review");
    expect((await engine.view(id))?.pendingSignals).toEqual({ review: 1 });
  });

  it("resumes a waiting run, which is the point of the form", async () => {
    const { engine, advance } = harness([fulfilment]);
    const id = await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1" });
    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);
    expect((await engine.view(id))?.status).toBe("waiting");

    await engine.dashboard()(post({ run: "run-1", signal: "review", payload: '{"approved":true}' }));

    const view = await engine.view(id);
    expect(view?.status).toBe("completed");
    expect(view?.output).toBe("shipped");
  });

  it("reads an empty payload box as null, and refuses one that is not JSON", async () => {
    const wf = defineWorkflow<null, unknown>("inbox", async (ctx) => ctx.waitFor("mail"));
    const { engine } = harness([wf]);
    const id = await engine.start(wf, null, { id: "run-1" });
    await engine.settle(id);
    const handler = engine.dashboard();

    const bad = await handler(post({ run: "run-1", signal: "mail", payload: "approved" }));
    expect(bad.headers.get("location")).toContain("error=payload+is+not+valid+JSON");
    expect((await engine.view(id))?.status).toBe("waiting"); // nothing was delivered

    await handler(post({ run: "run-1", signal: "mail", payload: "  " }));
    expect((await engine.view(id))?.output).toBeNull();
  });

  it("hands a schema's refusal back to the operator who typed it", async () => {
    const review = defineSignal("review", {
      parse: (payload: unknown) => {
        const value = payload as { approved?: unknown };
        if (typeof value?.approved !== "boolean") throw new Error("approved: expected boolean");
        return { approved: value.approved };
      },
    });
    const { engine, advance } = harness([fulfilment], { signals: [review] });
    const id = await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1" });
    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);
    const handler = engine.dashboard();

    const response = await handler(post({ run: "run-1", signal: "review", payload: '{"approved":"yes"}' }));
    expect(response.headers.get("location")).toContain("error=");
    expect((await engine.view(id))?.status).toBe("waiting");

    // A refused payload never reached history, so the timeline cannot show it;
    // the run page is where it is on record.
    const body = await (await handler(get(`?run=run-1&error=${encodeURIComponent("approved: expected boolean")}`))).text();
    expect(body).toContain("Rejected signals");
    expect(body).toContain("approved: expected boolean");
  });

  it("shows the names a run has stopped answering to, and since when", async () => {
    const { engine, advance } = harness([fulfilment]);
    await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1", tags: ["order:ord_1"] });
    advance(2_000);
    await engine.retag("run-1", ["order:ord_4182"]);
    const handler = engine.dashboard();

    const body = await (await handler(get("?run=run-1"))).text();

    // The operator got here by a name the filter above no longer finds, so the
    // page has to say both that this is the run and when it stopped being the
    // one they are describing.
    expect(body).toContain("Former tags");
    expect(body).toContain("order:ord_1");
    expect(body).toContain("order:ord_4182");
    expect(body).toContain("since 2027-01-15 08:00:02Z");
    expect(body).toContain("2027-01-15 08:00:00Z → 2027-01-15 08:00:02Z");
  });

  it("leaves the former tags off a run nobody has renamed", async () => {
    const { engine } = harness([fulfilment]);
    await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1", tags: ["order:ord_1"] });
    const handler = engine.dashboard();

    const body = await (await handler(get("?run=run-1"))).text();

    // Nothing to account for, so no section and no "since": the started row
    // already dates the names a run has carried all along.
    expect(body).toContain("order:ord_1");
    expect(body).not.toContain("Former tags");
    expect(body).not.toContain("since 2027-01-15");
  });

  it("will not offer to signal a run that has ended", async () => {
    const { engine, advance } = harness([fulfilment]);
    const id = await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1" });
    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);
    await engine.signal(id, "review", { approved: false });
    const handler = engine.dashboard();

    const body = await (await handler(get("?run=run-1"))).text();
    expect(body).not.toContain("<form class=\"signal\"");
    expect(body).toContain("it cannot be signalled");

    const response = await handler(post({ run: "run-1", signal: "review", payload: "null" }));
    expect(response.headers.get("location")).toContain("error=");
  });

  it("requires a run and a signal name before it touches the engine", async () => {
    const { engine } = harness([fulfilment]);
    await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1" });
    const handler = engine.dashboard();

    for (const fields of [{ run: "run-1", signal: "   " }, { run: "", signal: "review" }]) {
      const response = await handler(post(fields));
      expect(response.status).toBe(303);
      expect(response.headers.get("location")).toContain("a+run+id+and+a+signal+name+are+required");
    }
  });

  /**
   * The chain screen: one piece of work, however many runs it has been. Every
   * test here goes through a chain of three generations, which is the shape
   * that tells a chain apart from the run an operator happened to open.
   */
  describe("a chain", () => {
    /** Three generations with a leg apiece, ending on a wait. */
    const relay = defineWorkflow<{ left: number }, string>("relayed", async (ctx, input) => {
      await ctx.step(`leg-${input.left}`, () => input.left);
      if (input.left > 1) {
        await ctx.sleep("breathe", 1_000);
        return ctx.continueAsNew({ left: input.left - 1 });
      }
      return ctx.waitFor<string>("baton");
    });

    async function chained() {
      const h = harness([relay]);
      await h.engine.start(relay, { left: 3 }, { id: "r1", tags: ["order:ord_7"] });
      for (let i = 0; i < 2; i++) {
        await h.drain();
        h.advance(1_000);
      }
      await h.drain();
      return h;
    }

    it("renders every generation, and all their events on one axis", async () => {
      const { engine } = await chained();

      const body = await (await engine.dashboard()(get("?chain=r1"))).text();

      expect(body).toContain("Generations");
      // Each generation is a row of its own, linking to the record whose
      // history it is: the chain screen is a way into them, not a replacement.
      expect(body).toContain('href="/ops/workflows?run=r1"');
      expect(body).toContain('href="/ops/workflows?run=r1~2"');
      expect(body).toContain('href="/ops/workflows?run=r1~3"');
      // Every generation's events, which no single run's page can show.
      for (const leg of ["leg-3", "leg-2", "leg-1"]) {
        expect(body).toContain(`step &quot;${leg}&quot; completed`);
      }
      // Offsets from the root, so the axis reads as the age of the work rather
      // than restarting at zero on each handover.
      expect(body).toContain("2.0s");
    });

    it("answers about the work rather than the generation doing it", async () => {
      const { engine } = await chained();

      const body = await (await engine.dashboard()(get("?chain=r1"))).text();

      expect(body).toContain("<dt>generations</dt><dd>3</dd>");
      // The chain was asked to relay three legs; the live generation was asked
      // for one. The first is the question this page answers.
      expect(body).toContain("&quot;left&quot;: 3");
      expect(body).not.toContain("&quot;left&quot;: 1");
      expect(body).toContain("<code>baton</code>"); // what the work is blocked on
      expect(body).toContain("order:ord_7");
      // The generation a signal reaches, named so the one write is a click away.
      expect(body).toContain('<dt>live</dt><dd><a href="/ops/workflows?run=r1~3"');
    });

    it("is reached from every generation's own page, the root included", async () => {
      const { engine } = await chained();
      const handler = engine.dashboard();

      for (const id of ["r1", "r1~2", "r1~3"]) {
        const body = await (await handler(get(`?run=${encodeURIComponent(id)}`))).text();
        // The id is the chain's, not the page's: an operator six generations
        // back reads the same chain as one holding the live id.
        expect(body).toContain('href="/ops/workflows?chain=r1"');
      }
    });

    it("leaves the chain off a run the work has never been handed on from", async () => {
      const { engine, drain } = harness([fulfilment]);
      await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1" });
      await drain();

      const body = await (await engine.dashboard()(get("?run=run-1"))).text();

      // Nothing has been handed on, so there is no second piece of work to
      // read as one — the page in front of the operator is the whole of it.
      expect(body).not.toContain("?chain=");
      expect(body).not.toContain("<dt>chain</dt>");
    });

    it("writes nothing: the signal form stays on the run that answers for it", async () => {
      const { engine } = await chained();

      const body = await (await engine.dashboard({ csrf: { token: () => "tok", verify: () => true } })(
        get("?chain=r1"),
      )).text();

      // `signal` walks to the live generation either way, so a form here would
      // post what that generation's page posts — and the token would be on two
      // screens instead of one.
      expect(body).not.toContain("<form");
      expect(body).not.toContain("_csrf");
    });

    it("404s a chain that is not there", async () => {
      const { engine } = harness([relay]);

      const response = await engine.dashboard()(get("?chain=no-such-run"));

      expect(response.status).toBe(404);
      expect(await response.text()).toContain("No such chain");
    });
  });

  it("escapes everything it renders, wherever the text came from", async () => {
    const payload = '"><script>alert(1)</script>';
    const wf = defineWorkflow<{ note: string }, unknown>("xss", async (ctx) => ctx.waitFor("mail"));
    const { engine } = harness([wf]);
    const id = await engine.start(
      wf,
      { note: payload },
      { id: `run-${payload}`, tags: [`tag:${payload}`] },
    );
    await engine.settle(id);
    const handler = engine.dashboard();

    for (const body of [
      await (await handler(get())).text(),
      await (await handler(get(`?run=${encodeURIComponent(id)}`))).text(),
      await (await handler(get(`?run=${encodeURIComponent(id)}&error=${encodeURIComponent(payload)}`))).text(),
      await (await handler(get(`?workflow=${encodeURIComponent(payload)}&cursor=${encodeURIComponent(payload)}`))).text(),
      await (await handler(get(`?chain=${encodeURIComponent(id)}`))).text(),
    ]) {
      expect(body).not.toContain("<script>");
      expect(body).toContain("&lt;script&gt;");
    }
  });

  it("404s a run that is not there and 405s a method it does not serve", async () => {
    const { engine } = harness([fulfilment]);
    const handler = engine.dashboard();

    const missing = await handler(get("?run=no-such-run"));
    expect(missing.status).toBe(404);
    expect(await missing.text()).toContain("No such run");

    const deleted = await handler(new Request(MOUNT, { method: "DELETE" }));
    expect(deleted.status).toBe(405);
    expect(deleted.headers.get("allow")).toBe("GET, HEAD, POST");
  });

  it("uses the given title and page size", async () => {
    const { engine } = harness([fulfilment]);
    for (let i = 0; i < 3; i++) await engine.start(fulfilment, { order: `ord_${i}` }, { id: `run-${i}` });

    const body = await (await engine.dashboard({ title: "Acme ops", limit: 1 })(get())).text();
    expect(body).toContain("<title>Acme ops</title>");
    expect(body).toContain("?run=run-2");
    expect(body).not.toContain("?run=run-1");
  });
});

/**
 * The token the signal form carries, and the post refused without it. The
 * session it is bound to is the mounting layer's, so every test here plays
 * that layer: a cookie it reads, a token it mints from it.
 */
describe("dashboard CSRF", () => {
  const sid = (request: Request) => /sid=(\w+)/.exec(request.headers.get("cookie") ?? "")?.[1] ?? "";
  /** A layer that binds its token to the session its cookie names. */
  const bound: DashboardCsrf = {
    token: (request) => `tok-${sid(request)}`,
    verify: (request, token) => token === `tok-${sid(request)}`,
  };

  /** The same request, arriving with the cookie the layer in front reads. */
  function as(session: string, request: Request): Request {
    const headers = new Headers(request.headers);
    headers.set("cookie", `sid=${session}`);
    return new Request(request, { headers });
  }

  /** A waiting run, which is the only state the form is offered for. */
  async function waiting() {
    const { engine, advance } = harness([fulfilment]);
    const id = await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1" });
    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);
    return { engine, id };
  }

  function tokenIn(body: string): string {
    const token = /name="_csrf" value="([^"]*)"/.exec(body)?.[1];
    expect(token).toBeDefined();
    return token!.replaceAll("&quot;", '"').replaceAll("&amp;", "&");
  }

  it("writes the layer's token into the signal form, asked for per request", async () => {
    const { engine } = await waiting();
    const asked: string[] = [];
    const handler = engine.dashboard({
      csrf: { token: (request) => (asked.push(sid(request)), `tok"${sid(request)}`), verify: () => true },
    });

    const body = await (await handler(as("alice", get("?run=run-1")))).text();

    // Escaped like every other value this page writes into an attribute.
    expect(body).toContain('name="_csrf" value="tok&quot;alice"');
    expect(tokenIn(body)).toBe('tok"alice');
    expect(asked).toEqual(["alice"]);
  });

  it("refuses a post with no token, and one the layer does not recognise", async () => {
    const { engine, id } = await waiting();
    const checked: string[] = [];
    const handler = engine.dashboard({
      csrf: { token: bound.token, verify: (request, token) => (checked.push(token), bound.verify(request, token)) },
    });

    for (const fields of [
      { run: "run-1", signal: "review", payload: '{"approved":true}' },
      { run: "run-1", signal: "review", payload: '{"approved":true}', _csrf: "tok-mallory" },
    ]) {
      const response = await handler(as("alice", post(fields)));
      expect(response.status).toBe(403);
      expect(await response.text()).toContain("did not come from this session");
    }

    // A refused post is not an operator's mistake to narrate on the run page:
    // the engine was never asked, so the run is where it was.
    expect((await engine.view(id))?.status).toBe("waiting");
    expect(checked).toEqual(["tok-mallory"]); // nothing to verify when the field is absent
  });

  it("accepts the token out of the form it rendered", async () => {
    const { engine, id } = await waiting();
    const handler = engine.dashboard({ csrf: bound });

    const page = await (await handler(as("alice", get("?run=run-1")))).text();
    const response = await handler(
      as("alice", post({ run: "run-1", signal: "review", payload: '{"approved":true}', _csrf: tokenIn(page) })),
    );

    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toBe("/ops/workflows?run=run-1&sent=review");
    expect((await engine.view(id))?.status).toBe("completed");
  });

  it("will not take one session's token from another, which is the point of binding it", async () => {
    const { engine, id } = await waiting();
    const handler = engine.dashboard({ csrf: bound });

    const alices = tokenIn(await (await handler(as("alice", get("?run=run-1")))).text());
    const fields = { run: "run-1", signal: "review", payload: '{"approved":true}', _csrf: alices };

    const stolen = await handler(as("mallory", post(fields)));
    expect(stolen.status).toBe(403);
    expect((await engine.view(id))?.status).toBe("waiting");

    const owner = await handler(as("alice", post(fields)));
    expect(owner.status).toBe(303);
    expect((await engine.view(id))?.status).toBe("completed");
  });

  it("fails closed when the check itself cannot answer", async () => {
    const { engine, id } = await waiting();
    const handler = engine.dashboard({
      csrf: {
        token: bound.token,
        verify: () => {
          throw new Error("session store is down");
        },
      },
    });

    const response = await handler(as("alice", post({ run: "run-1", signal: "review", payload: "null", _csrf: "tok-alice" })));

    expect(response.status).toBe(403);
    expect((await engine.view(id))?.status).toBe("waiting");
  });

  it("leaves a dashboard mounted without it exactly where it was", async () => {
    const { engine, id } = await waiting();

    const body = await (await engine.dashboard()(get("?run=run-1"))).text();
    expect(body).not.toContain("_csrf");

    const response = await engine.dashboard()(post({ run: "run-1", signal: "review", payload: '{"approved":true}' }));
    expect(response.status).toBe(303);
    expect((await engine.view(id))?.status).toBe("completed");
  });
});

/**
 * Ending a run from the screen, with the call the mounting layer hands over.
 * The capability is the whole subject here: every test is either a dashboard
 * that was given one, or one that was not and therefore cannot.
 */
describe("dashboard cancel", () => {
  /** A waiting run, which is the only state a run can be ended from. */
  async function waiting() {
    const { engine, advance } = harness([fulfilment]);
    const id = await engine.start(fulfilment, { order: "ord_1" }, { id: "run-1" });
    await engine.settle(id);
    advance(1_000);
    await engine.settle(id);
    return { engine, id };
  }

  function cancelPost(fields: Record<string, string> = {}): Request {
    return post({ action: "cancel", run: "run-1", ...fields });
  }

  it("offers no button, and takes no post, without the option", async () => {
    const { engine, id } = await waiting();
    const handler = engine.dashboard();

    const body = await (await handler(get("?run=run-1"))).text();
    expect(body).not.toContain('value="cancel"');
    expect(body).not.toContain("Cancel run");

    // The post is the half that matters: a form the screen never rendered is
    // the one an operator who wants this writes by hand, and the capability
    // has to be what refuses it rather than the absence of a button.
    const response = await handler(cancelPost());
    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toBe(
      "/ops/workflows?run=run-1&error=this+dashboard+cannot+cancel+runs",
    );
    expect((await engine.view(id))?.status).toBe("waiting");
  });

  it("ends the run with the call it was given, and says so on the page it lands on", async () => {
    const { engine, id } = await waiting();
    const asked: string[] = [];
    const handler = engine.dashboard({ cancel: (runId) => (asked.push(runId), engine.cancel(runId)) });

    const response = await handler(cancelPost());

    expect(asked).toEqual(["run-1"]);
    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toBe("/ops/workflows?run=run-1&canceled=1");
    expect((await engine.view(id))?.status).toBe("canceled");

    const body = await (await handler(get("?run=run-1&canceled=1"))).text();
    expect(body).toContain("This run was canceled");
  });

  it("offers the button on a live run, and withdraws it once there is nothing to end", async () => {
    const { engine } = await waiting();
    const handler = engine.dashboard({ cancel: (id) => engine.cancel(id) });

    const live = await (await handler(get("?run=run-1"))).text();
    expect(live).toContain('<input type="hidden" name="action" value="cancel" />');
    expect(live).toContain("Cancel run");

    await handler(cancelPost());

    // Terminal, so the same reason the signal form stands down applies: there
    // is no run left to act on, and a button that cannot do anything is worse
    // than none during the incident it is read in.
    const ended = await (await handler(get("?run=run-1"))).text();
    expect(ended).not.toContain('value="cancel"');
    expect(ended).toContain("it cannot be signalled");
  });

  it("reports what the call refused, as the operator's own mistake", async () => {
    const { engine, id } = await waiting();
    const handler = engine.dashboard({
      cancel: () => {
        throw new Error("alice may not cancel this tenant's runs");
      },
    });

    const response = await handler(cancelPost());

    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toContain("error=alice+may+not+cancel");
    expect((await engine.view(id))?.status).toBe("waiting");
  });

  it("is checked for CSRF like any other post, and fails closed", async () => {
    const { engine, id } = await waiting();
    const asked: string[] = [];
    const handler = engine.dashboard({
      cancel: (runId) => (asked.push(runId), engine.cancel(runId)),
      csrf: { token: () => "tok-alice", verify: (_request, token) => token === "tok-alice" },
    });

    for (const fields of [{}, { _csrf: "tok-mallory" }]) {
      const response = await handler(cancelPost(fields));
      expect(response.status).toBe(403);
      expect(await response.text()).toContain("did not come from this session");
    }

    // The destructive post is the one a cross-site form would be written for,
    // so the capability must not be reachable past the token check at all.
    expect(asked).toEqual([]);
    expect((await engine.view(id))?.status).toBe("waiting");

    const accepted = await handler(cancelPost({ _csrf: "tok-alice" }));
    expect(accepted.status).toBe(303);
    expect((await engine.view(id))?.status).toBe("canceled");
  });

  it("carries the token in the cancel form too, not only the signal one", async () => {
    const { engine } = await waiting();
    const handler = engine.dashboard({
      cancel: (id) => engine.cancel(id),
      csrf: { token: () => "tok-alice", verify: () => true },
    });

    const body = await (await handler(get("?run=run-1"))).text();

    // Both forms post to the same handler behind the same check, so a token on
    // one and not the other is a button that cannot work.
    const forms = body.split("<form").filter((part) => part.includes('method="post"'));
    expect(forms).toHaveLength(2);
    for (const form of forms) expect(form).toContain('name="_csrf" value="tok-alice"');
  });

  it("leaves the signal form alone: a post without the action still signals", async () => {
    const { engine, id } = await waiting();
    const asked: string[] = [];
    const handler = engine.dashboard({ cancel: (runId) => (asked.push(runId), engine.cancel(runId)) });

    const response = await handler(post({ run: "run-1", signal: "review", payload: '{"approved":true}' }));

    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toBe("/ops/workflows?run=run-1&sent=review");
    expect(asked).toEqual([]);
    expect((await engine.view(id))?.status).toBe("completed");
  });
});

/**
 * The tag that re-reads a screen, and the screens it is kept off. Every test
 * here asks for an interval and then looks in the head of the document,
 * because that is the whole of this: one meta element the browser acts on.
 */
describe("dashboard refresh", () => {
  /** Two generations, the last of them waiting: a chain that is still moving. */
  const relay = defineWorkflow<{ left: number }, string>("relayed", async (ctx, input) => {
    await ctx.step(`leg-${input.left}`, () => input.left);
    if (input.left > 1) return ctx.continueAsNew({ left: input.left - 1 });
    return ctx.waitFor<string>("baton");
  });

  /** One of each screen: a run waiting on a signal, and a live chain. */
  async function screens() {
    const h = harness([fulfilment, relay]);
    const run = await h.engine.start(fulfilment, { order: "ord_1" }, { id: "run-1" });
    await h.engine.settle(run);
    h.advance(1_000);
    await h.engine.settle(run);
    await h.engine.start(relay, { left: 2 }, { id: "r1" });
    await h.drain();
    return h;
  }

  const head = (body: string) => body.slice(0, body.indexOf("</head>"));

  it("writes no refresh anywhere unless the mount asks for one", async () => {
    const { engine } = await screens();
    const handler = engine.dashboard();

    for (const path of ["", "?run=run-1", "?chain=r1"]) {
      expect(await (await handler(get(path))).text()).not.toContain("http-equiv");
    }
  });

  it("re-reads the list and the chain on the interval it was given", async () => {
    const { engine } = await screens();
    const handler = engine.dashboard({ refreshSeconds: 15 });

    for (const path of ["", "?chain=r1"]) {
      const body = await (await handler(get(path))).text();
      expect(head(body)).toContain('<meta http-equiv="refresh" content="15" />');
      // And that is all of it: one document still, with nothing to host.
      expect(body).not.toContain("<script");
    }
  });

  it("leaves the run page still, because the signal box is on it", async () => {
    const { engine } = await screens();
    const handler = engine.dashboard({ refreshSeconds: 15 });

    const body = await (await handler(get("?run=run-1"))).text();

    // The guarantee: a reload here would take the payload the operator is
    // halfway through typing with it.
    expect(body).toContain("<textarea");
    expect(body).not.toContain("http-equiv");
  });

  it("stops re-reading a chain that has ended", async () => {
    const h = await screens();
    const handler = h.engine.dashboard({ refreshSeconds: 15 });

    await h.engine.signal("r1", "baton", "handed over");
    await h.drain();

    const body = await (await handler(get("?chain=r1"))).text();

    // Nothing will happen on it again, so a browser asking every fifteen
    // seconds is asking a question that has been answered.
    expect(body).toContain("completed");
    expect(body).not.toContain("http-equiv");
  });

  it("keeps it off a screen with nothing on it to watch", async () => {
    const { engine } = await screens();
    const refused = engine.dashboard({ refreshSeconds: 15, csrf: { token: () => "tok", verify: () => false } });

    const missing = await (await engine.dashboard({ refreshSeconds: 15 })(get("?run=no-such-run"))).text();
    const post403 = await (await refused(post({ run: "run-1", signal: "review" }))).text();

    expect(missing).toContain("No such run");
    expect(missing).not.toContain("http-equiv");
    expect(post403).toContain("did not come from this session");
    expect(post403).not.toContain("http-equiv");
  });

  it("reads an interval under a second as off, and the rest as whole seconds", async () => {
    const { engine } = await screens();

    for (const refreshSeconds of [0, -15, 0.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      const body = await (await engine.dashboard({ refreshSeconds })(get())).text();
      // Clamping a typo up to one second would make it a reload the operator
      // on the other end cannot stop.
      expect(body).not.toContain("http-equiv");
    }

    const body = await (await engine.dashboard({ refreshSeconds: 9.7 })(get())).text();
    expect(head(body)).toContain('content="9"');
  });
});
