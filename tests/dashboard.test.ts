import { describe, expect, it } from "vitest";
import { type DashboardEngine, type RunQuery, dashboard, defineSignal, defineWorkflow } from "../src/index.js";
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
