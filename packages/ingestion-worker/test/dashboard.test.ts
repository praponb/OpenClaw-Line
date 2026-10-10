import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import { DASHBOARD_CSP, buildDashboardData, dashboardHtml } from "../src/dashboard.js";
import type { Dispatcher } from "../src/dispatcher.js";
import { FeedbackStore } from "../src/feedbackStore.js";
import { LineClient } from "../src/line.js";
import { createWorker } from "../src/server.js";

function seeded(): FeedbackStore {
  const s = new FeedbackStore(":memory:");
  // Delivered on prompt v1, voted down with a reason.
  s.recordDispatch({ ref: "INC-AAAAAA", incidentKey: "d:r1", dagId: "d", taskId: "t", runId: "r1", firstSeenAt: 1_000, dispatchedAt: 31_000, model: "moonshot/kimi-k3" });
  s.recordReport("INC-AAAAAA", { text: "🚨 report <b>not html</b>\nRef: INC-AAAAAA", sentAt: 66_000, promptVersion: "v1" });
  s.recordCompletion("INC-AAAAAA", { status: "ok", delivered: true, replyDisposition: "visible" }, 67_000);
  s.recordRating("INC-AAAAAA", "u:1", "down", 70_000);
  s.recordReason("INC-AAAAAA", "u:1", "blast_radius", 71_000);
  // Delivered on prompt v2, two 👍.
  s.recordDispatch({ ref: "INC-BBBBBB", incidentKey: "d:r2", dagId: "d", taskId: "t", runId: "r2", firstSeenAt: 100_000, dispatchedAt: 130_000 });
  s.recordReport("INC-BBBBBB", { text: "report 2", sentAt: 175_000, promptVersion: "v2" });
  s.recordCompletion("INC-BBBBBB", { status: "ok", delivered: true }, 176_000);
  s.recordRating("INC-BBBBBB", "u:1", "up", 180_000);
  s.recordRating("INC-BBBBBB", "u:2", "up", 181_000);
  // Failed run (fallback) and one still running.
  s.recordDispatch({ ref: "INC-CCCCCC", incidentKey: "d:r3", dagId: "d", taskId: "t", runId: "r3", firstSeenAt: 200_000, dispatchedAt: 230_000 });
  s.recordCompletion("INC-CCCCCC", { status: "error", delivered: false }, 240_000);
  s.recordDispatch({ ref: "INC-DDDDDD", incidentKey: "d:r4", dagId: "d", taskId: "t", runId: "r4", firstSeenAt: 300_000, dispatchedAt: 330_000 });
  return s;
}

describe("buildDashboardData", () => {
  it("summarizes the funnel, votes, triage and prompt versions", () => {
    const s = seeded();
    const d = buildDashboardData(s.incidentsSince(0), s.feedbackSince(0), { days: 30, now: 400_000 });
    expect(d.funnel).toEqual({ investigated: 4, completed: 3, delivered: 2, fallback: 1, rated: 2 });
    expect(d.votes).toMatchObject({ up: 2, down: 1, misleading: 0, total: 3 });
    expect(d.votes.helpfulRate).toBeCloseTo(2 / 3);
    expect(d.triage).toEqual({ medianSeconds: 70, p90Seconds: 74 }); // 65s and 75s
    expect(d.versions.map((v) => [v.version, v.incidents, v.helpfulRate])).toEqual([
      ["v1", 1, 0],
      ["v2", 1, 1],
      ["unknown", 2, null],
    ]);
    expect(d.reasons).toEqual([{ reason: "blast_radius", label: "Missed blast radius", count: 1 }]);
    expect(d.incidents.map((i) => [i.ref, i.state])).toEqual([
      ["INC-DDDDDD", "running"],
      ["INC-CCCCCC", "failed"],
      ["INC-BBBBBB", "delivered"],
      ["INC-AAAAAA", "delivered"],
    ]);
    const a = d.incidents.find((i) => i.ref === "INC-AAAAAA")!;
    expect(a).toMatchObject({ triageSeconds: 65, votes: { up: 0, down: 1, misleading: 0 }, model: "moonshot/kimi-k3" });
    expect(JSON.stringify(d)).not.toContain("u:1"); // voter hashes are never exposed
  });
});

describe("dashboard routes", () => {
  const servers: { close: () => void }[] = [];
  afterEach(() => servers.splice(0).forEach((s) => s.close()));

  async function listen(server: import("node:http").Server): Promise<number> {
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    servers.push(server);
    return (server.address() as AddressInfo).port;
  }

  const env = {
    WEBHOOK_SECRET: "whsec",
    OPENCLAW_HOOK_TOKEN: "t",
    LINE_TARGET_ID: "C1",
    LINE_CHANNEL_ACCESS_TOKEN: "tok",
    LINE_CHANNEL_SECRET: "sec",
    FEEDBACK_DB_PATH: ":memory:",
  };
  const dispatcher: Dispatcher = { dispatch: vi.fn() };
  const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

  it("serves the page with a strict CSP and the JSON API on the internal listener only", async () => {
    const worker = createWorker(loadConfig(env), dispatcher, { store: seeded(), line: new LineClient("tok"), log, now: () => 400_000 });
    const port = await listen(worker.server);
    const page = await fetch(`http://127.0.0.1:${port}/dashboard`);
    expect(page.status).toBe(200);
    expect(page.headers.get("content-security-policy")).toBe(DASHBOARD_CSP);
    expect(await page.text()).toContain("<title>Incident Investigator Dashboard</title>");
    expect((await fetch(`http://127.0.0.1:${port}/dashboard?reload=1`)).status).toBe(200);
    expect((await fetch(`http://127.0.0.1:${port}/api/dashboardx`)).status).toBe(404);
    const api = await fetch(`http://127.0.0.1:${port}/api/dashboard?days=7`);
    expect((await api.json()).funnel.investigated).toBe(4);
    expect((await fetch(`http://127.0.0.1:${port}/api/dashboard?days=0`)).status).toBe(400);

    const publicPort = await listen(worker.publicServer!);
    expect((await fetch(`http://127.0.0.1:${publicPort}/dashboard`)).status).toBe(404);
    expect((await fetch(`http://127.0.0.1:${publicPort}/api/dashboard`)).status).toBe(404);
  });

  it("reports feedback disabled when there is no store", async () => {
    const worker = createWorker(loadConfig({ WEBHOOK_SECRET: "w", OPENCLAW_HOOK_TOKEN: "t", LINE_TARGET_ID: "C1" }), dispatcher, { log });
    const port = await listen(worker.server);
    expect(await (await fetch(`http://127.0.0.1:${port}/api/dashboard`)).json()).toEqual({ enabled: false });
  });
});

describe("dashboard page", () => {
  it("never renders data with innerHTML", () => {
    const html = dashboardHtml();
    expect(html).not.toMatch(/innerHTML|outerHTML|insertAdjacentHTML|document\.write/);
  });
});
