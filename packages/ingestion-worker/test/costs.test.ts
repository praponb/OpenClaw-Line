import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { clearDatabricksTokenCache } from "@openclaw-line/shared";
import { loadConfig } from "../src/config.js";
import { CostTracker, bangkokMonthStart } from "../src/costs.js";
import { buildDashboardData, usageCostUsd } from "../src/dashboard.js";
import { HookDispatcher, type Dispatcher } from "../src/dispatcher.js";
import { FeedbackStore } from "../src/feedbackStore.js";
import { LineClient } from "../src/line.js";
import { PipelineStatus } from "../src/pipelineStatus.js";
import { createWorker } from "../src/server.js";
import { sign } from "../src/signature.js";

const prices = { input: 0.6, output: 2.5, cacheRead: 0.15, cacheWrite: 0.6 };

describe("model cost", () => {
  it("prices tokens per million and attributes usage to the incident via the agent run id", () => {
    expect(usageCostUsd({ input: 1_000_000, output: 200_000, cacheRead: 2_000_000, cacheWrite: 0 }, prices)).toBeCloseTo(0.6 + 0.5 + 0.3);
    const s = new FeedbackStore(":memory:");
    s.recordDispatch({ ref: "INC-AAAAAA", incidentKey: "k", dagId: "d", taskId: "t", runId: "r", firstSeenAt: 1000, dispatchedAt: 2000 });
    // usage arrives while the hook call is still open, before the run id is stored
    s.recordModelUsage({ runId: "run-1", provider: "moonshot", model: "kimi-k3", input: 10_000, output: 2_000, cacheRead: 30_000, cacheWrite: 0 }, 3000);
    s.recordModelUsage({ runId: "run-1", provider: "moonshot", model: "kimi-k3", input: 5_000, output: 1_000, cacheRead: 0, cacheWrite: 0 }, 4000);
    s.recordModelUsage({ runId: "other-run", input: 99, output: 99, cacheRead: 0, cacheWrite: 0 }, 4000);
    s.setAgentRunId("INC-AAAAAA", "run-1");
    const usage = s.usageSince(0);
    expect(usage.get("INC-AAAAAA")).toEqual({ input: 15_000, output: 3_000, cacheRead: 30_000, cacheWrite: 0, calls: 2, model: "moonshot/kimi-k3" });
    const d = buildDashboardData(s.incidentsSince(0), [], { days: 30, now: 5000, usage, prices });
    expect(d.incidents[0]!.costUsd).toBeCloseTo((15_000 * 0.6 + 3_000 * 2.5 + 30_000 * 0.15) / 1e6);
    expect(d.modelCost).toMatchObject({ input: 15_000, output: 3_000, cacheRead: 30_000, calls: 2, incidents: 1, priced: true });
    expect(buildDashboardData(s.incidentsSince(0), [], { days: 30, now: 5000, usage, prices: null }).modelCost).toMatchObject({ usd: null, priced: false, incidents: 1 });
  });

  it("links all model calls of the session whose report carries the incident ref", () => {
    const s = new FeedbackStore(":memory:");
    s.recordDispatch({ ref: "INC-CCCCCC", incidentKey: "k", dagId: "d", taskId: "t", runId: "r", firstSeenAt: 1000, dispatchedAt: 2000 });
    s.setAgentRunId("INC-CCCCCC", "hook-job-0d6e"); // the gateway's job id: never matches usage rows
    const session = "agent:pipeline-investigator:cron:e12c:run:a51a";
    s.recordModelUsage({ runId: "agent-run-702f", sessionKey: session, input: 200, output: 100, cacheRead: 5000, cacheWrite: 0 }, 2500); // tool-call turn
    s.recordModelUsage({ runId: "agent-run-702f", sessionKey: session, input: 416, output: 2200, cacheRead: 13312, cacheWrite: 0 }, 3000); // report turn
    s.recordModelUsage({ runId: "other", sessionKey: "agent:pipeline-investigator:cron:zzz:run:yyy", input: 9, output: 9, cacheRead: 0, cacheWrite: 0 }, 3000);
    expect(s.usageSince(0).get("INC-CCCCCC")).toBeUndefined();
    s.linkUsageSession("INC-CCCCCC", session);
    expect(s.usageSince(0).get("INC-CCCCCC")).toMatchObject({ input: 616, output: 2300, cacheRead: 18312, calls: 2 });
    const d = buildDashboardData(s.incidentsSince(0), [], { days: 30, now: 5000, usage: s.usageSince(0), prices: { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3 } });
    expect(d.incidents[0]!.costUsd).toBeCloseTo((616 * 3 + 2300 * 15 + 18312 * 0.3) / 1e6);
  });

  it("prices are optional and validated", () => {
    const base = { WEBHOOK_SECRET: "w", OPENCLAW_HOOK_TOKEN: "t", LINE_TARGET_ID: "C1" };
    expect(loadConfig(base).modelPrices).toBeNull();
    expect(loadConfig({ ...base, MODEL_PRICE_INPUT_PER_MTOK: "0.6", MODEL_PRICE_OUTPUT_PER_MTOK: "2.5" }).modelPrices).toEqual({ input: 0.6, output: 2.5, cacheRead: 0.6, cacheWrite: 0.6 });
    expect(() => loadConfig({ ...base, MODEL_PRICE_INPUT_PER_MTOK: "cheap", MODEL_PRICE_OUTPUT_PER_MTOK: "1" })).toThrow(/non-negative number/);
  });

  it("the hook dispatcher returns the agent run id from the gateway response", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify({ ok: true, runId: "d2f8bf39-38a2", completion: { status: "ok", delivered: true } })));
    const cfg = loadConfig({ WEBHOOK_SECRET: "w", OPENCLAW_HOOK_TOKEN: "t", LINE_TARGET_ID: "C1" }).openclaw;
    const r = await new HookDispatcher(cfg, { fetch: fetchMock as typeof fetch }).dispatch({ key: "k", ref: "INC-AAAAAA", dagId: "d", runId: "r", root: { dag_id: "d", task_id: "t", run_id: "r" }, failedDescendants: [], upstreamFailed: [], otherFailures: [] }, "m");
    expect(r.runId).toBe("d2f8bf39-38a2");
  });
});

describe("/v1/openclaw/usage", () => {
  const servers: { close: () => void }[] = [];
  afterEach(() => servers.splice(0).forEach((s) => s.close()));
  it("stores signed usage and rejects unsigned or malformed requests", async () => {
    const store = new FeedbackStore(":memory:");
    const dispatcher: Dispatcher = { dispatch: vi.fn() };
    const worker = createWorker(loadConfig({ WEBHOOK_SECRET: "whsec", OPENCLAW_HOOK_TOKEN: "t", LINE_TARGET_ID: "C1" }), dispatcher, { store, log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() }, now: () => 7_000 });
    await new Promise<void>((r) => worker.server.listen(0, "127.0.0.1", r));
    servers.push(worker.server);
    const url = `http://127.0.0.1:${(worker.server.address() as AddressInfo).port}/v1/openclaw/usage`;
    const post = (body: string, secret = "whsec") => {
      const ts = String(Math.floor(Date.now() / 1000));
      return fetch(url, { method: "POST", headers: { "x-openclaw-timestamp": ts, "x-openclaw-signature": sign(secret, ts, body) }, body });
    };
    store.recordDispatch({ ref: "INC-BBBBBB", incidentKey: "k", dagId: "d", taskId: "t", runId: "r", dispatchedAt: 1 });
    const session = "agent:pipeline-investigator:cron:j:run:r";
    expect((await post(JSON.stringify({ runId: "run-9", sessionKey: session, input: 100, output: 20, cacheRead: 0, cacheWrite: 0 }))).status).toBe(202);
    expect((await post(JSON.stringify({ runId: "run-9", sessionKey: session, ref: "INC-BBBBBB", input: 50, output: 10, cacheRead: 0, cacheWrite: 0 }))).status).toBe(202);
    expect((await post(JSON.stringify({ runId: "run-9", input: 1 }), "wrong")).status).toBe(401);
    expect((await post(JSON.stringify({ runId: "bad id; drop", input: 1 }))).status).toBe(400);
    expect((await post(JSON.stringify({ runId: "x", sessionKey: "s:1", ref: "INC-ZZZZZZ", input: 1 }))).status).toBe(202); // unknown ref: stored, not linked
    expect(store.usageSince(0).get("INC-BBBBBB")).toMatchObject({ input: 150, output: 30, calls: 2 });
  });
});

describe("month-to-date costs", () => {
  it("Bangkok month start", () => {
    // 2026-10-31 18:00 UTC is already 1 November in Bangkok.
    expect(new Date(bangkokMonthStart(Date.UTC(2026, 9, 31, 18))).toISOString()).toBe("2026-10-31T17:00:00.000Z");
    expect(new Date(bangkokMonthStart(Date.UTC(2026, 9, 11, 9))).toISOString()).toBe("2026-09-30T17:00:00.000Z");
  });

  it("reads LINE's quota and the Databricks job's monthly compute, and degrades per source", async () => {
    clearDatabricksTokenCache();
    const lineFetch = vi.fn(async (url: string | URL | Request) =>
      String(url).endsWith("/quota") ? Response.json({ type: "limited", value: 300 }) : Response.json({ totalUsage: 42 }),
    );
    const dbxFetch = vi.fn(async (url: string | URL | Request) => {
      const u = String(url);
      if (u.endsWith("/oidc/v1/token")) return Response.json({ access_token: "tok", expires_in: 3600 });
      expect(u).toContain("start_time_from=");
      return Response.json({
        runs: [
          { run_id: 1, end_time: 9, state: { result_state: "SUCCESS" }, tasks: [{ task_key: "a", setup_duration: 5_000, execution_duration: 60_000, state: { result_state: "SUCCESS" } }] },
          { run_id: 2, end_time: 9, state: { result_state: "FAILED" }, tasks: [{ task_key: "a", setup_duration: 300_000, execution_duration: 30_000, state: { result_state: "FAILED" } }] },
        ],
      });
    });
    const pipeline = new PipelineStatus({ host: "https://dbx", clientId: "c", clientSecret: "s", jobId: "1" }, { fetch: dbxFetch as typeof fetch });
    const tracker = new CostTracker({ line: new LineClient("tok", { fetch: lineFetch as typeof fetch }), pipeline, now: () => Date.UTC(2026, 9, 11, 9) });
    const d = await tracker.get();
    expect(d.line).toEqual({ limit: 300, used: 42 });
    expect(d.databricks).toMatchObject({ runs: 2, succeeded: 1, failed: 1, computeSeconds: 90, waitSeconds: 305 });

    const broken = new CostTracker({ line: { quota: async () => { throw new Error("LINE API 401 on /v2/bot/message/quota"); } }, now: () => 0 });
    expect(await broken.get()).toMatchObject({ line: { error: "LINE API 401 on /v2/bot/message/quota" }, databricks: null });
  });
});
