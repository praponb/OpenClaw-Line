import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AirflowClient } from "@openclaw-line/shared";
import { buildIncidentMessage } from "../src/buildMessage.js";
import { loadConfig } from "../src/config.js";
import { HookDispatcher, type Dispatcher } from "../src/dispatcher.js";
import { createWorker } from "../src/server.js";
import { sign, verifySignature } from "../src/signature.js";
import type { Incident } from "../src/types.js";

const SECRET = "whsec_test";
const baseEnv = {
  WEBHOOK_SECRET: SECRET,
  DEDUPE_WINDOW_S: "60",
  OPENCLAW_HOOK_TOKEN: "hook-token",
  LINE_TARGET_ID: "C0123456789abcdef0123456789abcdef",
  OPENCLAW_GATEWAY_URL: "http://gateway:18789",
};

const LOG = [
  "[2026-10-10T02:05:00Z] INFO - Connecting to warehouse postgres://etl_user:hunter2@warehouse:5432/analytics",
  "[2026-10-10T02:05:01Z] INFO - Processing charge.refunded evt_3 for sam.lee@example.com",
  "Traceback (most recent call last):",
  '  File "/opt/airflow/dags/stripe_ingest/extract_stripe.py", line 36, in to_staging_row',
  "KeyError: 'payment_intent_id'",
].join("\n");

function fakeAirflow(): AirflowClient {
  const json = (b: unknown) => new Response(JSON.stringify(b), { status: 200 });
  const fetchMock = vi.fn(async (url: string | URL | Request) => {
    const u = String(url);
    if (u.endsWith("/tasks")) {
      return json({ tasks: [{ task_id: "extract_stripe_transactions", downstream_task_ids: ["validate_extract_counts", "stg_stripe_transactions"] }, { task_id: "validate_extract_counts", downstream_task_ids: [] }, { task_id: "stg_stripe_transactions", downstream_task_ids: [] }] });
    }
    if (u.includes("/taskInstances?")) {
      return json({
        task_instances: [
          { task_id: "extract_stripe_transactions", state: "failed", try_number: 1, start_date: null, end_date: "2026-10-10T02:05:02Z" },
          { task_id: "validate_extract_counts", state: "failed", try_number: 1, start_date: null, end_date: "2026-10-10T02:05:09Z" },
          { task_id: "stg_stripe_transactions", state: "upstream_failed", try_number: 0, start_date: null, end_date: null },
        ],
      });
    }
    if (u.includes("/logs/")) return json({ content: LOG });
    return new Response("{}", { status: 404 });
  });
  return new AirflowClient({ baseUrl: "http://af", username: "u", password: "p", apiVersion: "v1", fetch: fetchMock as typeof fetch });
}

let close: (() => void) | undefined;
afterEach(() => {
  close?.();
  vi.useRealTimers();
});

async function post(port: number, body: unknown, secret = SECRET) {
  const raw = JSON.stringify(body);
  const ts = String(Math.floor(Date.now() / 1000));
  return fetch(`http://127.0.0.1:${port}/v1/airflow/failure`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-OpenClaw-Timestamp": ts, "X-OpenClaw-Signature": sign(secret, ts, raw) },
    body: raw,
  });
}

describe("ingestion worker end to end", () => {
  it("dedupes two callbacks into one sanitized investigation of the earliest failed task", async () => {
    const dispatched: { incident: Incident; message: string }[] = [];
    const dispatcher: Dispatcher = {
      dispatch: async (incident, message) => {
        dispatched.push({ incident, message });
        return { ok: true, detail: "test" };
      },
    };
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const { server, aggregator } = createWorker(loadConfig(baseEnv), dispatcher, { airflow: fakeAirflow(), log });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    close = () => server.close();
    const port = (server.address() as AddressInfo).port;

    const common = { dag_id: "daily_revenue_pipeline", run_id: "scheduled__2026-10-10T02:00:00+00:00", try_number: 1 };
    const r1 = await post(port, { ...common, task_id: "validate_extract_counts", exception: "ValueError: extract produced no rows" });
    const r2 = await post(port, { ...common, task_id: "extract_stripe_transactions", exception: "KeyError: 'payment_intent_id'" });
    expect(r1.status).toBe(202);
    expect(await r2.json()).toEqual({ accepted: true, outcome: "joined" });

    await aggregator.flushAll();
    expect(dispatched).toHaveLength(1);
    const { incident, message } = dispatched[0]!;
    expect(incident.root.task_id).toBe("extract_stripe_transactions");
    expect(incident.failedDescendants).toEqual(["validate_extract_counts"]);
    expect(incident.upstreamFailed).toEqual(["stg_stripe_transactions"]);
    expect(message).toContain("task_id (earliest failed task - root-cause this one): extract_stripe_transactions");
    expect(message).toContain("KeyError: 'payment_intent_id'");
    expect(message).toContain('extract_stripe.py", line 36');
    expect(message).not.toContain("hunter2");
    expect(message).not.toContain("sam.lee@example.com");

    // A late callback for the same run does not trigger a second investigation.
    const r3 = await post(port, { ...common, task_id: "stg_stripe_transactions" });
    expect(await r3.json()).toEqual({ accepted: true, outcome: "duplicate" });
  });

  it("rejects bad signatures and malformed payloads", async () => {
    const dispatcher: Dispatcher = { dispatch: vi.fn() };
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const { server } = createWorker(loadConfig(baseEnv), dispatcher, { log });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    close = () => server.close();
    const port = (server.address() as AddressInfo).port;
    expect((await post(port, { dag_id: "d", task_id: "t", run_id: "r" }, "wrong")).status).toBe(401);
    expect((await post(port, { dag_id: "d" })).status).toBe(400);
    expect((await fetch(`http://127.0.0.1:${port}/healthz`)).status).toBe(200);
  });
});

describe("verifySignature", () => {
  it("rejects stale timestamps", () => {
    const sig = sign(SECRET, "1000", "{}");
    expect(verifySignature(SECRET, "{}", "1000", sig, 1000)).toBe(true);
    expect(verifySignature(SECRET, "{}", "1000", sig, 1000 + 301)).toBe(false);
  });
});

describe("HookDispatcher", () => {
  const incident: Incident = {
    key: "daily_revenue_pipeline:run1",
    dagId: "daily_revenue_pipeline",
    runId: "run1",
    root: { dag_id: "daily_revenue_pipeline", task_id: "extract", run_id: "run1" },
    failedDescendants: [],
    upstreamFailed: [],
    otherFailures: [],
  };

  it("posts an isolated, LINE-delivered agent run with an idempotency key", async () => {
    const fetchMock = vi.fn(async () => new Response('{"ok":true,"runId":"x"}', { status: 200 }));
    const d = new HookDispatcher(loadConfig(baseEnv).openclaw, { fetch: fetchMock as typeof fetch });
    const r = await d.dispatch(incident, "msg");
    expect(r.ok).toBe(true);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("http://gateway:18789/hooks/agent");
    const headers = init.headers as Record<string, string>;
    expect(headers.Authorization).toBe("Bearer hook-token");
    expect(headers["Idempotency-Key"]).toBe("incident:daily_revenue_pipeline:run1");
    const body = JSON.parse(String(init.body));
    expect(body).toMatchObject({
      message: "msg",
      agentId: "pipeline-investigator",
      sessionMode: "isolated",
      deliver: true,
      channel: "line",
      to: "C0123456789abcdef0123456789abcdef",
    });
    expect(body).not.toHaveProperty("sessionKey");
  });

  it("retries 5xx but not 4xx", async () => {
    const fail5 = vi.fn(async () => new Response("down", { status: 503 }));
    const d5 = new HookDispatcher(loadConfig(baseEnv).openclaw, { fetch: fail5 as typeof fetch, sleep: async () => {} });
    expect((await d5.dispatch(incident, "m")).ok).toBe(false);
    expect(fail5).toHaveBeenCalledTimes(3);

    const fail4 = vi.fn(async () => new Response("bad", { status: 400 }));
    const d4 = new HookDispatcher(loadConfig(baseEnv).openclaw, { fetch: fail4 as typeof fetch, sleep: async () => {} });
    expect((await d4.dispatch(incident, "m")).ok).toBe(false);
    expect(fail4).toHaveBeenCalledTimes(1);
  });
});

describe("buildIncidentMessage", () => {
  it("sanitizes the callback exception and notes a missing log", () => {
    const msg = buildIncidentMessage({
      key: "k",
      dagId: "d",
      runId: "r",
      root: { dag_id: "d", task_id: "t", run_id: "r", exception: "OperationalError: password=hunter2 for admin@corp.com" },
      failedDescendants: [],
      upstreamFailed: [],
      otherFailures: [],
      logNote: "log fetch failed: Airflow API 404",
    });
    expect(msg).not.toContain("hunter2");
    expect(msg).not.toContain("admin@corp.com");
    expect(msg).toContain("LOG EXCERPT: unavailable (log fetch failed: Airflow API 404)");
  });
});
