import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import type { Dispatcher } from "../src/dispatcher.js";
import { PipelineTrigger } from "../src/pipelineTrigger.js";
import { createWorker } from "../src/server.js";

const cfg = { baseUrl: "http://airflow:8080", username: "dashboard", password: "pw", dagId: "databricks_daily_revenue", dailyLimit: 2 };

function airflow(opts: { paused?: boolean; active?: { dag_run_id: string; state: string }[] } = {}) {
  const calls: { method: string; url: string; body?: unknown }[] = [];
  const fetchMock = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
    const u = String(url);
    calls.push({ method: init?.method ?? "GET", url: u, body: init?.body ? JSON.parse(String(init.body)) : undefined });
    if (u.endsWith("/auth/token")) return Response.json({ access_token: "tok" }, { status: 201 });
    if (u.endsWith("/api/v2/dags/databricks_daily_revenue")) return Response.json({ is_paused: !!opts.paused });
    if (u.includes("/dagRuns?state=")) return Response.json({ dag_runs: opts.active ?? [] });
    if (u.endsWith("/dagRuns") && init?.method === "POST") return Response.json({ dag_run_id: "manual__2026-10-11T09:50:00+00:00" });
    return new Response("not found", { status: 404 });
  });
  return { fetchMock, calls };
}

describe("PipelineTrigger", () => {
  it("starts the DAG with the chosen fault, then enforces cooldown and the daily limit", async () => {
    let t = Date.UTC(2026, 9, 11, 9, 50);
    const { fetchMock, calls } = airflow();
    const trigger = new PipelineTrigger(cfg, { now: () => t, fetch: fetchMock as typeof fetch });
    expect(await trigger.trigger("schema_drift")).toEqual({ ok: true, dagRunId: "manual__2026-10-11T09:50:00+00:00", fault: "schema_drift", remainingToday: 1 });
    const post = calls.find((c) => c.method === "POST" && c.url.endsWith("/dagRuns"))!;
    expect(post.body).toEqual({ logical_date: null, conf: { inject_fault: "schema_drift" } });
    expect(calls[0]!.body).toEqual({ username: "dashboard", password: "pw" });
    t += 30_000;
    expect(await trigger.trigger("none")).toMatchObject({ ok: false, status: 429, retryAfterSeconds: 30 });
    t += 60_000;
    expect(await trigger.trigger("none")).toMatchObject({ ok: true, remainingToday: 0 });
    t += 120_000;
    expect(await trigger.trigger("none")).toMatchObject({ ok: false, status: 429, error: expect.stringContaining("Daily limit of 2") });
  });

  it("refuses unknown faults, a paused DAG and a run already in progress", async () => {
    const now = () => Date.UTC(2026, 9, 11, 9, 50);
    const ok = airflow();
    expect(await new PipelineTrigger(cfg, { now, fetch: ok.fetchMock as typeof fetch }).trigger("rm -rf")).toMatchObject({ ok: false, status: 400 });
    expect(ok.fetchMock).not.toHaveBeenCalled();
    const paused = airflow({ paused: true });
    expect(await new PipelineTrigger(cfg, { now, fetch: paused.fetchMock as typeof fetch }).trigger("none")).toMatchObject({ ok: false, status: 409, error: expect.stringContaining("paused") });
    const busy = airflow({ active: [{ dag_run_id: "manual__x", state: "running" }] });
    expect(await new PipelineTrigger(cfg, { now, fetch: busy.fetchMock as typeof fetch }).trigger("none")).toMatchObject({ ok: false, status: 409, error: expect.stringContaining("already running (manual__x)") });
    expect(busy.calls.some((c) => c.method === "POST" && c.url.endsWith("/dagRuns"))).toBe(false);
  });
});

describe("/api/trigger-pipeline", () => {
  const servers: { close: () => void }[] = [];
  afterEach(() => servers.splice(0).forEach((s) => s.close()));
  const env = {
    WEBHOOK_SECRET: "w",
    OPENCLAW_HOOK_TOKEN: "t",
    LINE_TARGET_ID: "C1",
    CF_ACCESS_TEAM_DOMAIN: "team.cloudflareaccess.com",
    CF_ACCESS_AUD: "aud",
    DASHBOARD_ALLOWED_EMAILS: "me@example.com",
  };

  it("is off without the trigger password and works behind Access with the dashboard header", async () => {
    expect(loadConfig({ ...env, AIRFLOW_BASE_URL: "http://af", AIRFLOW_USERNAME: "investigator", AIRFLOW_PASSWORD: "x" }).pipelineTrigger).toBeUndefined();
    expect(loadConfig({ ...env, AIRFLOW_BASE_URL: "http://af", AIRFLOW_USERNAME: "investigator", AIRFLOW_PASSWORD: "x", AIRFLOW_TRIGGER_PASSWORD: "pw" }).pipelineTrigger).toMatchObject({ username: "dashboard", dagId: "databricks_daily_revenue", dailyLimit: 20 });
    const trigger = { dagId: "databricks_daily_revenue", trigger: vi.fn(async (fault: string) => ({ ok: true as const, dagRunId: "manual__1", fault, remainingToday: 19 })) };
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const accessVerifier = { verify: vi.fn(async (tok: string | undefined) => (tok === "good" ? { ok: true as const, email: "me@example.com" } : { ok: false as const, reason: "missing" })) };
    const worker = createWorker(loadConfig(env), { dispatch: vi.fn() } as Dispatcher, { log, accessVerifier, trigger });
    await new Promise<void>((r) => worker.dashboardServer!.listen(0, "127.0.0.1", r));
    servers.push(worker.dashboardServer!);
    const url = `http://127.0.0.1:${(worker.dashboardServer!.address() as AddressInfo).port}/api/trigger-pipeline`;
    const auth = { "Cf-Access-Jwt-Assertion": "good" };
    expect(await (await fetch(url, { headers: auth })).json()).toEqual({ enabled: true, dagId: "databricks_daily_revenue" });
    expect((await fetch(`${url}?fault=none`, { method: "POST", headers: { "X-Dashboard-Action": "run-pipeline" } })).status).toBe(403); // no Access
    expect((await fetch(`${url}?fault=none`, { method: "POST", headers: { ...auth, "X-Dashboard-Action": "send-test" } })).status).toBe(403); // wrong action
    const res = await fetch(`${url}?fault=schema_drift`, { method: "POST", headers: { ...auth, "X-Dashboard-Action": "run-pipeline" } });
    expect(res.status).toBe(200);
    expect(trigger.trigger).toHaveBeenCalledWith("schema_drift");
    expect(log.info).toHaveBeenCalledWith(expect.stringContaining("pipeline run requested by me@example.com (fault=schema_drift): started manual__1"));
  });
});
