import type { AddressInfo } from "node:net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { clearDatabricksTokenCache } from "@openclaw-line/shared";
import { loadConfig } from "../src/config.js";
import type { Dispatcher } from "../src/dispatcher.js";
import { PipelineStatus } from "../src/pipelineStatus.js";
import { createWorker } from "../src/server.js";

const cfg = { host: "https://dbx.example/", clientId: "app", clientSecret: "s3cret", jobId: "954973835355130" };

const runs = {
  runs: [
    {
      run_id: 2,
      run_name: "daily_revenue_lakehouse",
      start_time: 1_000,
      end_time: 0,
      trigger: "ONE_TIME",
      run_page_url: "https://dbx.example/?o=1#job/954973835355130/run/2",
      state: { life_cycle_state: "RUNNING" },
      job_parameters: [{ name: "run_date", value: "2026-10-11" }, { name: "inject_fault", value: "schema_drift" }, { name: "catalog", default: "lakehouse" }],
      tasks: [
        { task_key: "silver_transactions", depends_on: [{ task_key: "bronze_ingest" }], state: { life_cycle_state: "BLOCKED" } },
        { task_key: "land_stripe_events", start_time: 1_000, end_time: 2_000, state: { life_cycle_state: "TERMINATED", result_state: "SUCCESS" } },
        { task_key: "bronze_ingest", attempt_number: 0, depends_on: [{ task_key: "land_stripe_events" }], state: { result_state: "FAILED" } },
        { task_key: "bronze_ingest", attempt_number: 1, start_time: 2_000, depends_on: [{ task_key: "land_stripe_events" }], state: { life_cycle_state: "RUNNING" } },
      ],
    },
    { run_id: 1, start_time: 100, end_time: 400_100, run_duration: 400_000, state: { life_cycle_state: "TERMINATED", result_state: "SUCCESS" }, run_page_url: "javascript:alert(1)" },
  ],
};

function mockFetch(body: unknown = runs, status = 200) {
  return vi.fn(async (url: string | URL | Request) => {
    const u = String(url);
    if (u.endsWith("/oidc/v1/token")) return Response.json({ access_token: "tok", expires_in: 3600 });
    if (u.includes("/api/2.2/jobs/runs/list?job_id=954973835355130")) return status === 200 ? Response.json(body) : new Response("Bearer abcdef0123456789xyz denied", { status });
    return new Response("not found", { status: 404 });
  });
}

describe("PipelineStatus", () => {
  beforeEach(() => clearDatabricksTokenCache());

  it("maps runs: task dependency order, latest attempt, parameters, safe links", async () => {
    const d = await new PipelineStatus(cfg, { fetch: mockFetch() as typeof fetch, now: () => 5_000 }).get();
    if (!d.enabled) throw new Error("expected enabled");
    expect(d.job).toEqual({ id: "954973835355130", name: "daily_revenue_lakehouse", url: "https://dbx.example/jobs/954973835355130" });
    const [latest, older] = d.runs;
    expect(latest).toMatchObject({ runId: "2", state: "RUNNING", runDate: "2026-10-11", fault: "schema_drift", endedAt: null, trigger: "ONE_TIME" });
    expect(latest!.tasks.map((t) => [t.key, t.state])).toEqual([
      ["land_stripe_events", "SUCCESS"],
      ["bronze_ingest", "RUNNING"],
      ["silver_transactions", "BLOCKED"],
    ]);
    expect(older).toMatchObject({ runId: "1", state: "SUCCESS", durationSeconds: 400, url: null, runDate: null });
  });

  it("caches for a short time", async () => {
    let t = 0;
    const fetchMock = mockFetch();
    const status = new PipelineStatus(cfg, { fetch: fetchMock as typeof fetch, now: () => t });
    await status.get();
    t = 10_000;
    await status.get();
    t = 25_000;
    await status.get();
    expect(fetchMock.mock.calls.filter(([u]) => String(u).includes("runs/list"))).toHaveLength(2);
  });

  it("returns a sanitized error instead of throwing", async () => {
    const d = await new PipelineStatus(cfg, { fetch: mockFetch(null, 403) as typeof fetch }).get();
    expect(d).toMatchObject({ enabled: true, runs: [] });
    expect(d.enabled && d.error).toContain("Databricks API 403");
    expect(JSON.stringify(d)).not.toContain("abcdef0123456789xyz");
  });
});

describe("/api/pipeline", () => {
  const servers: { close: () => void }[] = [];
  afterEach(() => servers.splice(0).forEach((s) => s.close()));
  const dispatcher: Dispatcher = { dispatch: vi.fn() };
  const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const env = { WEBHOOK_SECRET: "w", OPENCLAW_HOOK_TOKEN: "t", LINE_TARGET_ID: "C1" };

  async function get(worker: ReturnType<typeof createWorker>): Promise<unknown> {
    await new Promise<void>((r) => worker.server.listen(0, "127.0.0.1", r));
    servers.push(worker.server);
    return (await fetch(`http://127.0.0.1:${(worker.server.address() as AddressInfo).port}/api/pipeline`)).json();
  }

  it("is disabled without Databricks settings", async () => {
    expect(loadConfig({ ...env, DATABRICKS_HOST: "https://dbx", DATABRICKS_JOB_ID: "1" }).databricks).toBeUndefined();
    expect(await get(createWorker(loadConfig(env), dispatcher, { log }))).toEqual({ enabled: false });
  });

  it("serves the pipeline status", async () => {
    const pipeline = { get: vi.fn(async () => ({ enabled: true as const, generatedAt: 1, job: { id: "1", name: "j", url: "https://dbx/jobs/1" }, runs: [] })) };
    expect(await get(createWorker(loadConfig(env), dispatcher, { log, pipeline }))).toMatchObject({ enabled: true, job: { id: "1" } });
  });
});
