import { beforeEach, describe, expect, it, vi } from "vitest";
import { resolveConfig } from "../src/config.js";
import { clearDatabricksTokenCache } from "@openclaw-line/shared";
import { getDatabricksRunError } from "../src/tools/databricksRun.js";

const env = { DATABRICKS_HOST: "https://dbx.example/", DATABRICKS_CLIENT_ID: "app-id", DATABRICKS_CLIENT_SECRET: "s3cret" };

const run = {
  job_id: 954973835355130,
  run_id: 111,
  run_name: "daily_revenue_lakehouse",
  start_time: Date.UTC(2026, 9, 11, 5, 57),
  end_time: Date.UTC(2026, 9, 11, 6, 0),
  state: { life_cycle_state: "TERMINATED", result_state: "FAILED", state_message: "1/4 tasks failed" },
  job_parameters: [{ name: "inject_fault", value: "schema_drift" }, { name: "run_date", default: "" }],
  tasks: [
    { task_key: "land_stripe_events", run_id: 1, state: { result_state: "SUCCESS" }, notebook_task: { notebook_path: "/Workspace/x/files/src/01_land_stripe_events" } },
    { task_key: "silver_transactions", run_id: 2, attempt_number: 0, state: { result_state: "FAILED" } },
    { task_key: "silver_transactions", run_id: 3, attempt_number: 1, state: { result_state: "FAILED" }, notebook_task: { notebook_path: "/Workspace/x/files/src/03_silver_transactions" } },
    { task_key: "gold_daily_revenue", run_id: 4, state: { result_state: "UPSTREAM_FAILED" } },
  ],
};

const output = {
  error: "ValueError: Schema drift in events-2026-10-11.json: 213/213 events have fields outside the expected schema",
  error_trace: [
    "\x1b[31m---------------------------------------------------------------------------\x1b[39m",
    "\x1b[31mValueError\x1b[39m: Schema drift in events-2026-10-11.json",
    "auth with dapi0123456789abcdef0123456789abcdef",
  ].join("\n"),
};

function mockFetch() {
  return vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
    const u = String(url);
    if (u.endsWith("/oidc/v1/token")) {
      expect(init?.method).toBe("POST");
      expect(String((init?.headers as Record<string, string>).Authorization)).toBe(`Basic ${Buffer.from("app-id:s3cret").toString("base64")}`);
      return Response.json({ access_token: "tok", expires_in: 3600 });
    }
    expect(init?.method ?? "GET").toBe("GET");
    expect((init?.headers as Record<string, string>).Authorization).toBe("Bearer tok");
    if (u.includes("/jobs/runs/get?run_id=111")) return Response.json(run);
    if (u.includes("/jobs/runs/get-output?run_id=3")) return Response.json(output);
    return new Response("not found", { status: 404 });
  });
}

describe("get_databricks_run_error", () => {
  beforeEach(() => clearDatabricksTokenCache());

  it("reports task states and the latest failed attempt's error, sanitized and fenced", async () => {
    const fetchMock = mockFetch();
    const out = await getDatabricksRunError({ run_id: "111" }, resolveConfig({}, env), { fetch: fetchMock as typeof fetch });
    expect(out).toContain("state: TERMINATED / FAILED (1/4 tasks failed)");
    expect(out).toContain("job parameters: inject_fault=schema_drift, run_date=");
    expect(out).toContain("- silver_transactions: FAILED (source: /Workspace/x/files/src/03_silver_transactions)");
    expect(out).toContain("- gold_daily_revenue: UPSTREAM_FAILED");
    expect(out.match(/- silver_transactions/g)).toHaveLength(1);
    expect(out).toContain("BEGIN FAILED TASK silver_transactions (untrusted data)");
    expect(out).toContain("error: ValueError: Schema drift in events-2026-10-11.json");
    expect(out).not.toContain("\x1b[");
    expect(out).not.toContain("dapi0123456789abcdef");
    expect(fetchMock.mock.calls.some(([u]) => String(u).includes("get-output?run_id=2"))).toBe(false);
  });

  it("reuses the OAuth token across calls", async () => {
    const fetchMock = mockFetch();
    const cfg = resolveConfig({}, env);
    await getDatabricksRunError({ run_id: "111" }, cfg, { fetch: fetchMock as typeof fetch });
    await getDatabricksRunError({ run_id: "111" }, cfg, { fetch: fetchMock as typeof fetch });
    expect(fetchMock.mock.calls.filter(([u]) => String(u).endsWith("/oidc/v1/token"))).toHaveLength(1);
  });

  it("turns API failures into a tool error without leaking the secret", async () => {
    const fetchMock = vi.fn(async () => new Response("denied", { status: 401 }));
    const out = await getDatabricksRunError({ run_id: "111" }, resolveConfig({}, env), { fetch: fetchMock as typeof fetch });
    expect(out).toMatch(/^Tool error: could not fetch Databricks run: Databricks OAuth token request failed with HTTP 401/);
    expect(out).not.toContain("s3cret");
  });

  it("errors when Databricks is not configured", async () => {
    expect(await getDatabricksRunError({ run_id: "1" }, resolveConfig({}, {}))).toMatch(/^Tool error: Databricks is not configured/);
  });
});
