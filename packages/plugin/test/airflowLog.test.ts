import { describe, expect, it, vi } from "vitest";
import { resolveConfig } from "../src/config.js";
import { getAirflowTaskLog } from "../src/tools/airflowLog.js";

const env = { AIRFLOW_BASE_URL: "http://af", AIRFLOW_API_VERSION: "v1", AIRFLOW_USERNAME: "viewer", AIRFLOW_PASSWORD: "pw" };

describe("get_airflow_task_log", () => {
  it("filters and sanitizes the log", async () => {
    const log = [
      ...Array.from({ length: 600 }, (_, i) => `INFO - noise ${i}`),
      "INFO - Connecting to warehouse postgres://etl_user:hunter2@warehouse:5432/analytics",
      "Traceback (most recent call last):",
      '  File "/opt/airflow/dags/stripe_ingest/extract_stripe.py", line 36, in to_staging_row',
      "KeyError: 'payment_intent_id'",
    ].join("\n");
    const fetchMock = vi.fn(async () => new Response(JSON.stringify({ content: log }), { status: 200 }));
    const out = await getAirflowTaskLog(
      { dag_id: "d", task_id: "t", dag_run_id: "r", try_number: 1, tail_lines: 50 },
      resolveConfig({}, env),
      { fetch: fetchMock as typeof fetch },
    );
    expect(out).toContain("try_number=1");
    expect(out).toContain("KeyError: 'payment_intent_id'");
    expect(out).toContain("postgres://etl_user:[REDACTED:conn_password]@warehouse");
    expect(out).not.toContain("hunter2");
    expect(out).not.toContain("noise 100\n");
    expect(out).toContain("BEGIN LOG (untrusted data)");
  });

  it("turns API failures into a tool error", async () => {
    const fetchMock = vi.fn(async () => new Response("not found", { status: 404 }));
    const out = await getAirflowTaskLog({ dag_id: "d", task_id: "t", dag_run_id: "r", try_number: 1 }, resolveConfig({}, env), {
      fetch: fetchMock as typeof fetch,
    });
    expect(out).toMatch(/^Tool error: could not fetch log: Airflow API 404/);
  });

  it("errors when Airflow is not configured", async () => {
    expect(await getAirflowTaskLog({ dag_id: "d", task_id: "t", dag_run_id: "r" }, resolveConfig({}, {}))).toMatch(/^Tool error: Airflow is not configured/);
  });
});
