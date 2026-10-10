import { describe, expect, it, vi } from "vitest";
import { AirflowClient, flattenLogContent } from "../src/airflowClient.js";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

describe("AirflowClient", () => {
  it("v2: exchanges a token and fetches the latest try's log with encoded run id", async () => {
    const fetchMock = vi.fn(async (url: string | URL | Request, _init?: RequestInit) => {
      const u = String(url);
      if (u.endsWith("/auth/token")) return jsonResponse({ access_token: "tok" }, 201);
      if (u.endsWith("/taskInstances/extract")) return jsonResponse({ task_id: "extract", try_number: 2, state: "failed" });
      if (u.includes("/logs/2")) return jsonResponse({ content: "line1\nline2" });
      return jsonResponse({}, 404);
    });
    const c = new AirflowClient({ baseUrl: "http://af:8080/", username: "u", password: "p", fetch: fetchMock as typeof fetch });
    const r = await c.getTaskLog("dag", "scheduled__2026-10-10T02:00:00+00:00", "extract");
    expect(r).toEqual({ text: "line1\nline2", tryNumber: 2 });
    const logCall = fetchMock.mock.calls.find((c) => String(c[0]).includes("/logs/"))!;
    expect(String(logCall[0])).toBe(
      "http://af:8080/api/v2/dags/dag/dagRuns/scheduled__2026-10-10T02%3A00%3A00%2B00%3A00/taskInstances/extract/logs/2?full_content=true",
    );
    expect((logCall[1]!.headers as Record<string, string>).Authorization).toBe("Bearer tok");
    expect(logCall[1]!.method).toBe("GET");
    // Every call other than the token exchange is a GET.
    for (const [u, init] of fetchMock.mock.calls) if (!String(u).endsWith("/auth/token")) expect(init!.method).toBe("GET");
  });

  it("v1: uses basic auth and the explicit try number", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ content: "x" }));
    const c = new AirflowClient({ baseUrl: "http://af", username: "u", password: "p", apiVersion: "v1", fetch: fetchMock as typeof fetch });
    await c.getTaskLog("d", "r", "t", 3);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("http://af/api/v1/dags/d/dagRuns/r/taskInstances/t/logs/3?full_content=true");
    expect((init.headers as Record<string, string>).Authorization).toBe("Basic " + Buffer.from("u:p").toString("base64"));
  });

  it("raises a descriptive error on API failure", async () => {
    const fetchMock = vi.fn(async () => new Response("nope", { status: 403 }));
    const c = new AirflowClient({ baseUrl: "http://af", username: "u", password: "p", apiVersion: "v1", fetch: fetchMock as typeof fetch });
    await expect(c.listTaskInstances("d", "r")).rejects.toThrow(/Airflow API 403/);
  });
});

describe("flattenLogContent", () => {
  it("renders Airflow 3 structured logs including tracebacks", () => {
    const text = flattenLogContent([
      { event: "::group::Log message source details", sources: ["/x"] },
      { timestamp: "2026-10-10T02:05:01Z", level: "info", event: "Loaded 3 Stripe events" },
      {
        timestamp: "2026-10-10T02:05:02Z",
        level: "error",
        event: "Task failed with exception",
        error_detail: [
          {
            exc_type: "KeyError",
            exc_value: "'payment_intent_id'",
            frames: [{ filename: "/opt/airflow/dags/stripe_ingest/extract_stripe.py", lineno: 36, name: "to_staging_row" }],
          },
        ],
      },
    ]);
    expect(text).toContain("[2026-10-10T02:05:01Z] INFO - Loaded 3 Stripe events");
    expect(text).toContain('File "/opt/airflow/dags/stripe_ingest/extract_stripe.py", line 36, in to_staging_row');
    expect(text).toContain("KeyError: 'payment_intent_id'");
  });
});
