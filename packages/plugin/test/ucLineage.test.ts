import { describe, expect, it, vi } from "vitest";
import { buildLineageSection } from "../src/tools/ucLineage.js";

const JOB = 954973835355130;
const job = [{ job_id: JOB }];
// Mirrors the real lakehouse lineage: landing volume -> bronze -> silver -> gold,
// plus another job's report table reading gold.
const lineage: Record<string, unknown> = {
  "lakehouse.bronze.stripe_events": {
    upstreams: [{ fileInfo: { path: "/Volumes/lakehouse/bronze/landing/stripe_events/" }, jobInfos: job, notebookInfos: [{ notebook_id: 275 }] }],
    downstreams: [{ tableInfo: { catalog_name: "lakehouse", schema_name: "silver", name: "stripe_transactions" }, jobInfos: job, notebookInfos: [{ notebook_id: 276 }] }],
  },
  "lakehouse.silver.stripe_transactions": {
    upstreams: [{ tableInfo: { catalog_name: "lakehouse", schema_name: "bronze", name: "stripe_events" }, jobInfos: job, notebookInfos: [{ notebook_id: 276 }] }],
    downstreams: [{ tableInfo: { catalog_name: "lakehouse", schema_name: "gold", name: "daily_revenue" }, jobInfos: job, notebookInfos: [{ notebook_id: 271 }] }],
  },
  "lakehouse.gold.daily_revenue": {
    upstreams: [{ tableInfo: { catalog_name: "lakehouse", schema_name: "silver", name: "stripe_transactions" }, jobInfos: job, notebookInfos: [{ notebook_id: 271 }] }],
    downstreams: [
      { tableInfo: { catalog_name: "lakehouse", schema_name: "gold", name: "finance_report" }, jobInfos: [{ job_id: 42 }], notebookInfos: [{ notebook_id: 900 }] },
      { lakeviewDashboardInfos: [{ dashboard_id: "abc" }] },
    ],
  },
  "lakehouse.gold.finance_report": { upstreams: [{ tableInfo: { catalog_name: "lakehouse", schema_name: "gold", name: "daily_revenue" }, jobInfos: [{ job_id: 42 }], notebookInfos: [{ notebook_id: 900 }] }], downstreams: null },
};
const notebooks: Record<string, number> = { "/b/src/01_land": 274, "/b/src/02_bronze": 275, "/b/src/03_silver": 276, "/b/src/04_gold": 271 };

function client() {
  return {
    get: vi.fn(async (path: string) => {
      const u = new URL(path, "https://x");
      if (u.pathname === "/api/2.0/workspace/get-status") {
        const id = notebooks[u.searchParams.get("path")!];
        if (!id) throw new Error("Databricks API 404");
        return { object_id: id };
      }
      if (u.pathname === "/api/2.1/unity-catalog/schemas") {
        expect(u.searchParams.get("include_browse")).toBe("true");
        return { schemas: [{ name: "bronze" }, { name: "gold" }, { name: "information_schema" }, { name: "silver" }] };
      }
      if (u.pathname === "/api/2.1/unity-catalog/tables") {
        const schema = u.searchParams.get("schema_name");
        if (schema === "information_schema") throw new Error("should be skipped");
        return { tables: Object.keys(lineage).filter((t) => t.split(".")[1] === schema).map((full_name) => ({ full_name })) };
      }
      if (u.pathname === "/api/2.0/lineage-tracking/table-lineage") return lineage[u.searchParams.get("table_name")!];
      throw new Error(`unexpected ${path}`);
    }),
  };
}

const tasks = [
  { taskKey: "land_stripe_events", state: "SUCCESS", notebookPath: "/b/src/01_land" },
  { taskKey: "bronze_ingest", state: "SUCCESS", notebookPath: "/b/src/02_bronze" },
  { taskKey: "silver_transactions", state: "FAILED", notebookPath: "/b/src/03_silver" },
  { taskKey: "gold_daily_revenue", state: "UPSTREAM_FAILED", notebookPath: "/b/src/04_gold" },
];

describe("Unity Catalog lineage section", () => {
  it("maps tasks to the tables they write and lists what this run left stale", async () => {
    const out = await buildLineageSection(client(), { jobId: JOB, catalogs: ["lakehouse"], tasks });
    expect(out).toContain("- land_stripe_events (SUCCESS) writes: no tables recorded");
    expect(out).toContain("- bronze_ingest (SUCCESS) writes: lakehouse.bronze.stripe_events");
    expect(out).toContain("- silver_transactions (FAILED) writes: lakehouse.silver.stripe_transactions");
    expect(out).toContain("- gold_daily_revenue (UPSTREAM_FAILED) writes: lakehouse.gold.daily_revenue");
    expect(out).toContain("Tables NOT refreshed by this run: lakehouse.silver.stripe_transactions; lakehouse.gold.daily_revenue");
    expect(out).toContain("Further downstream tables: lakehouse.gold.finance_report");
    expect(out).toContain("job 42 reads lakehouse.gold.daily_revenue");
    expect(out).toContain("lakeviewDashboard (1) reads lakehouse.gold.daily_revenue");
  });

  it("marks tasks it cannot map and reports none stale when nothing failed", async () => {
    const out = await buildLineageSection(client(), {
      jobId: JOB,
      catalogs: ["lakehouse"],
      tasks: [{ taskKey: "bronze_ingest", state: "SUCCESS", notebookPath: "/b/src/02_bronze" }, { taskKey: "py_task", state: "SUCCESS" }],
    });
    expect(out).toContain("- py_task (SUCCESS) writes: not mapped");
    expect(out).toContain("Tables NOT refreshed by this run: none");
  });

  it("degrades to 'unavailable' instead of failing the tool", async () => {
    const c = { get: vi.fn(async () => { throw new Error("Databricks API 403 on /api/2.1/unity-catalog/schemas: no BROWSE"); }) };
    expect(await buildLineageSection(c, { jobId: JOB, catalogs: ["lakehouse"], tasks })).toContain("unavailable: Databricks API 403");
    expect(await buildLineageSection(c, { jobId: JOB, catalogs: [], tasks })).toContain("unavailable: no catalog to inspect");
  });
});
