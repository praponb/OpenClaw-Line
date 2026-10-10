import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { resolveConfig } from "../src/config.js";
import { downstreamImpact, resolveNodeId, type DbtManifest } from "../src/lineage/manifest.js";
import { getDownstreamImpact } from "../src/tools/downstreamImpact.js";

const manifestPath = fileURLToPath(new URL("../../../demo/dbt/manifest.json", import.meta.url));
const lineageMapPath = fileURLToPath(new URL("../../../demo/dbt/lineage_map.json", import.meta.url));
const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as DbtManifest;
const cfg = resolveConfig({ dbtManifestPath: manifestPath, lineageMapPath }, {});

describe("resolveNodeId", () => {
  it.each([
    ["model.analytics.stg_stripe_transactions", "model.analytics.stg_stripe_transactions"],
    ["stg_stripe_transactions", "model.analytics.stg_stripe_transactions"],
    ["analytics.fct_daily_revenue", "model.analytics.fct_daily_revenue"],
    ["warehouse.marts.finance_reconciliation", "model.analytics.finance_reconciliation"],
    ["stripe.transactions", "source.analytics.stripe.transactions"],
  ])("%s -> %s", (input, expected) => {
    expect(resolveNodeId(manifest, input)).toMatchObject({ id: expected });
  });

  it("uses the lineage map for Airflow task ids", () => {
    expect(resolveNodeId(manifest, "extract_stripe_transactions", { extract_stripe_transactions: "source.analytics.stripe.transactions" })).toEqual({
      id: "source.analytics.stripe.transactions",
      via: "lineage map (extract_stripe_transactions)",
    });
  });

  it("reports unknown nodes instead of guessing", () => {
    expect(resolveNodeId(manifest, "does_not_exist")).toHaveProperty("error");
  });
});

describe("downstreamImpact", () => {
  it("walks child_map to models and exposures with criticality", () => {
    const r = downstreamImpact(manifest, "model.analytics.stg_stripe_transactions");
    expect(r.models.map((m) => m.uniqueId)).toEqual(["model.analytics.fct_daily_revenue", "model.analytics.finance_reconciliation"]);
    expect(r.models[0]!.critical).toContain("sla=06:00 UTC");
    expect(r.exposures.map((e) => e.name)).toEqual(["Executive Daily KPI (Looker)", "Daily Net Inflow Alert"]);
    expect(r.testCount).toBe(2);
    // Unrelated branches are not included.
    expect(r.exposures.map((e) => e.name)).not.toContain("Orders Dashboard");
  });
});

describe("get_downstream_impact tool", () => {
  it("formats blast radius for an Airflow task id", async () => {
    const text = await getDownstreamImpact({ node_id: "extract_stripe_transactions" }, cfg);
    expect(text).toContain("source.analytics.stripe.transactions");
    expect(text).toContain("model.analytics.fct_daily_revenue [analytics.fct_daily_revenue]");
    expect(text).toContain("CRITICAL(sla=06:00 UTC, tag=critical)");
    expect(text).toContain("Executive Daily KPI (Looker) type=dashboard owner=Finance BI");
    expect(text).not.toContain("finance-bi@example.com");
  });

  it("returns a tool error when not configured", async () => {
    expect(await getDownstreamImpact({ node_id: "x" }, resolveConfig({}, {}))).toMatch(/^Tool error: no dbt manifest/);
  });

  it("states unknown lineage explicitly", async () => {
    expect(await getDownstreamImpact({ node_id: "nope" }, cfg)).toMatch(/Lineage lookup failed: .*not found.*Downstream impact is unknown/);
  });
});
