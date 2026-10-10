import { readFile } from "node:fs/promises";
import { Type, type Static } from "typebox";
import { sanitize } from "@openclaw-line/shared";
import type { ResolvedConfig } from "../config.js";
import { downstreamImpact, ManifestStore, resolveNodeId } from "../lineage/manifest.js";
import { toolError } from "./result.js";

export const downstreamImpactParams = Type.Object({
  node_id: Type.String({
    description: "Target dbt model, table, or task identifier (e.g., model.analytics.stg_orders)",
  }),
});

export type DownstreamImpactParams = Static<typeof downstreamImpactParams>;

export const DOWNSTREAM_IMPACT_DESCRIPTION =
  "Query lineage metadata from dbt manifest artifacts or data catalog to determine impacted downstream nodes.";

const MAX_LISTED = 60;
const store = new ManifestStore();

export async function getDownstreamImpact(params: DownstreamImpactParams, cfg: ResolvedConfig): Promise<string> {
  if (!cfg.dbtManifestPath) return toolError("no dbt manifest configured (DBT_MANIFEST_PATH is unset).");
  try {
    const manifest = await store.load(cfg.dbtManifestPath);
    const lineageMap = await loadLineageMap(cfg.lineageMapPath);
    const resolved = resolveNodeId(manifest, params.node_id, lineageMap);
    if ("error" in resolved) return `Lineage lookup failed: ${resolved.error}. Downstream impact is unknown for this identifier.`;

    const impact = downstreamImpact(manifest, resolved.id);
    const lines = [
      `node: ${impact.rootId} [${impact.rootRelation}] (resolved via ${resolved.via})`,
      `manifest generated_at: ${manifest.metadata?.generated_at ?? "unknown"}`,
      "",
      `Downstream tables/models (${impact.models.length}):`,
      ...listOrNone(
        impact.models.map(
          (n) => `  - ${n.uniqueId} [${n.relation}] ${n.resourceType} depth=${n.depth}${n.critical ? ` CRITICAL(${n.critical})` : ""}`,
        ),
      ),
      "",
      `Downstream exposures - business reports/dashboards (${impact.exposures.length}):`,
      ...listOrNone(
        impact.exposures.map(
          (e) =>
            `  - ${e.name} type=${e.type}${e.owner ? ` owner=${e.owner}` : ""}${e.maturity ? ` maturity=${e.maturity}` : ""}` +
            `${e.url ? ` url=${e.url}` : ""} depth=${e.depth}${e.critical ? ` CRITICAL(${e.critical})` : ""}`,
        ),
      ),
      "",
      `Downstream data tests: ${impact.testCount}`,
    ];
    return sanitize(lines.join("\n"));
  } catch (err) {
    return toolError(`could not read lineage: ${sanitize((err as Error).message)}`);
  }
}

function listOrNone(items: string[]): string[] {
  if (!items.length) return ["  (none)"];
  if (items.length <= MAX_LISTED) return items;
  return [...items.slice(0, MAX_LISTED), `  ... and ${items.length - MAX_LISTED} more`];
}

async function loadLineageMap(path: string | undefined): Promise<Record<string, string>> {
  if (!path) return {};
  try {
    const parsed = JSON.parse(await readFile(path, "utf8")) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return Object.fromEntries(Object.entries(parsed).filter((e): e is [string, string] => typeof e[1] === "string"));
  } catch {
    return {};
  }
}
