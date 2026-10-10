import { readFile, stat } from "node:fs/promises";

/** The subset of a dbt `manifest.json` used for lineage. */
export type DbtNode = {
  unique_id?: string;
  resource_type: string;
  name: string;
  schema?: string;
  database?: string;
  alias?: string;
  relation_name?: string | null;
  source_name?: string;
  identifier?: string;
  tags?: string[];
  meta?: Record<string, unknown>;
  config?: { meta?: Record<string, unknown>; tags?: string[] };
};

export type DbtExposure = {
  name: string;
  label?: string;
  type?: string;
  maturity?: string;
  url?: string;
  owner?: { name?: string; email?: string };
  tags?: string[];
  meta?: Record<string, unknown>;
};

export type DbtManifest = {
  metadata?: { generated_at?: string; dbt_version?: string; project_name?: string };
  nodes: Record<string, DbtNode>;
  sources?: Record<string, DbtNode>;
  exposures?: Record<string, DbtExposure>;
  child_map: Record<string, string[]>;
};

export type ImpactedNode = {
  uniqueId: string;
  resourceType: string;
  relation: string;
  depth: number;
  critical?: string;
};

export type ImpactedExposure = {
  uniqueId: string;
  name: string;
  type: string;
  owner?: string;
  url?: string;
  maturity?: string;
  depth: number;
  critical?: string;
};

export type ImpactResult = {
  rootId: string;
  rootRelation: string;
  models: ImpactedNode[];
  exposures: ImpactedExposure[];
  testCount: number;
};

export type ResolveResult = { id: string; via: string } | { error: string };

const ASSET_TYPES = new Set(["model", "snapshot", "seed", "semantic_model", "metric", "saved_query"]);

/** Load a manifest, cached by path + mtime (manifests can be tens of MB). */
export class ManifestStore {
  private cache?: { path: string; mtimeMs: number; manifest: DbtManifest };

  async load(path: string): Promise<DbtManifest> {
    const { mtimeMs } = await stat(path);
    if (this.cache && this.cache.path === path && this.cache.mtimeMs === mtimeMs) return this.cache.manifest;
    const manifest = JSON.parse(await readFile(path, "utf8")) as DbtManifest;
    if (!manifest.nodes || !manifest.child_map) throw new Error(`${path} is not a compiled dbt manifest (missing nodes/child_map)`);
    this.cache = { path, mtimeMs, manifest };
    return manifest;
  }
}

/**
 * Resolve a user/agent supplied identifier to a manifest unique_id. Accepts:
 * exact unique_id, Airflow task id via lineage map, `schema.table` relation,
 * `source_name.table` for sources, or a bare model name.
 */
export function resolveNodeId(m: DbtManifest, input: string, lineageMap: Record<string, string> = {}): ResolveResult {
  const q = input.trim();
  if (!q) return { error: "node_id is empty" };
  if (m.nodes[q] || m.sources?.[q] || m.exposures?.[q]) return { id: q, via: "unique_id" };

  const mapped = lineageMap[q];
  if (mapped && (m.nodes[mapped] || m.sources?.[mapped])) return { id: mapped, via: `lineage map (${q})` };

  const lower = q.toLowerCase().replace(/["`]/g, "");
  const candidates = new Set<string>();
  for (const [id, n] of Object.entries(m.nodes)) {
    if (!ASSET_TYPES.has(n.resource_type)) continue;
    if (relationKeys(n).has(lower)) candidates.add(id);
  }
  for (const [id, s] of Object.entries(m.sources ?? {})) {
    if (relationKeys(s).has(lower) || `${s.source_name}.${s.name}`.toLowerCase() === lower) candidates.add(id);
  }
  if (candidates.size === 1) return { id: [...candidates][0]!, via: "name match" };
  if (candidates.size > 1) {
    return { error: `"${q}" is ambiguous; candidates: ${[...candidates].slice(0, 10).join(", ")}` };
  }
  return { error: `"${q}" was not found in the dbt manifest (no matching unique_id, relation, or model name)` };
}

export function downstreamImpact(m: DbtManifest, rootId: string, maxNodes = 500): ImpactResult {
  const depthOf = new Map<string, number>([[rootId, 0]]);
  const queue = [rootId];
  while (queue.length && depthOf.size <= maxNodes) {
    const id = queue.shift()!;
    for (const child of m.child_map[id] ?? []) {
      if (depthOf.has(child)) continue;
      depthOf.set(child, depthOf.get(id)! + 1);
      queue.push(child);
    }
  }
  depthOf.delete(rootId);

  const models: ImpactedNode[] = [];
  const exposures: ImpactedExposure[] = [];
  let testCount = 0;
  for (const [id, depth] of depthOf) {
    const node = m.nodes[id];
    if (node) {
      if (node.resource_type === "test" || node.resource_type === "unit_test") {
        testCount++;
      } else if (ASSET_TYPES.has(node.resource_type)) {
        models.push({ uniqueId: id, resourceType: node.resource_type, relation: relationOf(node), depth, critical: criticality(node) });
      }
      continue;
    }
    const exp = m.exposures?.[id];
    if (exp) {
      exposures.push({
        uniqueId: id,
        name: exp.label ?? exp.name,
        type: exp.type ?? "exposure",
        owner: exp.owner?.name,
        url: exp.url,
        maturity: exp.maturity,
        depth,
        critical: criticality(exp),
      });
    }
  }
  const byDepth = (a: { depth: number; uniqueId: string }, b: { depth: number; uniqueId: string }) =>
    a.depth - b.depth || a.uniqueId.localeCompare(b.uniqueId);
  models.sort(byDepth);
  exposures.sort(byDepth);

  const root = m.nodes[rootId] ?? m.sources?.[rootId];
  return { rootId, rootRelation: root ? relationOf(root) : rootId, models, exposures, testCount };
}

function relationOf(n: DbtNode): string {
  return `${n.schema ?? "?"}.${n.alias ?? n.identifier ?? n.name}`;
}

function relationKeys(n: DbtNode): Set<string> {
  const table = (n.alias ?? n.identifier ?? n.name).toLowerCase();
  const keys = new Set([n.name.toLowerCase(), table]);
  if (n.schema) keys.add(`${n.schema.toLowerCase()}.${table}`);
  if (n.database && n.schema) keys.add(`${n.database.toLowerCase()}.${n.schema.toLowerCase()}.${table}`);
  if (n.relation_name) keys.add(n.relation_name.toLowerCase().replace(/["`]/g, ""));
  return keys;
}

/** Returns a short reason when a node is SLA/business critical, per meta or tags. */
function criticality(n: { meta?: Record<string, unknown>; tags?: string[]; config?: { meta?: Record<string, unknown>; tags?: string[] } }): string | undefined {
  const meta = { ...(n.config?.meta ?? {}), ...(n.meta ?? {}) };
  const tags = [...(n.tags ?? []), ...(n.config?.tags ?? [])].map((t) => t.toLowerCase());
  const parts: string[] = [];
  if (meta.sla !== undefined) parts.push(`sla=${String(meta.sla)}`);
  if (meta.criticality !== undefined) parts.push(`criticality=${String(meta.criticality)}`);
  for (const tag of ["critical", "sla"]) if (tags.includes(tag)) parts.push(`tag=${tag}`);
  return parts.length ? parts.join(", ") : undefined;
}
