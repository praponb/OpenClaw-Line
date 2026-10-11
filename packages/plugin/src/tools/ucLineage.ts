import type { DatabricksClient } from "@openclaw-line/shared";

/**
 * Unity Catalog lineage for one job run: which tables each task writes, which
 * of them this run left stale, and what reads them further downstream.
 *
 * Lineage is recorded by Databricks from past successful reads/writes; it is
 * keyed by table and names the writing notebook, so tasks are matched to
 * tables through their notebook object ids. Needs only BROWSE on the catalog
 * (metadata) and CAN_READ on the job's notebooks; no data access.
 */

export type LineageTask = { taskKey: string; state: string; notebookPath?: string };

type TableInfo = { catalog_name?: string; schema_name?: string; name?: string };
type LineageEntity = {
  tableInfo?: TableInfo;
  fileInfo?: { path?: string };
  jobInfos?: { job_id?: number }[];
  notebookInfos?: { notebook_id?: number }[];
  [key: string]: unknown;
};
type TableLineage = { upstreams?: LineageEntity[] | null; downstreams?: LineageEntity[] | null };

const NOT_REFRESHED = new Set(["FAILED", "TIMEDOUT", "INTERNAL_ERROR", "CANCELED", "CANCELLED", "UPSTREAM_FAILED", "UPSTREAM_CANCELED", "SKIPPED"]);
const MAX_TABLES = 100;
const CONCURRENCY = 8;

const fullName = (t: TableInfo | undefined) => (t?.catalog_name && t.schema_name && t.name ? `${t.catalog_name}.${t.schema_name}.${t.name}` : undefined);

export async function buildLineageSection(client: Pick<DatabricksClient, "get">, opts: { jobId: number | undefined; catalogs: string[]; tasks: LineageTask[] }): Promise<string> {
  const header = `UNITY CATALOG LINEAGE (catalogs: ${opts.catalogs.join(", ") || "none"}; recorded by Databricks from past successful runs)`;
  if (!opts.catalogs.length) return `${header}\nunavailable: no catalog to inspect (set DATABRICKS_LINEAGE_CATALOGS or a "catalog" job parameter).`;
  try {
    // 1. Task -> notebook object id.
    const notebookOf = new Map<string, number>();
    await mapLimit(opts.tasks.filter((t) => t.notebookPath), CONCURRENCY, async (t) => {
      try {
        const st = await client.get<{ object_id?: number }>(`/api/2.0/workspace/get-status?path=${encodeURIComponent(t.notebookPath!)}`);
        if (st.object_id) notebookOf.set(t.taskKey, st.object_id);
      } catch {
        // no access to that notebook: the task just shows as unmapped
      }
    });

    // 2. Tables in the catalogs (metadata only, BROWSE is enough with include_browse).
    const tables: string[] = [];
    let truncated = false;
    for (const catalog of opts.catalogs) {
      const schemas = await client.get<{ schemas?: { name: string }[] }>(`/api/2.1/unity-catalog/schemas?catalog_name=${encodeURIComponent(catalog)}&include_browse=true`);
      for (const s of schemas.schemas ?? []) {
        if (s.name === "information_schema") continue;
        const res = await client.get<{ tables?: { full_name: string }[] }>(
          `/api/2.1/unity-catalog/tables?catalog_name=${encodeURIComponent(catalog)}&schema_name=${encodeURIComponent(s.name)}&omit_columns=true&include_browse=true&max_results=${MAX_TABLES}`,
        );
        for (const t of res.tables ?? []) {
          if (tables.length >= MAX_TABLES) truncated = true;
          else tables.push(t.full_name);
        }
      }
    }

    // 3. Lineage per table: who writes it, what reads it.
    const writers = new Map<string, Set<number>>();
    const downstream = new Map<string, Set<string>>();
    const external = new Map<string, Set<string>>();
    await mapLimit(tables, CONCURRENCY, async (table) => {
      const l = await client.get<TableLineage>(`/api/2.0/lineage-tracking/table-lineage?table_name=${encodeURIComponent(table)}&include_entity_lineage=true`);
      const w = new Set<number>();
      for (const up of l.upstreams ?? []) for (const n of up.notebookInfos ?? []) if (n.notebook_id) w.add(n.notebook_id);
      writers.set(table, w);
      const down = new Set<string>();
      const ext = new Set<string>();
      for (const d of l.downstreams ?? []) {
        const name = fullName(d.tableInfo);
        if (name) down.add(name);
        for (const j of d.jobInfos ?? []) if (j.job_id && j.job_id !== opts.jobId) ext.add(`job ${j.job_id}`);
        for (const [key, value] of Object.entries(d)) {
          if (!key.endsWith("Infos") || key === "jobInfos" || key === "notebookInfos" || !Array.isArray(value) || !value.length) continue;
          ext.add(`${key.replace(/Infos$/, "")} (${value.length})`);
        }
      }
      downstream.set(table, down);
      external.set(table, ext);
    });

    // 4. Task -> tables it writes; tables this run left stale; everything below them.
    const writes = new Map<string, string[]>();
    for (const t of opts.tasks) {
      const nb = notebookOf.get(t.taskKey);
      writes.set(t.taskKey, nb === undefined ? [] : tables.filter((tbl) => writers.get(tbl)?.has(nb)));
    }
    const stale = [...new Set(opts.tasks.filter((t) => NOT_REFRESHED.has(t.state)).flatMap((t) => writes.get(t.taskKey) ?? []))];
    const further: string[] = [];
    const seen = new Set(stale);
    const queue = [...stale];
    while (queue.length) {
      for (const next of downstream.get(queue.shift()!) ?? []) {
        if (seen.has(next)) continue;
        seen.add(next);
        further.push(next);
        queue.push(next);
      }
    }
    const consumers = [...new Set([...seen].flatMap((tbl) => [...(external.get(tbl) ?? [])].map((c) => `${c} reads ${tbl}`)))];

    const lines = [header];
    for (const t of opts.tasks) {
      const w = writes.get(t.taskKey) ?? [];
      const what = notebookOf.has(t.taskKey) ? (w.length ? w.join("; ") : "no tables recorded") : "not mapped (no notebook access or not a notebook task)";
      lines.push(`- ${t.taskKey} (${t.state}) writes: ${what}`);
    }
    lines.push(`Tables NOT refreshed by this run: ${stale.length ? stale.join("; ") : "none"}`);
    lines.push(`Further downstream tables: ${further.length ? further.join("; ") : "none"}`);
    lines.push(`Other consumers (other jobs, dashboards, queries): ${consumers.length ? consumers.join("; ") : "none recorded"}`);
    if (truncated) lines.push(`(only the first ${MAX_TABLES} tables were inspected)`);
    return lines.join("\n");
  } catch (err) {
    return `${header}\nunavailable: ${(err as Error).message}`;
  }
}

async function mapLimit<T>(items: T[], limit: number, fn: (item: T) => Promise<void>): Promise<void> {
  const queue = [...items];
  await Promise.all(
    Array.from({ length: Math.min(limit, queue.length) }, async () => {
      while (queue.length) await fn(queue.shift()!);
    }),
  );
}
