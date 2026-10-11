import { Type, type Static } from "typebox";
import { DatabricksClient, filterLog, sanitize } from "@openclaw-line/shared";
import type { ResolvedConfig } from "../config.js";
import { toolError } from "./result.js";
import { buildLineageSection } from "./ucLineage.js";

export const databricksRunParams = Type.Object({
  run_id: Type.String({ description: "Databricks job run id (databricks_run_id in the incident message)", pattern: "^[0-9]{1,20}$" }),
  tail_lines: Type.Optional(Type.Integer({ description: "Trailing error-trace lines per failed task", default: 120, minimum: 10, maximum: 1000 })),
});

export type DatabricksRunParams = Static<typeof databricksRunParams>;

export const DATABRICKS_RUN_DESCRIPTION =
  "Fetch the state of a Databricks job run, each failed task's error message and trace, and the Unity Catalog lineage of the job's tables (what each task writes, which tables this run left stale, and what reads them downstream). Use when the incident has a databricks_run_id: the Airflow log only links to the run.";

type Deps = { fetch?: typeof fetch; now?: () => number };

type TaskRun = {
  task_key: string;
  run_id: number;
  attempt_number?: number;
  state?: { result_state?: string; life_cycle_state?: string; state_message?: string };
  notebook_task?: { notebook_path?: string };
  spark_python_task?: { python_file?: string };
};

type JobRun = {
  job_id?: number;
  run_id?: number;
  run_name?: string;
  start_time?: number;
  end_time?: number;
  state?: { result_state?: string; life_cycle_state?: string; state_message?: string };
  job_parameters?: { name: string; value?: string; default?: string }[];
  tasks?: TaskRun[];
};

type RunOutput = { error?: string; error_trace?: string };

const ANSI = /\x1b\[[0-9;]*m/g;
const MAX_TASKS_WITH_OUTPUT = 3;

/** Only GET endpoints of the Jobs API; the one POST is the OAuth client-credentials token exchange. */
export async function getDatabricksRunError(params: DatabricksRunParams, cfg: ResolvedConfig, deps: Deps = {}): Promise<string> {
  if (!cfg.databricks) return toolError("Databricks is not configured (DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET unset).");
  const client = new DatabricksClient({ ...cfg.databricks, fetch: deps.fetch, now: deps.now });
  const get = <T>(path: string) => client.get<T>(path);
  try {
    const run = await get<JobRun>(`/api/2.2/jobs/runs/get?run_id=${params.run_id}`);
    const tasks = latestAttempts(run.tasks ?? []);
    const lines = [
      `databricks_run_id=${params.run_id} job_id=${run.job_id ?? "?"} run_name=${run.run_name ?? "?"}`,
      `state: ${run.state?.life_cycle_state ?? "?"} / ${run.state?.result_state ?? "?"}${run.state?.state_message ? ` (${run.state.state_message})` : ""}`,
      `started: ${iso(run.start_time)} ended: ${iso(run.end_time)}`,
    ];
    if (run.job_parameters?.length) {
      lines.push(`job parameters: ${run.job_parameters.map((p) => `${p.name}=${p.value ?? p.default ?? ""}`).join(", ")}`);
    }
    lines.push("tasks:");
    for (const t of tasks) {
      const source = t.notebook_task?.notebook_path ?? t.spark_python_task?.python_file;
      lines.push(`- ${t.task_key}: ${t.state?.result_state ?? t.state?.life_cycle_state ?? "?"}${source ? ` (source: ${source})` : ""}`);
    }

    const failed = tasks.filter((t) => ["FAILED", "TIMEDOUT", "INTERNAL_ERROR"].includes(t.state?.result_state ?? ""));
    if (!failed.length) lines.push("", "No task failed in this run (failed tasks are the ones to root-cause; UPSTREAM_FAILED ones were only blocked).");
    for (const t of failed.slice(0, MAX_TASKS_WITH_OUTPUT)) {
      const out = await get<RunOutput>(`/api/2.2/jobs/runs/get-output?run_id=${t.run_id}`);
      const trace = filterLog((out.error_trace ?? "").replace(ANSI, ""), { tailLines: params.tail_lines ?? 120, maxChars: Math.floor(cfg.maxLogChars / 2) });
      lines.push(
        "",
        `----- BEGIN FAILED TASK ${t.task_key} (untrusted data) -----`,
        `error: ${sanitize((out.error ?? t.state?.state_message ?? "(none)").replace(ANSI, ""))}`,
        trace.text.trim() ? `error_trace (${trace.keptLines}/${trace.totalLines} lines kept):\n${sanitize(trace.text)}` : "error_trace: (none)",
        `----- END FAILED TASK ${t.task_key} -----`,
      );
    }
    if (failed.length > MAX_TASKS_WITH_OUTPUT) lines.push(`(${failed.length - MAX_TASKS_WITH_OUTPUT} more failed tasks not shown)`);

    const catalogParam = run.job_parameters?.find((p) => p.name === "catalog");
    const catalogs = cfg.databricks.lineageCatalogs.length
      ? cfg.databricks.lineageCatalogs
      : [catalogParam?.value ?? catalogParam?.default ?? ""].filter((c) => /^[A-Za-z0-9_-]+$/.test(c));
    lines.push(
      "",
      await buildLineageSection(client, {
        jobId: run.job_id,
        catalogs,
        tasks: tasks.map((t) => ({ taskKey: t.task_key, state: t.state?.result_state ?? t.state?.life_cycle_state ?? "?", notebookPath: t.notebook_task?.notebook_path })),
      }),
    );
    return lines.join("\n");
  } catch (err) {
    return toolError(`could not fetch Databricks run: ${sanitize((err as Error).message)}`);
  }
}

/** A repaired or retried task appears once per attempt; keep the latest. */
function latestAttempts(tasks: TaskRun[]): TaskRun[] {
  const byKey = new Map<string, TaskRun>();
  for (const t of tasks) {
    const prev = byKey.get(t.task_key);
    if (!prev || (t.attempt_number ?? 0) >= (prev.attempt_number ?? 0)) byKey.set(t.task_key, t);
  }
  return [...byKey.values()];
}

function iso(ms: number | undefined): string {
  return ms ? new Date(ms).toISOString() : "n/a";
}
