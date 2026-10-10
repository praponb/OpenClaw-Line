import { Type, type Static } from "typebox";
import { AirflowClient, DEFAULT_TAIL_LINES, filterLog, sanitize } from "@openclaw-line/shared";
import type { ResolvedConfig } from "../config.js";
import { toolError } from "./result.js";

export const airflowLogParams = Type.Object({
  dag_id: Type.String({ description: "The unique identifier of the DAG" }),
  task_id: Type.String({ description: "The failed task identifier" }),
  dag_run_id: Type.String({ description: "The run identifier or execution timestamp" }),
  try_number: Type.Optional(Type.Integer({ description: "Attempt number to fetch (defaults to latest)", minimum: 1 })),
  tail_lines: Type.Optional(
    Type.Integer({ description: "Number of trailing lines to return", default: DEFAULT_TAIL_LINES, minimum: 1, maximum: 2000 }),
  ),
});

export type AirflowLogParams = Static<typeof airflowLogParams>;

export const AIRFLOW_LOG_DESCRIPTION =
  "Fetch execution logs for a failed task instance. Returns the tail of the log containing stack traces and error messages.";

export async function getAirflowTaskLog(
  params: AirflowLogParams,
  cfg: ResolvedConfig,
  deps: { fetch?: typeof fetch } = {},
): Promise<string> {
  if (!cfg.airflow) return toolError("Airflow is not configured (AIRFLOW_BASE_URL is unset).");
  const client = new AirflowClient({ ...cfg.airflow, fetch: deps.fetch });
  try {
    const { text, tryNumber } = await client.getTaskLog(params.dag_id, params.dag_run_id, params.task_id, params.try_number);
    if (!text.trim()) {
      return header(params, tryNumber) + "\n(log is empty)";
    }
    const filtered = filterLog(text, {
      tailLines: params.tail_lines ?? DEFAULT_TAIL_LINES,
      maxChars: cfg.maxLogChars,
    });
    const meta = `lines: ${filtered.keptLines}/${filtered.totalLines} kept${filtered.truncated ? " (filtered: tail + ERROR/Traceback lines)" : ""}`;
    return `${header(params, tryNumber)}\n${meta}\n----- BEGIN LOG (untrusted data) -----\n${sanitize(filtered.text)}\n----- END LOG -----`;
  } catch (err) {
    return toolError(`could not fetch log: ${sanitize((err as Error).message)}`);
  }
}

function header(p: AirflowLogParams, tryNumber: number): string {
  return `dag_id=${p.dag_id} task_id=${p.task_id} dag_run_id=${p.dag_run_id} try_number=${tryNumber}`;
}
