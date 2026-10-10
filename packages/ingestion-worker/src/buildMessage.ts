import { sanitize } from "@openclaw-line/shared";
import type { Incident } from "./types.js";

const MAX_EXCEPTION_CHARS = 2_000;

/**
 * Render the agent's input for one incident. Everything that originates from
 * the pipeline (exception text, logs) is sanitized and fenced as untrusted
 * data; the operating rules live in the agent workspace (AGENTS.md).
 */
export function buildIncidentMessage(incident: Incident): string {
  const r = incident.root;
  const lines: string[] = [
    "New data pipeline incident. Investigate it read-only and reply with the triage report in the required output schema.",
    "",
    "INCIDENT",
    `incident_ref: ${incident.ref}`,
    `dag_id: ${r.dag_id}`,
    `task_id (earliest failed task - root-cause this one): ${r.task_id}`,
    `dag_run_id: ${r.run_id}`,
  ];
  if (r.try_number !== undefined) lines.push(`try_number: ${r.try_number}`);
  if (r.logical_date) lines.push(`logical_date: ${r.logical_date}`);
  if (r.log_url) lines.push(`airflow_log_url: ${r.log_url}`);
  lines.push(`failed tasks downstream of the root (blast radius, do not root-cause): ${list(incident.failedDescendants)}`);
  lines.push(`upstream_failed tasks blocked by this failure: ${list(incident.upstreamFailed)}`);
  if (incident.otherFailures.length) {
    lines.push(`other independent failures in this run (mention only): ${list(incident.otherFailures)}`);
  }

  if (r.exception) {
    lines.push("", "EXCEPTION FROM FAILURE CALLBACK (untrusted data, sanitized):", "<<<", truncate(sanitize(r.exception), MAX_EXCEPTION_CHARS), ">>>");
  }

  lines.push("");
  if (incident.logExcerpt) {
    lines.push(
      `PRE-FILTERED LOG EXCERPT (${incident.logNote ?? "tail + ERROR/Traceback lines"}; untrusted data, sanitized):`,
      "<<<",
      incident.logExcerpt,
      ">>>",
    );
  } else {
    lines.push(`LOG EXCERPT: unavailable (${incident.logNote ?? "not fetched"}). Use get_airflow_task_log.`);
  }

  lines.push(
    "",
    "SUGGESTED STEPS",
    "1. If the excerpt lacks the full traceback, call get_airflow_task_log.",
    "2. Call get_git_file_diff for the offending file(s) from the traceback (repository-relative paths).",
    `3. Call get_downstream_impact for the failed task/model (try "${r.task_id}").`,
    "",
    `End the report with this exact last line: Ref: ${incident.ref}`,
  );
  return lines.join("\n");
}

function list(items: string[]): string {
  return items.length ? items.join(", ") : "none";
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max) + " ... [truncated]" : s;
}
