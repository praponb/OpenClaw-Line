/** Payload posted by the Airflow `on_failure_callback`. */
export type FailureEvent = {
  dag_id: string;
  task_id: string;
  run_id: string;
  try_number?: number;
  logical_date?: string;
  log_url?: string;
  /** Log file path on the Airflow worker (spec field); log_url stays for the UI link. */
  log_filepath?: string;
  exception?: string;
  /** Set when the failed task ran a Databricks job (DatabricksRunNowOperator XCom). */
  databricks_run_id?: string;
  databricks_run_url?: string;
  /** When the worker received the callback (ms epoch); set by the worker, not the payload. */
  receivedAt?: number;
};

/** One deduplicated incident per (dag_id, run_id). */
export type Incident = {
  key: string;
  /** Short human-facing reference (INC-XXXXXX), shown in the report and feedback card. */
  ref: string;
  dagId: string;
  runId: string;
  /** Earliest failed task: the only one we root-cause. */
  root: FailureEvent;
  /** Failed tasks downstream of the root (blast radius, not root-caused). */
  failedDescendants: string[];
  /** Tasks Airflow marked upstream_failed because of the failure. */
  upstreamFailed: string[];
  /** Other failed tasks that do not depend on the root (independent failures). */
  otherFailures: string[];
  /** Root task timing from Airflow's task instance (execution time for the summary line). */
  rootStartedAt?: string;
  rootDurationSeconds?: number;
  /** Sanitized, pre-filtered log excerpt for the root task. */
  logExcerpt?: string;
  logNote?: string;
};
