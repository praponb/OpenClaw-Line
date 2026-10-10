/** Payload posted by the Airflow `on_failure_callback`. */
export type FailureEvent = {
  dag_id: string;
  task_id: string;
  run_id: string;
  try_number?: number;
  logical_date?: string;
  log_url?: string;
  exception?: string;
};

/** One deduplicated incident per (dag_id, run_id). */
export type Incident = {
  key: string;
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
  /** Sanitized, pre-filtered log excerpt for the root task. */
  logExcerpt?: string;
  logNote?: string;
};
