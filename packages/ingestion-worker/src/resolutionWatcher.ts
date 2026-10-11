import { sanitize, type AirflowClient } from "@openclaw-line/shared";
import type { FeedbackStore } from "./feedbackStore.js";

/**
 * MTTR without anyone pressing a button: an open incident counts as resolved
 * when a later run of its DAG finished with the failed task in state
 * "success". Resolution time = that run's end. Read-only (Airflow viewer).
 */
export class ResolutionWatcher {
  constructor(
    private readonly store: Pick<FeedbackStore, "unresolvedSince" | "recordResolution">,
    private readonly airflow: Pick<AirflowClient, "listDagRuns" | "getTaskInstance">,
    private readonly opts: { now: () => number; log: Pick<Console, "info" | "warn">; lookbackMs?: number },
  ) {}

  /** Returns how many incidents were resolved by this pass. */
  async checkOnce(): Promise<number> {
    const open = this.store.unresolvedSince(this.opts.now() - (this.opts.lookbackMs ?? 7 * 24 * 3600 * 1000));
    const byDag = new Map<string, typeof open>();
    for (const i of open) byDag.set(i.dag_id, [...(byDag.get(i.dag_id) ?? []), i]);
    let resolved = 0;
    for (const [dagId, incidents] of byDag) {
      let runs;
      try {
        runs = await this.airflow.listDagRuns(dagId, 25);
      } catch (err) {
        this.opts.log.warn(`[mttr] could not list runs of ${dagId}: ${sanitize((err as Error).message)}`);
        continue;
      }
      // Oldest first, so the first qualifying run is the one that fixed it.
      const finished = runs
        .filter((r) => r.state === "success" && r.start_date && r.end_date)
        .sort((a, b) => Date.parse(a.start_date!) - Date.parse(b.start_date!));
      for (const incident of incidents) {
        const since = incident.first_seen_at ?? incident.dispatched_at ?? 0;
        for (const run of finished) {
          if (run.dag_run_id === incident.run_id || Date.parse(run.start_date!) < since) continue;
          try {
            const ti = await this.airflow.getTaskInstance(dagId, run.dag_run_id, incident.task_id);
            if (ti.state !== "success") continue;
          } catch {
            continue; // task not in that run (DAG changed): not evidence of a fix
          }
          if (this.store.recordResolution(incident.ref, Date.parse(run.end_date!), "next_success", run.dag_run_id)) {
            resolved++;
            this.opts.log.info(`[mttr] ${incident.ref}: resolved by successful run ${run.dag_run_id}`);
          }
          break;
        }
      }
    }
    return resolved;
  }
}
