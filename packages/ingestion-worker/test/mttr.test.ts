import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { buildDashboardData } from "../src/dashboard.js";
import { FeedbackStore } from "../src/feedbackStore.js";
import { renderFeedbackReport } from "../src/feedbackReport.js";
import { ResolutionWatcher } from "../src/resolutionWatcher.js";

const H = 3600_000;
const iso = (ms: number) => new Date(ms).toISOString();

function store() {
  const s = new FeedbackStore(":memory:");
  s.recordDispatch({ ref: "INC-AAAAAA", incidentKey: "d:r1", dagId: "d", taskId: "t", runId: "r1", firstSeenAt: 10 * H, dispatchedAt: 10 * H + 30_000 });
  s.recordDispatch({ ref: "INC-BBBBBB", incidentKey: "e:r1", dagId: "e", taskId: "t", runId: "r1", firstSeenAt: 11 * H, dispatchedAt: 11 * H + 30_000 });
  return s;
}

describe("ResolutionWatcher", () => {
  it("resolves an incident at the end of the first later run where the failed task succeeded", async () => {
    const s = store();
    const airflow = {
      listDagRuns: vi.fn(async (dagId: string) =>
        dagId === "d"
          ? [
              { dag_run_id: "r4", state: "success", start_date: iso(14 * H), end_date: iso(14 * H + 60_000) },
              { dag_run_id: "r3", state: "success", start_date: iso(12 * H), end_date: iso(12 * H + 90_000) }, // task skipped here
              { dag_run_id: "r2", state: "failed", start_date: iso(11 * H), end_date: iso(11 * H + 60_000) },
              { dag_run_id: "r0", state: "success", start_date: iso(9 * H), end_date: iso(9 * H + 60_000) }, // before the failure
            ]
          : [{ dag_run_id: "x", state: "running", start_date: iso(12 * H), end_date: null }],
      ),
      getTaskInstance: vi.fn(async (_d: string, runId: string) => ({ task_id: "t", state: runId === "r3" ? "skipped" : "success", try_number: 1, start_date: null, end_date: null })),
    };
    const w = new ResolutionWatcher(s, airflow, { now: () => 15 * H, log: { info: vi.fn(), warn: vi.fn() } });
    expect(await w.checkOnce()).toBe(1);
    expect(s.getIncident("INC-AAAAAA")).toMatchObject({ resolved_at: 14 * H + 60_000, resolution: "next_success", resolved_by: "r4" });
    expect(s.getIncident("INC-BBBBBB")!.resolved_at).toBeNull();
    expect(airflow.getTaskInstance).not.toHaveBeenCalledWith("d", "r0", "t");
    expect(await w.checkOnce()).toBe(0); // already resolved: not checked again
  });

  it("keeps going when Airflow is unreachable for one DAG", async () => {
    const s = store();
    const log = { info: vi.fn(), warn: vi.fn() };
    const airflow = { listDagRuns: vi.fn(async () => { throw new Error("Airflow API 503"); }), getTaskInstance: vi.fn() };
    expect(await new ResolutionWatcher(s, airflow, { now: () => 15 * H, log }).checkOnce()).toBe(0);
    expect(log.warn).toHaveBeenCalledTimes(2);
  });
});

describe("MTTR reporting", () => {
  it("dashboard and feedback report show median MTTR, how incidents were resolved, and open ones", () => {
    const s = store();
    s.recordResolution("INC-AAAAAA", 10 * H + 45 * 60_000, "button", "u:1");
    expect(s.recordResolution("INC-AAAAAA", 13 * H, "next_success", "r9")).toBe(false); // first wins
    const d = buildDashboardData(s.incidentsSince(0), s.feedbackSince(0), { days: 30, now: 15 * H });
    expect(d.mttr).toEqual({ medianSeconds: 2700, p90Seconds: 2700, resolved: 1, open: 1 });
    expect(d.incidents.find((i) => i.ref === "INC-AAAAAA")).toMatchObject({ resolution: "button", resolveSeconds: 2700 });
    expect(renderFeedbackReport(s.incidentsSince(0), [], { days: 30 })).toContain("(1 resolved: 1 by button, 0 by next successful run; 1 open)");
  });

  it("adds the MTTR columns to an existing database", () => {
    const dir = mkdtempSync(join(tmpdir(), "oc-mttr-"));
    try {
      const path = join(dir, "feedback.db");
      const old = new DatabaseSync(path);
      old.exec(`CREATE TABLE incidents (ref TEXT PRIMARY KEY, incident_key TEXT NOT NULL, dag_id TEXT NOT NULL, task_id TEXT NOT NULL, run_id TEXT NOT NULL,
        first_seen_at INTEGER, dispatched_at INTEGER, completed_at INTEGER, status TEXT, delivered INTEGER, reply_disposition TEXT, delivery_error TEXT,
        model TEXT, prompt_version TEXT, report_text TEXT, report_sent_at INTEGER, report_message_id TEXT)`);
      old.exec(`INSERT INTO incidents (ref, incident_key, dag_id, task_id, run_id, first_seen_at) VALUES ('INC-OLD001', 'k', 'd', 't', 'r', 1000)`);
      old.close();
      const s = new FeedbackStore(path);
      expect(s.recordResolution("INC-OLD001", 5000, "button")).toBe(true);
      expect(s.getIncident("INC-OLD001")).toMatchObject({ resolved_at: 5000, resolution: "button" });
      s.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("report text retention", () => {
  it("removes only the report text of old incidents; timings, votes, resolution and cost stay", () => {
    const s = new FeedbackStore(":memory:");
    s.recordDispatch({ ref: "INC-OLD000", incidentKey: "a", dagId: "d", taskId: "t", runId: "r1", firstSeenAt: 1 * H, dispatchedAt: 1 * H });
    s.recordReport("INC-OLD000", { text: "old report", sentAt: 1 * H + 60_000 });
    s.recordRating("INC-OLD000", "u:1", "up", 2 * H);
    s.recordResolution("INC-OLD000", 3 * H, "button");
    s.recordDispatch({ ref: "INC-NEW000", incidentKey: "b", dagId: "d", taskId: "t", runId: "r2", firstSeenAt: 200 * H, dispatchedAt: 200 * H });
    s.recordReport("INC-NEW000", { text: "new report", sentAt: 200 * H + 60_000 });
    expect(s.pruneReportText(100 * H, 300 * H)).toBe(1);
    expect(s.pruneReportText(100 * H, 301 * H)).toBe(0); // idempotent
    expect(s.getIncident("INC-OLD000")).toMatchObject({ report_text: null, report_pruned_at: 300 * H, report_sent_at: 1 * H + 60_000, resolution: "button" });
    expect(s.getIncident("INC-NEW000")!.report_text).toBe("new report");
    expect(s.getFeedback("INC-OLD000")).toHaveLength(1);
    const d = buildDashboardData(s.incidentsSince(0), s.feedbackSince(0), { days: 30, now: 300 * H });
    expect(d.incidents.find((i) => i.ref === "INC-OLD000")).toMatchObject({ reportText: null, reportPrunedAt: 300 * H, resolveSeconds: 7200 });
  });
});

