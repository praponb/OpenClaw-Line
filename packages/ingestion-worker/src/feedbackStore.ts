import { createHash } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { Rating, Reason } from "./feedback.js";

/**
 * Incident + feedback log for Phase 3 (prompt tuning and triage-time metrics).
 * Single-writer SQLite (node:sqlite), owned by the ingestion worker.
 */

const SCHEMA = `
CREATE TABLE IF NOT EXISTS incidents (
  ref               TEXT PRIMARY KEY,
  incident_key      TEXT NOT NULL,
  dag_id            TEXT NOT NULL,
  task_id           TEXT NOT NULL,
  run_id            TEXT NOT NULL,
  first_seen_at     INTEGER,
  dispatched_at     INTEGER,
  completed_at      INTEGER,
  status            TEXT,
  delivered         INTEGER,
  reply_disposition TEXT,
  delivery_error    TEXT,
  model             TEXT,
  prompt_version    TEXT,
  report_text       TEXT,
  report_sent_at    INTEGER,
  report_message_id TEXT,
  resolved_at       INTEGER,
  resolution        TEXT,
  resolved_by       TEXT,
  agent_run_id      TEXT,
  session_key       TEXT
);
CREATE TABLE IF NOT EXISTS model_usage (
  run_id      TEXT NOT NULL,
  session_key TEXT,
  provider    TEXT,
  model       TEXT,
  input       INTEGER NOT NULL,
  output      INTEGER NOT NULL,
  cache_read  INTEGER NOT NULL,
  cache_write INTEGER NOT NULL,
  at          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS model_usage_run ON model_usage (run_id);
CREATE TABLE IF NOT EXISTS feedback (
  ref        TEXT NOT NULL,
  voter      TEXT NOT NULL,
  rating     TEXT,
  reason     TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (ref, voter)
);
CREATE TABLE IF NOT EXISTS line_events (
  event_id    TEXT PRIMARY KEY,
  received_at INTEGER NOT NULL
);
`;

export type IncidentRow = {
  ref: string;
  incident_key: string;
  dag_id: string;
  task_id: string;
  run_id: string;
  first_seen_at: number | null;
  dispatched_at: number | null;
  completed_at: number | null;
  status: string | null;
  delivered: number | null;
  reply_disposition: string | null;
  delivery_error: string | null;
  model: string | null;
  prompt_version: string | null;
  report_text: string | null;
  report_sent_at: number | null;
  report_message_id: string | null;
  /** MTTR: when the incident was resolved, how ("button" | "next_success"), and by whom (voter hash or Airflow run id). */
  resolved_at: number | null;
  resolution: string | null;
  resolved_by: string | null;
  /** OpenClaw agent run id (joins model_usage). */
  agent_run_id: string | null;
  /** OpenClaw session key of the investigator run, learned from the model call that wrote the report (joins model_usage). */
  session_key: string | null;
};

export type ModelUsage = { input: number; output: number; cacheRead: number; cacheWrite: number; calls: number; model: string | null };

export type Resolution = "button" | "next_success";

/** Columns added after the first release; existing databases get them on startup. */
const MIGRATIONS: [string, string][] = [
  ["resolved_at", "INTEGER"],
  ["resolution", "TEXT"],
  ["resolved_by", "TEXT"],
  ["agent_run_id", "TEXT"],
  ["session_key", "TEXT"],
];

export type FeedbackRow = { ref: string; voter: string; rating: Rating | null; reason: Reason | null; created_at: number; updated_at: number };

export type Completion = { status?: string; delivered?: boolean; replyDisposition?: string; deliveryError?: string };

export class FeedbackStore {
  private readonly db: DatabaseSync;

  constructor(path: string) {
    if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    if (path !== ":memory:") this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec(SCHEMA);
    const have = new Set((this.db.prepare(`PRAGMA table_info(incidents)`).all() as { name: string }[]).map((c) => c.name));
    for (const [col, type] of MIGRATIONS) if (!have.has(col)) this.db.exec(`ALTER TABLE incidents ADD COLUMN ${col} ${type}`);
    const usageCols = new Set((this.db.prepare(`PRAGMA table_info(model_usage)`).all() as { name: string }[]).map((c) => c.name));
    if (!usageCols.has("session_key")) this.db.exec(`ALTER TABLE model_usage ADD COLUMN session_key TEXT`);
    this.db.exec(`CREATE INDEX IF NOT EXISTS model_usage_session ON model_usage (session_key)`);
  }

  close(): void {
    this.db.close();
  }

  recordDispatch(i: { ref: string; incidentKey: string; dagId: string; taskId: string; runId: string; firstSeenAt?: number; dispatchedAt: number; model?: string }): void {
    this.db
      .prepare(
        `INSERT INTO incidents (ref, incident_key, dag_id, task_id, run_id, first_seen_at, dispatched_at, model)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(ref) DO UPDATE SET
           incident_key = excluded.incident_key, dag_id = excluded.dag_id, task_id = excluded.task_id,
           run_id = excluded.run_id, first_seen_at = COALESCE(incidents.first_seen_at, excluded.first_seen_at),
           dispatched_at = excluded.dispatched_at, model = excluded.model`,
      )
      .run(i.ref, i.incidentKey, i.dagId, i.taskId, i.runId, i.firstSeenAt ?? null, i.dispatchedAt, i.model ?? null);
  }

  recordCompletion(ref: string, c: Completion, completedAt: number): void {
    this.db
      .prepare(`UPDATE incidents SET completed_at = ?, status = ?, delivered = ?, reply_disposition = ?, delivery_error = ? WHERE ref = ?`)
      .run(completedAt, c.status ?? null, c.delivered === undefined ? null : c.delivered ? 1 : 0, c.replyDisposition ?? null, c.deliveryError ?? null, ref);
  }

  /** Report text captured by the report-capture plugin (may arrive before the hook completion). */
  recordReport(ref: string, r: { text: string; sentAt: number; messageId?: string; promptVersion?: string }): boolean {
    const res = this.db
      .prepare(`UPDATE incidents SET report_text = ?, report_sent_at = ?, report_message_id = ?, prompt_version = COALESCE(?, prompt_version) WHERE ref = ?`)
      .run(r.text, r.sentAt, r.messageId ?? null, r.promptVersion ?? null, ref);
    return Number(res.changes) > 0;
  }

  /** The investigator session that wrote this incident's report; all model calls in it count toward the incident. */
  linkUsageSession(ref: string, sessionKey: string): void {
    this.db.prepare(`UPDATE incidents SET session_key = ? WHERE ref = ?`).run(sessionKey, ref);
  }

  setAgentRunId(ref: string, runId: string): void {
    this.db.prepare(`UPDATE incidents SET agent_run_id = ? WHERE ref = ?`).run(runId, ref);
  }

  /** One row per model call, forwarded by the report-capture plugin (may arrive before the run id is known). */
  recordModelUsage(
    u: { runId: string; sessionKey?: string; provider?: string; model?: string; input: number; output: number; cacheRead: number; cacheWrite: number },
    at: number,
  ): void {
    this.db
      .prepare(`INSERT INTO model_usage (run_id, session_key, provider, model, input, output, cache_read, cache_write, at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(u.runId, u.sessionKey ?? null, u.provider ?? null, u.model ?? null, u.input, u.output, u.cacheRead, u.cacheWrite, at);
  }

  /** Token totals per incident ref for incidents since `since`. */
  usageSince(since: number): Map<string, ModelUsage> {
    const rows = this.db
      .prepare(
        `SELECT i.ref AS ref, SUM(u.input) AS input, SUM(u.output) AS output, SUM(u.cache_read) AS cacheRead,
                SUM(u.cache_write) AS cacheWrite, COUNT(*) AS calls, MAX(u.provider || '/' || u.model) AS model
         FROM incidents i JOIN model_usage u
           ON (u.session_key IS NOT NULL AND u.session_key = i.session_key) OR u.run_id = i.agent_run_id
         WHERE COALESCE(i.dispatched_at, i.first_seen_at) >= ? GROUP BY i.ref`,
      )
      .all(since) as (ModelUsage & { ref: string })[];
    return new Map(rows.map(({ ref, ...u }) => [ref, { ...u, model: u.model && u.model !== "/" ? u.model : null }]));
  }

  /** First resolution wins; returns false when the incident was already resolved (or is unknown). */
  recordResolution(ref: string, at: number, how: Resolution, by?: string): boolean {
    const res = this.db
      .prepare(`UPDATE incidents SET resolved_at = ?, resolution = ?, resolved_by = ? WHERE ref = ? AND resolved_at IS NULL`)
      .run(at, how, by ?? null, ref);
    return Number(res.changes) > 0;
  }

  /** Incidents still open, newest first, for the auto-resolution check. */
  unresolvedSince(since: number): IncidentRow[] {
    return this.db
      .prepare(`SELECT * FROM incidents WHERE resolved_at IS NULL AND COALESCE(first_seen_at, dispatched_at) >= ? ORDER BY first_seen_at DESC`)
      .all(since) as IncidentRow[];
  }

  getIncident(ref: string): IncidentRow | undefined {
    return this.db.prepare(`SELECT * FROM incidents WHERE ref = ?`).get(ref) as IncidentRow | undefined;
  }

  /** One vote per person per incident: a later tap replaces the earlier rating. */
  recordRating(ref: string, voter: string, rating: Rating, at: number): void {
    this.db
      .prepare(
        `INSERT INTO feedback (ref, voter, rating, reason, created_at, updated_at) VALUES (?, ?, ?, NULL, ?, ?)
         ON CONFLICT(ref, voter) DO UPDATE SET
           rating = excluded.rating,
           reason = CASE WHEN excluded.rating = 'up' THEN NULL ELSE feedback.reason END,
           updated_at = excluded.updated_at`,
      )
      .run(ref, voter, rating, at, at);
  }

  recordReason(ref: string, voter: string, reason: Reason, at: number): void {
    this.db
      .prepare(
        `INSERT INTO feedback (ref, voter, rating, reason, created_at, updated_at) VALUES (?, ?, NULL, ?, ?, ?)
         ON CONFLICT(ref, voter) DO UPDATE SET reason = excluded.reason, updated_at = excluded.updated_at`,
      )
      .run(ref, voter, reason, at, at);
  }

  getFeedback(ref: string): FeedbackRow[] {
    return this.db.prepare(`SELECT * FROM feedback WHERE ref = ? ORDER BY created_at`).all(ref) as FeedbackRow[];
  }

  /** Returns false when this LINE webhook event was already handled (redelivery). */
  claimEvent(eventId: string, at: number): boolean {
    const res = this.db.prepare(`INSERT OR IGNORE INTO line_events (event_id, received_at) VALUES (?, ?)`).run(eventId, at);
    return Number(res.changes) === 1;
  }

  pruneEvents(olderThan: number): void {
    this.db.prepare(`DELETE FROM line_events WHERE received_at < ?`).run(olderThan);
  }

  incidentsSince(since: number): IncidentRow[] {
    return this.db.prepare(`SELECT * FROM incidents WHERE COALESCE(dispatched_at, first_seen_at) >= ? ORDER BY dispatched_at`).all(since) as IncidentRow[];
  }

  feedbackSince(since: number): FeedbackRow[] {
    return this.db
      .prepare(`SELECT f.* FROM feedback f JOIN incidents i ON i.ref = f.ref WHERE COALESCE(i.dispatched_at, i.first_seen_at) >= ? ORDER BY f.created_at`)
      .all(since) as FeedbackRow[];
  }
}

/** Store a salted hash, never the raw LINE user id. */
export function voterId(salt: string, userId: string | undefined, fallback: string): string {
  if (!userId) return `anon:${fallback}`;
  return "u:" + createHash("sha256").update(`${salt}:${userId}`).digest("hex").slice(0, 16);
}
