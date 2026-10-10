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
  report_message_id TEXT
);
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
};

export type FeedbackRow = { ref: string; voter: string; rating: Rating | null; reason: Reason | null; created_at: number; updated_at: number };

export type Completion = { status?: string; delivered?: boolean; replyDisposition?: string; deliveryError?: string };

export class FeedbackStore {
  private readonly db: DatabaseSync;

  constructor(path: string) {
    if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    if (path !== ":memory:") this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec(SCHEMA);
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
