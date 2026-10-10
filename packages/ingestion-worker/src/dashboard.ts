import { readFileSync } from "node:fs";
import { REASONS, RATINGS, type Rating, type Reason } from "./feedback.js";
import { quantile } from "./feedbackReport.js";
import type { FeedbackRow, IncidentRow } from "./feedbackStore.js";

/**
 * Live dashboard (internal listener only): JSON for /api/dashboard plus the
 * static page. Voter hashes are never exposed; report text is returned as
 * plain text and rendered with textContent by the page.
 */

export type IncidentState = "running" | "delivered" | "failed";

export type DashboardIncident = {
  ref: string;
  dagId: string;
  taskId: string;
  runId: string;
  state: IncidentState;
  status: string | null;
  deliveryError: string | null;
  replyDisposition: string | null;
  model: string | null;
  promptVersion: string | null;
  firstSeenAt: number | null;
  dispatchedAt: number | null;
  reportSentAt: number | null;
  completedAt: number | null;
  triageSeconds: number | null;
  votes: Record<Rating, number>;
  reasons: { reason: Reason; label: string; count: number }[];
  reportText: string | null;
};

export type DashboardData = {
  enabled: true;
  generatedAt: number;
  days: number;
  funnel: { investigated: number; completed: number; delivered: number; fallback: number; rated: number };
  votes: Record<Rating, number> & { total: number; helpfulRate: number | null };
  triage: { medianSeconds: number | null; p90Seconds: number | null };
  versions: { version: string; firstSeenAt: number | null; incidents: number; votes: Record<Rating, number>; helpfulRate: number | null }[];
  reasons: { reason: Reason; label: string; count: number }[];
  ratingLabels: Record<Rating, string>;
  incidents: DashboardIncident[];
};

const emptyVotes = (): Record<Rating, number> => ({ up: 0, down: 0, misleading: 0 });
const helpful = (v: Record<Rating, number>): number | null => {
  const total = v.up + v.down + v.misleading;
  return total ? v.up / total : null;
};

export function buildDashboardData(incidents: IncidentRow[], feedback: FeedbackRow[], opts: { days: number; now: number }): DashboardData {
  const byRef = new Map<string, FeedbackRow[]>();
  for (const f of feedback) byRef.set(f.ref, [...(byRef.get(f.ref) ?? []), f]);

  const rows: DashboardIncident[] = incidents.map((i) => {
    const fb = byRef.get(i.ref) ?? [];
    const votes = emptyVotes();
    const reasonCounts = new Map<Reason, number>();
    for (const f of fb) {
      if (f.rating) votes[f.rating]++;
      if (f.reason) reasonCounts.set(f.reason, (reasonCounts.get(f.reason) ?? 0) + 1);
    }
    const state: IncidentState = i.completed_at === null ? "running" : i.status === "ok" && i.delivered === 1 ? "delivered" : "failed";
    const reportAt = i.report_sent_at ?? i.completed_at;
    return {
      ref: i.ref,
      dagId: i.dag_id,
      taskId: i.task_id,
      runId: i.run_id,
      state,
      status: i.status,
      deliveryError: i.delivery_error,
      replyDisposition: i.reply_disposition,
      model: i.model,
      promptVersion: i.prompt_version,
      firstSeenAt: i.first_seen_at,
      dispatchedAt: i.dispatched_at,
      reportSentAt: i.report_sent_at,
      completedAt: i.completed_at,
      triageSeconds: state === "delivered" && i.first_seen_at && reportAt ? Math.round((reportAt - i.first_seen_at) / 100) / 10 : null,
      votes,
      reasons: [...reasonCounts].map(([reason, count]) => ({ reason, label: REASONS[reason], count })),
      reportText: i.report_text,
    };
  });

  const votes = emptyVotes();
  const reasonCounts = new Map<Reason, number>();
  for (const f of feedback) {
    if (f.rating) votes[f.rating]++;
    if (f.reason) reasonCounts.set(f.reason, (reasonCounts.get(f.reason) ?? 0) + 1);
  }

  const versions = new Map<string, DashboardData["versions"][number]>();
  for (const r of rows) {
    const key = r.promptVersion ?? "unknown";
    const v = versions.get(key) ?? { version: key, firstSeenAt: r.dispatchedAt, incidents: 0, votes: emptyVotes(), helpfulRate: null };
    v.incidents++;
    if (r.dispatchedAt !== null && (v.firstSeenAt === null || r.dispatchedAt < v.firstSeenAt)) v.firstSeenAt = r.dispatchedAt;
    for (const k of Object.keys(votes) as Rating[]) v.votes[k] += r.votes[k];
    versions.set(key, v);
  }
  for (const v of versions.values()) v.helpfulRate = helpful(v.votes);

  const triage = rows
    .map((r) => r.triageSeconds)
    .filter((s): s is number => s !== null)
    .sort((a, b) => a - b);
  const round = (s: number | undefined) => (s === undefined ? null : Math.round(s * 10) / 10);

  return {
    enabled: true,
    generatedAt: opts.now,
    days: opts.days,
    funnel: {
      investigated: rows.length,
      completed: rows.filter((r) => r.state !== "running").length,
      delivered: rows.filter((r) => r.state === "delivered").length,
      fallback: rows.filter((r) => r.state === "failed").length,
      rated: rows.filter((r) => r.votes.up + r.votes.down + r.votes.misleading > 0).length,
    },
    votes: { ...votes, total: votes.up + votes.down + votes.misleading, helpfulRate: helpful(votes) },
    triage: { medianSeconds: round(quantile(triage, 0.5)), p90Seconds: round(quantile(triage, 0.9)) },
    versions: [...versions.values()].sort((a, b) => (a.firstSeenAt ?? 0) - (b.firstSeenAt ?? 0)),
    reasons: [...reasonCounts].sort((a, b) => b[1] - a[1]).map(([reason, count]) => ({ reason, label: REASONS[reason], count })),
    ratingLabels: { ...RATINGS },
    incidents: rows.sort((a, b) => (b.dispatchedAt ?? b.firstSeenAt ?? 0) - (a.dispatchedAt ?? a.firstSeenAt ?? 0)),
  };
}

let page: string | undefined;

/** The static page: dist/dashboard.html when bundled, public/dashboard.html from source. */
export function dashboardHtml(): string {
  if (page) return page;
  for (const candidate of ["./dashboard.html", "../public/dashboard.html"]) {
    try {
      page = readFileSync(new URL(candidate, import.meta.url), "utf8");
      return page;
    } catch {
      // try the next location
    }
  }
  throw new Error("dashboard.html not found");
}

/** The page uses inline script/style only and fetches its data from the same origin. */
export const DASHBOARD_CSP =
  "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
