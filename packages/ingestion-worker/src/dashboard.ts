import { readFileSync } from "node:fs";
import { REASONS, RATINGS, type Rating, type Reason } from "./feedback.js";
import { quantile } from "./feedbackReport.js";
import type { FeedbackRow, IncidentRow, ModelUsage } from "./feedbackStore.js";

export type ModelPrices = { input: number; output: number; cacheRead: number; cacheWrite: number };

/** USD for one incident's tokens; `input` from the provider includes no cached tokens. */
export function usageCostUsd(u: Pick<ModelUsage, "input" | "output" | "cacheRead" | "cacheWrite">, p: ModelPrices): number {
  return (u.input * p.input + u.output * p.output + u.cacheRead * p.cacheRead + u.cacheWrite * p.cacheWrite) / 1e6;
}

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
  /** MTTR: first failure -> resolved ("button" in LINE or the DAG's "next_success"). */
  resolvedAt: number | null;
  resolution: string | null;
  resolveSeconds: number | null;
  /** Investigator model usage for this incident (null until token usage arrives) and its USD estimate (null without prices). */
  usage: ModelUsage | null;
  costUsd: number | null;
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
  modelCost: { input: number; output: number; cacheRead: number; cacheWrite: number; calls: number; incidents: number; usd: number | null; avgUsd: number | null; priced: boolean };
  mttr: { medianSeconds: number | null; p90Seconds: number | null; resolved: number; open: number };
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

export function buildDashboardData(
  incidents: IncidentRow[],
  feedback: FeedbackRow[],
  opts: { days: number; now: number; usage?: Map<string, ModelUsage>; prices?: ModelPrices | null },
): DashboardData {
  const prices = opts.prices ?? null;
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
      usage: opts.usage?.get(i.ref) ?? null,
      costUsd: prices && opts.usage?.get(i.ref) ? usageCostUsd(opts.usage.get(i.ref)!, prices) : null,
      resolvedAt: i.resolved_at,
      resolution: i.resolution,
      resolveSeconds: i.resolved_at && (i.first_seen_at ?? i.dispatched_at) ? Math.max(0, Math.round((i.resolved_at - (i.first_seen_at ?? i.dispatched_at)!) / 1000)) : null,
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
  const resolveTimes = rows
    .map((r) => r.resolveSeconds)
    .filter((s): s is number => s !== null)
    .sort((a, b) => a - b);

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
    modelCost: (() => {
      const used = rows.filter((r) => r.usage);
      const sum = (k: "input" | "output" | "cacheRead" | "cacheWrite" | "calls") => used.reduce((a, r) => a + r.usage![k], 0);
      const usd = prices ? used.reduce((a, r) => a + (r.costUsd ?? 0), 0) : null;
      return {
        input: sum("input"),
        output: sum("output"),
        cacheRead: sum("cacheRead"),
        cacheWrite: sum("cacheWrite"),
        calls: sum("calls"),
        incidents: used.length,
        usd,
        avgUsd: usd !== null && used.length ? usd / used.length : null,
        priced: prices !== null,
      };
    })(),
    mttr: {
      medianSeconds: round(quantile(resolveTimes, 0.5)),
      p90Seconds: round(quantile(resolveTimes, 0.9)),
      resolved: resolveTimes.length,
      open: rows.length - resolveTimes.length,
    },
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
