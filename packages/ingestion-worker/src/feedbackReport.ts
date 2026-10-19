import { REASONS, RATINGS, type Rating, type Reason } from "./feedback.js";
import type { FeedbackRow, IncidentRow } from "./feedbackStore.js";

/**
 * Plain-text review of investigator feedback, grouped by prompt version, for
 * tuning AGENTS.md (spec §6 Phase 3). Pure function over store rows.
 */
export function renderFeedbackReport(incidents: IncidentRow[], feedback: FeedbackRow[], opts: { days: number; reportChars?: number }): string {
  const reportChars = opts.reportChars ?? 600;
  const byRef = new Map(incidents.map((i) => [i.ref, i]));
  const out: string[] = [`Investigator feedback, last ${opts.days} day(s)`, ""];

  const delivered = incidents.filter((i) => i.delivered === 1);
  const triage = delivered
    .map((i) => (i.first_seen_at && (i.report_sent_at ?? i.completed_at) ? (i.report_sent_at ?? i.completed_at)! - i.first_seen_at : undefined))
    .filter((v): v is number => v !== undefined)
    .sort((a, b) => a - b);
  out.push(
    `Incidents: ${incidents.length} (report delivered: ${delivered.length}, failed or undelivered: ${incidents.filter((i) => i.completed_at && i.delivered !== 1).length})`,
    `Triage time, failure -> report (median / p90): ${fmtDuration(quantile(triage, 0.5))} / ${fmtDuration(quantile(triage, 0.9))}`,
    mttrLine(incidents),
    `Incidents with feedback: ${new Set(feedback.map((f) => f.ref)).size}`,
    groundingLine(incidents),
    "",
    "By prompt version:",
  );

  const versions = new Map<string, { incidents: number; votes: Record<Rating, number> }>();
  for (const i of incidents) {
    const v = i.prompt_version ?? "unknown";
    if (!versions.has(v)) versions.set(v, { incidents: 0, votes: { up: 0, down: 0, misleading: 0 } });
    versions.get(v)!.incidents++;
  }
  for (const f of feedback) {
    if (!f.rating) continue;
    const s = versions.get(byRef.get(f.ref)?.prompt_version ?? "unknown");
    if (s) s.votes[f.rating]++;
  }
  for (const [v, s] of versions) {
    const total = s.votes.up + s.votes.down + s.votes.misleading;
    const rate = total ? `${Math.round((100 * s.votes.up) / total)}% helpful` : "no votes";
    out.push(`  ${v}: ${s.incidents} incidents, 👍 ${s.votes.up} / 👎 ${s.votes.down} / ⚠️ ${s.votes.misleading} (${rate})`);
  }

  const reasonCounts = new Map<Reason, number>();
  for (const f of feedback) if (f.reason) reasonCounts.set(f.reason, (reasonCounts.get(f.reason) ?? 0) + 1);
  out.push("", "Reasons given:");
  if (!reasonCounts.size) out.push("  none");
  for (const [r, n] of [...reasonCounts].sort((a, b) => b[1] - a[1])) out.push(`  ${REASONS[r]}: ${n}`);

  const negative = feedback.filter((f) => f.rating === "down" || f.rating === "misleading" || (!f.rating && f.reason));
  out.push("", `Negative feedback to review (${negative.length}):`);
  if (!negative.length) out.push("  none");
  for (const f of negative) {
    const i = byRef.get(f.ref);
    out.push(
      "",
      `--- ${f.ref} ${f.rating ? RATINGS[f.rating] : "(reason only)"}${f.reason ? ` - ${REASONS[f.reason]}` : ""}`,
      `    ${i ? `${i.dag_id} / ${i.task_id} / ${i.run_id}` : "unknown incident"} | prompt ${i?.prompt_version ?? "unknown"} | voted ${new Date(f.updated_at).toISOString()}`,
    );
    const text = i?.report_text;
    out.push(text ? indent(text.length > reportChars ? text.slice(0, reportChars) + " …" : text) : "    (report text not captured)");
  }
  return out.join("\n");
}

/**
 * Short daily summary for LINE (`report.js --summary --line`, server crontab): the
 * numbers plus the incidents that need a look (not delivered, 👎/⚠️, unverified claims).
 */
export function renderDailySummary(
  incidents: IncidentRow[],
  feedback: FeedbackRow[],
  opts: { days: number; costUsd?: number | null; maxItems?: number },
): string {
  const maxItems = opts.maxItems ?? 8;
  const period = opts.days === 1 ? "last 24 h" : `last ${opts.days} days`;
  const out = [`☀️ Investigator summary, ${period}`];
  if (!incidents.length) {
    out.push("✅ No incidents.");
    return out.join("\n");
  }

  const byRef = new Map(incidents.map((i) => [i.ref, i]));
  const label = (ref: string) => {
    const i = byRef.get(ref);
    return i ? `${ref} ${i.dag_id} / ${i.task_id}` : ref;
  };
  const items: string[] = [];
  for (const i of incidents) {
    if (i.completed_at && i.delivered !== 1) items.push(`• ${label(i.ref)}: report not delivered (${i.status ?? "unknown status"})`);
  }
  for (const f of feedback) {
    if (f.rating === "down" || f.rating === "misleading" || (!f.rating && f.reason)) {
      items.push(`• ${label(f.ref)}: ${f.rating ? RATINGS[f.rating] : "reason only"}${f.reason ? ` - ${REASONS[f.reason]}` : ""}`);
    }
  }
  for (const i of incidents) {
    if (i.ungrounded && i.ungrounded !== "[]") items.push(`• ${label(i.ref)}: not found in the evidence: ${(JSON.parse(i.ungrounded) as string[]).join("; ")}`);
  }

  const delivered = incidents.filter((i) => i.delivered === 1);
  const triage = delivered
    .map((i) => (i.first_seen_at && (i.report_sent_at ?? i.completed_at) ? (i.report_sent_at ?? i.completed_at)! - i.first_seen_at : undefined))
    .filter((v): v is number => v !== undefined)
    .sort((a, b) => a - b);
  const votes = { up: 0, down: 0, misleading: 0 };
  for (const f of feedback) if (f.rating) votes[f.rating]++;
  const voted = new Set(feedback.filter((f) => byRef.has(f.ref)).map((f) => f.ref)).size;

  out.push(
    items.length ? `⚠️ ${items.length} item(s) need a look` : "✅ Nothing needs a look",
    "",
    `Incidents: ${incidents.length} (delivered ${delivered.length}, failed ${incidents.filter((i) => i.completed_at && i.delivered !== 1).length})`,
    `Triage median / p90: ${fmtDuration(quantile(triage, 0.5))} / ${fmtDuration(quantile(triage, 0.9))}`,
    `Feedback: 👍 ${votes.up} / 👎 ${votes.down} / ⚠️ ${votes.misleading} (${incidents.length - voted} without a vote)`,
  );
  if (opts.costUsd != null) out.push(`Model cost: $${opts.costUsd.toFixed(2)}`);
  if (items.length) {
    out.push("", "Needs a look:", ...items.slice(0, maxItems));
    if (items.length > maxItems) out.push(`• … and ${items.length - maxItems} more`);
  }
  out.push("", "Details: make feedback-report on the server, or the dashboard.");
  return out.join("\n");
}

/** Linear-interpolation quantile of an ascending list (the median of an even count is the mean of the middle two). */
export function quantile(sorted: number[], p: number): number | undefined {
  if (!sorted.length) return undefined;
  const pos = (sorted.length - 1) * p;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo]! + (sorted[hi]! - sorted[lo]!) * (pos - lo);
}

function fmtDuration(ms: number | undefined): string {
  if (ms === undefined) return "n/a";
  const s = Math.round(ms / 1000);
  return s < 120 ? `${s}s` : `${Math.round(s / 60)}m`;
}

function indent(s: string): string {
  return s
    .split("\n")
    .map((l) => `    ${l}`)
    .join("\n");
}

/** Grounding check: reports whose SHAs, paths or table names were all found in the evidence. */
function groundingLine(incidents: IncidentRow[]): string {
  const checked = incidents.filter((i) => i.ungrounded !== null);
  const flagged = checked.filter((i) => i.ungrounded !== "[]");
  const list = flagged.map((i) => `${i.ref}: ${(JSON.parse(i.ungrounded!) as string[]).join("; ")}`);
  return `Grounding check: ${checked.length} report(s) checked, ${flagged.length} with claims not found in the evidence${list.length ? `\n  ${list.join("\n  ")}` : ""}`;
}

/** MTTR: first failure -> resolved (✅ in LINE or the DAG's next successful run). */
function mttrLine(incidents: IncidentRow[]): string {
  const times = incidents
    .map((i) => (i.resolved_at && (i.first_seen_at ?? i.dispatched_at) ? i.resolved_at - (i.first_seen_at ?? i.dispatched_at)! : undefined))
    .filter((v): v is number => v !== undefined)
    .sort((a, b) => a - b);
  const byButton = incidents.filter((i) => i.resolution === "button").length;
  return `MTTR, failure -> resolved (median / p90): ${fmtDuration(quantile(times, 0.5))} / ${fmtDuration(quantile(times, 0.9))} (${times.length} resolved: ${byButton} by button, ${times.length - byButton} by next successful run; ${incidents.length - times.length} open)`;
}
