// CLI: print the feedback review. Usage: node dist/report.js [--days 30] [--summary [--line]]
// --summary prints the short daily summary instead; --line also pushes it to LINE_TARGET_ID.
import { envString } from "@openclaw-line/shared";
import { loadConfig } from "./config.js";
import { incidentCostUsd } from "./dashboard.js";
import { renderDailySummary, renderFeedbackReport } from "./feedbackReport.js";
import { FeedbackStore } from "./feedbackStore.js";
import { LineClient } from "./line.js";

const i = process.argv.indexOf("--days");
const days = i > 0 ? Number(process.argv[i + 1]) : 30;
if (!Number.isFinite(days) || days <= 0) {
  console.error("usage: report.js [--days N] [--summary [--line]]");
  process.exit(2);
}
const summary = process.argv.includes("--summary");
const toLine = process.argv.includes("--line");
const store = new FeedbackStore(envString(process.env, "FEEDBACK_DB_PATH", "./data/feedback.db"));
const since = Date.now() - days * 24 * 3600 * 1000;
const incidents = store.incidentsSince(since);
const feedback = store.feedbackSince(since);
let text: string;
if (summary) {
  const cfg = loadConfig(process.env);
  let cost: number | null = null;
  for (const u of store.usageSince(since).values()) {
    const c = incidentCostUsd(u, cfg.modelPrices, cfg.fallbackPrices);
    if (c !== null) cost = (cost ?? 0) + c;
  }
  text = renderDailySummary(incidents, feedback, { days, costUsd: cost });
  if (toLine) {
    if (!cfg.feedback || !cfg.openclaw.to) {
      console.error("LINE is not configured (LINE_CHANNEL_ACCESS_TOKEN / LINE_TARGET_ID)");
      process.exit(1);
    }
    await new LineClient(cfg.feedback.channelAccessToken, { baseUrl: cfg.feedback.lineApiBaseUrl }).push(cfg.openclaw.to, [{ type: "text", text }]);
  }
} else {
  text = renderFeedbackReport(incidents, feedback, { days });
}
console.log(text);
store.close();
