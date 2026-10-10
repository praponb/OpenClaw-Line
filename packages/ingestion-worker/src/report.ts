// CLI: print the feedback review. Usage: node dist/report.js [--days 30]
import { envString } from "@openclaw-line/shared";
import { renderFeedbackReport } from "./feedbackReport.js";
import { FeedbackStore } from "./feedbackStore.js";

const i = process.argv.indexOf("--days");
const days = i > 0 ? Number(process.argv[i + 1]) : 30;
if (!Number.isFinite(days) || days <= 0) {
  console.error("usage: report.js [--days N]");
  process.exit(2);
}
const store = new FeedbackStore(envString(process.env, "FEEDBACK_DB_PATH", "./data/feedback.db"));
const since = Date.now() - days * 24 * 3600 * 1000;
console.log(renderFeedbackReport(store.incidentsSince(since), store.feedbackSince(since), { days }));
store.close();
