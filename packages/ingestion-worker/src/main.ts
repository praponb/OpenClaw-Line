import { loadConfig } from "./config.js";
import { FileDispatcher, HookDispatcher } from "./dispatcher.js";
import { createWorker } from "./server.js";

const cfg = loadConfig();
const dispatcher = cfg.dryRun ? new FileDispatcher(cfg.incidentDir) : new HookDispatcher(cfg.openclaw);
const { server, publicServer, dashboardServer, aggregator, store, resolutionWatcher } = createWorker(cfg, dispatcher);

server.listen(cfg.port, () => {
  console.info(
    `[worker] listening on :${cfg.port} mode=${cfg.dryRun ? `dry-run (${cfg.incidentDir})` : `hook -> ${cfg.openclaw.gatewayUrl} channel=${cfg.openclaw.channel}`} dedupe_window=${cfg.dedupeWindowMs / 1000}s`,
  );
});
if (publicServer && cfg.feedback) {
  publicServer.listen(cfg.feedback.publicPort, () => {
    console.info(`[worker] feedback enabled: LINE webhook on :${cfg.feedback!.publicPort}/line/webhook, db=${cfg.feedback!.dbPath}`);
  });
} else {
  console.info("[worker] feedback disabled (LINE_CHANNEL_ACCESS_TOKEN / LINE_CHANNEL_SECRET not set)");
}

if (dashboardServer && cfg.dashboard) {
  dashboardServer.listen(cfg.dashboard.port, () => {
    console.info(`[worker] public dashboard on :${cfg.dashboard!.port} (Cloudflare Access: ${cfg.dashboard!.access.teamDomain}, ${cfg.dashboard!.access.allowedEmails.length} allowed email(s))`);
  });
} else {
  console.info("[worker] public dashboard disabled (set CF_ACCESS_TEAM_DOMAIN, CF_ACCESS_AUD and DASHBOARD_ALLOWED_EMAILS)");
}

let resolutionTimer: NodeJS.Timeout | undefined;
if (resolutionWatcher && cfg.resolutionPollSeconds > 0) {
  const check = () => resolutionWatcher.checkOnce().catch((err) => console.warn(`[mttr] check failed: ${(err as Error).message}`));
  setTimeout(check, 30_000).unref();
  resolutionTimer = setInterval(check, cfg.resolutionPollSeconds * 1000);
  resolutionTimer.unref();
  console.info(`[worker] MTTR: checking Airflow for later successful runs every ${cfg.resolutionPollSeconds}s`);
}

let retentionTimer: NodeJS.Timeout | undefined;
if (store && cfg.reportRetentionDays > 0) {
  const prune = () => {
    const n = store.pruneReportText(Date.now() - cfg.reportRetentionDays * 86_400_000, Date.now());
    if (n) console.info(`[retention] removed report text of ${n} incident(s) older than ${cfg.reportRetentionDays} days`);
  };
  prune();
  retentionTimer = setInterval(prune, 24 * 3600 * 1000);
  retentionTimer.unref();
}

const shutdown = async () => {
  clearInterval(retentionTimer);
  clearInterval(resolutionTimer);
  console.info("[worker] shutting down; flushing pending incidents");
  server.close();
  publicServer?.close();
  dashboardServer?.close();
  await aggregator.flushAll();
  store?.close();
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
