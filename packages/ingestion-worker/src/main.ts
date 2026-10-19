import { loadConfig } from "./config.js";
import { FileDispatcher, HookDispatcher } from "./dispatcher.js";
import { createWorker } from "./server.js";

const cfg = loadConfig();
const dispatcher = cfg.dryRun ? new FileDispatcher(cfg.incidentDir) : new HookDispatcher(cfg.openclaw);
const { server, publicServer, dashboardServer, ingestServer, aggregator, store, resolutionWatcher, alerts, backup, healthProblems, investigating, idle } = createWorker(cfg, dispatcher);

// Failure windows that were open when the worker last stopped (incident log only).
const restored = aggregator.restore();
if (restored) console.info(`[worker] resumed ${restored} incident(s) that were still being grouped at the last shutdown`);

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

if (ingestServer && cfg.ingestPort !== undefined) {
  ingestServer.listen(cfg.ingestPort, () => {
    console.info(`[worker] Airflow callbacks also on :${cfg.ingestPort}/v1/airflow/failure (failure route only)`);
  });
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

let backupTimer: NodeJS.Timeout | undefined;
if (backup && cfg.feedback) {
  // Hourly check; at most one copy per Bangkok day.
  backup.runIfDue();
  backupTimer = setInterval(() => backup.runIfDue(), 3600 * 1000);
  backupTimer.unref();
  console.info(`[worker] feedback log backups: daily into ${cfg.feedback.backup.dir}, keeping ${cfg.feedback.backup.keep}`);
}

let heartbeatTimer: NodeJS.Timeout | undefined;
if (alerts.enabled) {
  const beat = () => void healthProblems().then((p) => alerts.heartbeat(p));
  // First beat after a minute: during a deploy the gateway restarts too and isn't ready yet.
  setTimeout(beat, 60_000).unref();
  heartbeatTimer = setInterval(beat, cfg.alerts.heartbeatSeconds * 1000);
  heartbeatTimer.unref();
  console.info(`[worker] alerts: heartbeat ${cfg.alerts.heartbeatUrl ? `every ${cfg.alerts.heartbeatSeconds}s` : "off"}, LINE check ${cfg.alerts.lineUrl ? "on" : "off"}`);
} else {
  console.info("[worker] alerts disabled (set HEALTHCHECK_URL / HEALTHCHECK_LINE_URL)");
}

// Running investigations get up to the hook timeout plus 30 s to finish, so their feedback
// cards are still sent (compose stop_grace_period allows for it). A second signal stops at once.
const drainMs = (cfg.openclaw.timeoutSeconds + 30) * 1000;
let stopping = false;
const shutdown = async () => {
  if (stopping) process.exit(1);
  stopping = true;
  clearInterval(retentionTimer);
  clearInterval(resolutionTimer);
  clearInterval(backupTimer);
  clearInterval(heartbeatTimer);
  server.close();
  publicServer?.close();
  dashboardServer?.close();
  ingestServer?.close();
  if (aggregator.persistent) {
    // Saved in the incident log; the next start resumes them.
    console.info(`[worker] shutting down; ${aggregator.pendingCount} pending incident(s) resume on the next start`);
    aggregator.stop();
  } else {
    console.info("[worker] shutting down; flushing pending incidents");
    await aggregator.flushAll();
  }
  if (investigating() > 0) {
    console.info(`[worker] waiting up to ${drainMs / 1000}s for ${investigating()} running investigation(s)`);
    if (!(await idle(drainMs))) console.warn(`[worker] stopping with ${investigating()} investigation(s) still running; their feedback cards won't be sent`);
  }
  store?.close();
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
