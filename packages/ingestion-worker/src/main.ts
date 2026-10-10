import { loadConfig } from "./config.js";
import { FileDispatcher, HookDispatcher } from "./dispatcher.js";
import { createWorker } from "./server.js";

const cfg = loadConfig();
const dispatcher = cfg.dryRun ? new FileDispatcher(cfg.incidentDir) : new HookDispatcher(cfg.openclaw);
const { server, publicServer, aggregator, store } = createWorker(cfg, dispatcher);

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

const shutdown = async () => {
  console.info("[worker] shutting down; flushing pending incidents");
  server.close();
  publicServer?.close();
  await aggregator.flushAll();
  store?.close();
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
