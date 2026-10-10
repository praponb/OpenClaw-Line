import { loadConfig } from "./config.js";
import { FileDispatcher, HookDispatcher } from "./dispatcher.js";
import { createWorker } from "./server.js";

const cfg = loadConfig();
const dispatcher = cfg.dryRun ? new FileDispatcher(cfg.incidentDir) : new HookDispatcher(cfg.openclaw);
const { server, aggregator } = createWorker(cfg, dispatcher);

server.listen(cfg.port, () => {
  console.info(
    `[worker] listening on :${cfg.port} mode=${cfg.dryRun ? `dry-run (${cfg.incidentDir})` : `hook -> ${cfg.openclaw.gatewayUrl} channel=${cfg.openclaw.channel}`} dedupe_window=${cfg.dedupeWindowMs / 1000}s`,
  );
});

const shutdown = async () => {
  console.info("[worker] shutting down; flushing pending incidents");
  server.close();
  await aggregator.flushAll();
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
