import { envBool, envInt, envOptional, envString, type Env } from "@openclaw-line/shared";

export type WorkerConfig = {
  port: number;
  webhookSecret: string;
  dedupeWindowMs: number;
  dedupeTtlMs: number;
  airflow?: { baseUrl: string; apiVersion: "v1" | "v2"; username: string; password: string };
  logTailLines: number;
  maxLogChars: number;
  dryRun: boolean;
  incidentDir: string;
  openclaw: {
    gatewayUrl: string;
    hookToken: string;
    agentId: string;
    channel: string;
    to: string;
    accountId?: string;
    model?: string;
    timeoutSeconds: number;
  };
};

export function loadConfig(env: Env = process.env): WorkerConfig {
  const dryRun = envBool(env, "DRY_RUN");
  const airflowUrl = envOptional(env, "AIRFLOW_BASE_URL");
  return {
    port: envInt(env, "PORT", 8080),
    webhookSecret: envString(env, "WEBHOOK_SECRET"),
    dedupeWindowMs: envInt(env, "DEDUPE_WINDOW_S", 60) * 1000,
    dedupeTtlMs: envInt(env, "DEDUPE_TTL_S", 6 * 3600) * 1000,
    airflow: airflowUrl
      ? {
          baseUrl: airflowUrl,
          apiVersion: envString(env, "AIRFLOW_API_VERSION", "v2") === "v1" ? "v1" : "v2",
          username: envString(env, "AIRFLOW_USERNAME"),
          password: envString(env, "AIRFLOW_PASSWORD"),
        }
      : undefined,
    logTailLines: envInt(env, "LOG_TAIL_LINES", 300),
    maxLogChars: envInt(env, "INITIAL_LOG_MAX_CHARS", 12_000),
    dryRun,
    incidentDir: envString(env, "INCIDENT_DIR", "./incidents"),
    openclaw: {
      gatewayUrl: envString(env, "OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789"),
      hookToken: dryRun ? envString(env, "OPENCLAW_HOOK_TOKEN", "") : envString(env, "OPENCLAW_HOOK_TOKEN"),
      agentId: envString(env, "OPENCLAW_AGENT_ID", "pipeline-investigator"),
      channel: envString(env, "DELIVERY_CHANNEL", "line"),
      to: dryRun ? envString(env, "LINE_TARGET_ID", "") : envString(env, "LINE_TARGET_ID"),
      accountId: envOptional(env, "LINE_ACCOUNT_ID"),
      model: envOptional(env, "OPENCLAW_MODEL"),
      timeoutSeconds: envInt(env, "OPENCLAW_TIMEOUT_S", 300),
    },
  };
}
