import { createHmac } from "node:crypto";
import { envBool, envInt, envOptional, envString, type Env } from "@openclaw-line/shared";
import { normalizeTeamDomain, type AccessConfig } from "./cfAccess.js";
import type { PipelineStatusConfig } from "./pipelineStatus.js";

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
  /** Phase 3: feedback card, LINE webhook and incident log. Off when LINE credentials are missing. */
  feedback?: {
    channelAccessToken: string;
    channelSecret: string;
    lineApiBaseUrl: string;
    /** Public listener for POST /line/webhook (exposed through the Cloudflare tunnel). */
    publicPort: number;
    signingKey: string;
    userSalt: string;
    dbPath: string;
    /** Recorded with each incident; the gateway's agent config decides the model actually used. */
    modelLabel?: string;
  };
  /**
   * Public dashboard listener (e.g. dashboard.example.com through the Cloudflare
   * tunnel). Only started when Cloudflare Access verification is fully configured.
   */
  dashboard?: { port: number; access: AccessConfig };
  /** "Test LINE" button: max test pushes per Bangkok day (each uses LINE push quota). */
  dashboardTestDailyLimit: number;
  /** Databricks job status on the dashboard (read-only service principal). Off unless all four are set. */
  databricks?: PipelineStatusConfig;
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
    feedback: loadFeedbackConfig(env),
    dashboard: loadDashboardConfig(env),
    databricks: loadDatabricksConfig(env),
    dashboardTestDailyLimit: envInt(env, "DASHBOARD_TEST_DAILY_LIMIT", 10),
  };
}

function loadDatabricksConfig(env: Env): WorkerConfig["databricks"] {
  const host = envOptional(env, "DATABRICKS_HOST");
  const clientId = envOptional(env, "DATABRICKS_CLIENT_ID");
  const clientSecret = envOptional(env, "DATABRICKS_CLIENT_SECRET");
  const jobId = envOptional(env, "DATABRICKS_JOB_ID");
  return host && clientId && clientSecret && jobId && /^\d+$/.test(jobId) ? { host, clientId, clientSecret, jobId } : undefined;
}

function loadDashboardConfig(env: Env): WorkerConfig["dashboard"] {
  const teamDomain = envOptional(env, "CF_ACCESS_TEAM_DOMAIN");
  const aud = envOptional(env, "CF_ACCESS_AUD");
  const allowedEmails = (envOptional(env, "DASHBOARD_ALLOWED_EMAILS") ?? "")
    .split(",")
    .map((e) => e.trim().toLowerCase())
    .filter(Boolean);
  // Fail closed: no public listener unless every check can be enforced.
  if (!teamDomain || !aud || !allowedEmails.length) return undefined;
  return { port: envInt(env, "DASHBOARD_PORT", 8092), access: { teamDomain: normalizeTeamDomain(teamDomain), aud, allowedEmails } };
}

function loadFeedbackConfig(env: Env): WorkerConfig["feedback"] {
  const token = envOptional(env, "LINE_CHANNEL_ACCESS_TOKEN");
  const secret = envOptional(env, "LINE_CHANNEL_SECRET");
  if (envBool(env, "FEEDBACK_DISABLED") || !token || !secret || token === "unset" || secret === "unset") return undefined;
  // Default the feedback keys from WEBHOOK_SECRET so no extra secrets are required.
  const derive = (label: string) => createHmac("sha256", envString(env, "WEBHOOK_SECRET")).update(label).digest("hex");
  return {
    channelAccessToken: token,
    channelSecret: secret,
    lineApiBaseUrl: envString(env, "LINE_API_BASE_URL", "https://api.line.me"),
    publicPort: envInt(env, "PUBLIC_PORT", 8091),
    signingKey: envOptional(env, "FEEDBACK_SIGNING_KEY") ?? derive("feedback-postback-signing"),
    userSalt: envOptional(env, "FEEDBACK_USER_SALT") ?? derive("feedback-user-salt"),
    dbPath: envString(env, "FEEDBACK_DB_PATH", "./data/feedback.db"),
    modelLabel: envOptional(env, "OPENCLAW_MODEL_LABEL") ?? envOptional(env, "OPENCLAW_MODEL"),
  };
}
