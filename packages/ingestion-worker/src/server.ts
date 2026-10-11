import { createHash } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { AirflowClient, filterLog, sanitize, type TaskDefinition, type TaskInstance } from "@openclaw-line/shared";
import { buildIncidentMessage } from "./buildMessage.js";
import type { WorkerConfig } from "./config.js";
import { FailureAggregator, selectRootTask } from "./dedupe.js";
import type { DispatchResult, Dispatcher } from "./dispatcher.js";
import { AccessVerifier, accessToken } from "./cfAccess.js";
import { DASHBOARD_CSP, buildDashboardData, dashboardHtml } from "./dashboard.js";
import { PipelineStatus } from "./pipelineStatus.js";
import { CostTracker } from "./costs.js";
import { PipelineTrigger } from "./pipelineTrigger.js";
import { ResolutionWatcher } from "./resolutionWatcher.js";
import { TestNotifier, isSameOriginAction } from "./testNotification.js";
import { fallbackMessage, feedbackCard } from "./feedback.js";
import { FeedbackStore } from "./feedbackStore.js";
import { INCIDENT_REF_RE, incidentRef } from "./incidentRef.js";
import { LINE_SIGNATURE_HEADER, LineClient, verifyLineSignature, type LineWebhookEvent } from "./line.js";
import { handleLineEvents } from "./lineWebhook.js";
import { SIGNATURE_HEADER, TIMESTAMP_HEADER, verifySignature } from "./signature.js";
import type { FailureEvent, Incident } from "./types.js";

const MAX_BODY_BYTES = 64 * 1024;
const MAX_REPORT_BODY_BYTES = 256 * 1024;
const MAX_REPORT_CHARS = 20_000;
const LINE_EVENT_RETENTION_MS = 7 * 24 * 3600 * 1000;

export type Logger = Pick<Console, "info" | "warn" | "error">;

export type Worker = {
  /** Internal listener: Airflow callbacks, report capture, health. Never exposed publicly. */
  server: Server;
  /** Public listener for the LINE webhook only (present when feedback is configured). */
  publicServer?: Server;
  /** Public dashboard listener behind Cloudflare Access (present when CF_ACCESS_* is configured). */
  dashboardServer?: Server;
  aggregator: FailureAggregator;
  investigate: (key: string, events: FailureEvent[]) => Promise<void>;
  store?: FeedbackStore;
  /** MTTR auto-resolution (present when both the incident log and Airflow are configured). */
  resolutionWatcher?: ResolutionWatcher;
};

export type WorkerDeps = {
  airflow?: AirflowClient;
  log?: Logger;
  line?: LineClient;
  store?: FeedbackStore;
  now?: () => number;
  accessVerifier?: Pick<AccessVerifier, "verify">;
  pipeline?: Pick<PipelineStatus, "get">;
  trigger?: Pick<PipelineTrigger, "trigger" | "dagId">;
};

export function createWorker(cfg: WorkerConfig, dispatcher: Dispatcher, deps: WorkerDeps = {}): Worker {
  const log = deps.log ?? console;
  const now = deps.now ?? Date.now;
  const airflow = deps.airflow ?? (cfg.airflow ? new AirflowClient(cfg.airflow) : undefined);
  const fb = cfg.feedback;
  const store = deps.store ?? (fb ? new FeedbackStore(fb.dbPath) : undefined);
  const line = deps.line ?? (fb ? new LineClient(fb.channelAccessToken, { baseUrl: fb.lineApiBaseUrl }) : undefined);
  const pipeline = deps.pipeline ?? (cfg.databricks ? new PipelineStatus(cfg.databricks, { now }) : undefined);
  const costs = new CostTracker({ line, pipeline: cfg.databricks && pipeline instanceof PipelineStatus ? pipeline : undefined, now });
  const trigger = deps.trigger ?? (cfg.pipelineTrigger ? new PipelineTrigger(cfg.pipelineTrigger, { now }) : undefined);
  const testNotifier = new TestNotifier(line, cfg.openclaw.to, { now, dailyLimit: cfg.dashboardTestDailyLimit });

  async function investigate(key: string, events: FailureEvent[]): Promise<void> {
    const first = events[0]!;
    let instances: TaskInstance[] = [];
    let tasks: TaskDefinition[] = [];
    if (airflow) {
      try {
        [instances, tasks] = await Promise.all([airflow.listTaskInstances(first.dag_id, first.run_id), airflow.listTasks(first.dag_id)]);
      } catch (err) {
        log.warn(`[worker] ${key}: could not read run state from Airflow, using callbacks only: ${sanitize((err as Error).message)}`);
      }
    }
    const sel = selectRootTask(events, instances, tasks);
    const rootEvent = events.find((e) => e.task_id === sel.root) ?? { ...first, task_id: sel.root, exception: undefined, log_url: undefined };

    const incident: Incident = {
      key,
      ref: incidentRef(key),
      dagId: first.dag_id,
      runId: first.run_id,
      root: rootEvent,
      failedDescendants: sel.failedDescendants,
      upstreamFailed: sel.upstreamFailed,
      otherFailures: sel.otherFailures,
    };
    const rootTi = instances.find((i) => i.task_id === sel.root);
    if (rootTi?.start_date) {
      incident.rootStartedAt = rootTi.start_date;
      const end = rootTi.end_date ? Date.parse(rootTi.end_date) : NaN;
      if (Number.isFinite(end)) incident.rootDurationSeconds = Math.max(0, Math.round((end - Date.parse(rootTi.start_date)) / 1000));
    }

    if (airflow) {
      try {
        const { text } = await airflow.getTaskLog(first.dag_id, first.run_id, sel.root, rootEvent.try_number);
        const filtered = filterLog(text, { tailLines: cfg.logTailLines, maxChars: cfg.maxLogChars });
        incident.logExcerpt = sanitize(filtered.text);
        incident.logNote = `${filtered.keptLines}/${filtered.totalLines} lines kept: tail ${cfg.logTailLines} + ERROR/Traceback lines`;
      } catch (err) {
        incident.logNote = `log fetch failed: ${sanitize((err as Error).message)}`;
      }
    } else {
      incident.logNote = "Airflow API not configured in the ingestion worker";
    }

    const receivedTimes = events.map((e) => e.receivedAt).filter((t): t is number => t !== undefined);
    store?.recordDispatch({
      ref: incident.ref,
      incidentKey: key,
      dagId: incident.dagId,
      taskId: sel.root,
      runId: incident.runId,
      firstSeenAt: receivedTimes.length ? Math.min(...receivedTimes) : undefined,
      dispatchedAt: now(),
      model: cfg.openclaw.model ?? fb?.modelLabel,
    });

    const message = buildIncidentMessage(incident);
    const result = await dispatcher.dispatch(incident, message);
    const summary = `ref=${incident.ref} root=${sel.root} descendants=${sel.failedDescendants.length} upstream_failed=${sel.upstreamFailed.length} events=${events.length}`;
    if (result.ok) log.info(`[worker] ${key}: dispatched (${summary}) ${result.detail}`);
    else log.error(`[worker] ${key}: dispatch FAILED (${summary}) ${result.detail}`);

    if (result.runId) store?.setAgentRunId(incident.ref, result.runId);
    if (result.completion) store?.recordCompletion(incident.ref, result.completion, now());
    if (!cfg.dryRun) await followUp(incident, result);
  }

  /** After the run settles: feedback card if the report reached LINE, otherwise a fallback notice. */
  async function followUp(incident: Incident, result: DispatchResult): Promise<void> {
    if (!fb || !line) return;
    const c = result.completion;
    const card = { ref: incident.ref, dagId: incident.dagId, taskId: incident.root.task_id };
    try {
      if (result.ok && c?.status === "ok" && c.delivered === true) {
        await line.push(cfg.openclaw.to, [feedbackCard(fb.signingKey, card)], retryKey(`card:${incident.key}`));
        log.info(`[worker] ${incident.key}: feedback card sent (${incident.ref})`);
        return;
      }
      if (result.ok && !c) return; // gateway did not report completion: nothing reliable to say
      const reason = !result.ok
        ? "the investigator could not be started"
        : c?.status !== "ok"
          ? `investigation ended with status "${c?.status ?? "unknown"}"`
          : c?.replyDisposition && c.replyDisposition !== "visible"
            ? "the investigator produced no report"
            : "the report could not be delivered to LINE";
      await line.push(cfg.openclaw.to, [fallbackMessage({ ...card, runId: incident.runId, logUrl: incident.root.log_url }, reason)], retryKey(`fallback:${incident.key}`));
      log.warn(`[worker] ${incident.key}: fallback notice sent (${incident.ref}): ${reason}`);
    } catch (err) {
      log.error(`[worker] ${incident.key}: follow-up message failed: ${sanitize((err as Error).message)}`);
    }
  }

  const aggregator = new FailureAggregator({
    windowMs: cfg.dedupeWindowMs,
    ttlMs: cfg.dedupeTtlMs,
    onFlush: (key, events) =>
      investigate(key, events).catch((err) => log.error(`[worker] ${key}: investigation crashed: ${sanitize(String(err))}`)),
  });

  /** Dashboard page + API. Returns false when the request is not a dashboard route. */
  function serveDashboard(req: IncomingMessage, res: ServerResponse, send: ReturnType<typeof jsonSender>, who = "internal"): boolean {
    const path = new URL(req.url ?? "/", "http://localhost").pathname;
    if (path === "/api/trigger-pipeline") {
      if (req.method === "GET") send(200, trigger ? { enabled: true, dagId: trigger.dagId } : { enabled: false });
      else if (req.method !== "POST") send(405, { error: "method not allowed" });
      else if (!isSameOriginAction(req.headers, "run-pipeline")) send(403, { error: "forbidden" });
      else if (!trigger) send(503, { ok: false, error: "Pipeline trigger is not configured (AIRFLOW_TRIGGER_PASSWORD)." });
      else {
        const fault = new URL(req.url ?? "/", "http://localhost").searchParams.get("fault") ?? "none";
        trigger.trigger(fault).then(
          (r) => {
            log.info(`[dashboard] pipeline run requested by ${who} (fault=${fault.slice(0, 20)}): ${r.ok ? `started ${r.dagRunId}` : `${r.status} ${r.error}`}`);
            send(r.ok ? 200 : r.status, r);
          },
          () => send(500, { error: "internal error" }),
        );
      }
      return true;
    }
    if (path === "/api/test-notification") {
      if (req.method !== "POST") send(405, { error: "method not allowed" });
      else if (!isSameOriginAction(req.headers)) send(403, { error: "forbidden" });
      else
        testNotifier.send().then(
          (r) => {
            log.info(`[dashboard] test notification by ${who}: ${r.ok ? "sent" : `${r.status} ${r.error}`}`);
            send(r.ok ? 200 : r.status, r);
          },
          () => send(500, { error: "internal error" }),
        );
      return true;
    }
    if (req.method !== "GET") return false;
    if (path === "/" || path === "/dashboard") {
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Content-Security-Policy": DASHBOARD_CSP, "X-Content-Type-Options": "nosniff", "Cache-Control": "no-store" });
      res.end(dashboardHtml());
      return true;
    }
    if (path === "/api/dashboard") {
      handleDashboardApi(req, send);
      return true;
    }
    if (path === "/api/costs") {
      costs.get().then((d) => send(200, d), () => send(500, { error: "internal error" }));
      return true;
    }
    if (path === "/api/pipeline") {
      if (!pipeline) send(200, { enabled: false });
      else pipeline.get().then((d) => send(200, d), () => send(500, { error: "internal error" }));
      return true;
    }
    return false;
  }

  const server = createServer(async (req, res) => {
    const send = jsonSender(res);
    try {
      if (req.method === "GET" && req.url === "/healthz") return send(200, { ok: true, pending: aggregator.pendingCount });
      if (serveDashboard(req, res, send)) return;
      if (req.url === "/v1/openclaw/report") return await handleReport(req, send);
      if (req.url === "/v1/openclaw/usage") return await handleUsage(req, send);
      if (req.url !== "/v1/airflow/failure") return send(404, { error: "not found" });
      if (req.method !== "POST") return send(405, { error: "method not allowed" });

      const raw = await readBody(req, MAX_BODY_BYTES);
      if (raw === undefined) return send(413, { error: "payload too large" });
      const body = raw.toString("utf8");
      if (!verifySignature(cfg.webhookSecret, body, header(req, TIMESTAMP_HEADER), header(req, SIGNATURE_HEADER))) {
        return send(401, { error: "invalid signature" });
      }
      let event: FailureEvent;
      try {
        event = { ...parseEvent(JSON.parse(body)), receivedAt: now() };
      } catch (err) {
        return send(400, { error: (err as Error).message });
      }
      const outcome = aggregator.add(event);
      log.info(`[worker] ${FailureAggregator.key(event)} task=${event.task_id}: ${outcome}`);
      return send(202, { accepted: true, outcome });
    } catch (err) {
      log.error(`[worker] request failed: ${sanitize(String(err))}`);
      return send(500, { error: "internal error" });
    }
  });

  /** Data for the live dashboard. Internal listener only: it includes report text. */
  function handleDashboardApi(req: IncomingMessage, send: ReturnType<typeof jsonSender>): void {
    if (!store) return send(200, { enabled: false });
    const days = Number(new URL(req.url ?? "/", "http://localhost").searchParams.get("days") ?? 30);
    if (!Number.isInteger(days) || days < 1 || days > 3650) return send(400, { error: "days must be an integer between 1 and 3650" });
    const since = now() - days * 24 * 3600 * 1000;
    return send(200, buildDashboardData(store.incidentsSince(since), store.feedbackSince(since), { days, now: now(), usage: store.usageSince(since), prices: cfg.modelPrices }));
  }

  /** Per-call token usage from the report-capture plugin's llm_output hook (signed like Airflow callbacks). */
  async function handleUsage(req: IncomingMessage, send: ReturnType<typeof jsonSender>): Promise<void> {
    if (req.method !== "POST") return send(405, { error: "method not allowed" });
    const raw = await readBody(req, MAX_BODY_BYTES);
    if (raw === undefined) return send(413, { error: "payload too large" });
    const body = raw.toString("utf8");
    if (!verifySignature(cfg.webhookSecret, body, header(req, TIMESTAMP_HEADER), header(req, SIGNATURE_HEADER))) {
      return send(401, { error: "invalid signature" });
    }
    if (!store) return send(202, { recorded: false, reason: "incident log disabled" });
    let u: Record<string, unknown>;
    try {
      u = JSON.parse(body) as Record<string, unknown>;
    } catch {
      return send(400, { error: "invalid JSON" });
    }
    const n = (v: unknown) => (typeof v === "number" && Number.isFinite(v) && v >= 0 ? Math.min(Math.round(v), 1e9) : 0);
    const str = (v: unknown, max: number) => (typeof v === "string" ? v.slice(0, max) : undefined);
    const runId = str(u.runId, 100);
    if (!runId || !/^[A-Za-z0-9_-]+$/.test(runId)) return send(400, { error: "missing or invalid runId" });
    const sessionKey = validSessionKey(u.sessionKey);
    const ref = typeof u.ref === "string" && INCIDENT_REF_RE.test(u.ref) ? u.ref : undefined;
    if (ref && sessionKey && store.getIncident(ref)) store.linkUsageSession(ref, sessionKey);
    store.recordModelUsage(
      { runId, sessionKey, provider: str(u.provider, 60), model: str(u.model, 100), input: n(u.input), output: n(u.output), cacheRead: n(u.cacheRead), cacheWrite: n(u.cacheWrite) },
      now(),
    );
    return send(202, { recorded: true });
  }

  /** Report text captured by the report-capture OpenClaw plugin (signed like Airflow callbacks). */
  async function handleReport(req: IncomingMessage, send: ReturnType<typeof jsonSender>): Promise<void> {
    if (req.method !== "POST") return send(405, { error: "method not allowed" });
    const raw = await readBody(req, MAX_REPORT_BODY_BYTES);
    if (raw === undefined) return send(413, { error: "payload too large" });
    const body = raw.toString("utf8");
    if (!verifySignature(cfg.webhookSecret, body, header(req, TIMESTAMP_HEADER), header(req, SIGNATURE_HEADER))) {
      return send(401, { error: "invalid signature" });
    }
    if (!store) return send(202, { stored: false, reason: "feedback disabled" });
    let r: { ref?: unknown; content?: unknown; messageId?: unknown; promptVersion?: unknown; success?: unknown };
    try {
      r = JSON.parse(body);
    } catch {
      return send(400, { error: "invalid JSON" });
    }
    if (typeof r.ref !== "string" || !INCIDENT_REF_RE.test(r.ref) || typeof r.content !== "string") {
      return send(400, { error: "ref and content are required" });
    }
    if (r.success === false) return send(202, { stored: false, reason: "delivery failed" });
    const stored = store.recordReport(r.ref, {
      text: r.content.slice(0, MAX_REPORT_CHARS),
      sentAt: now(),
      messageId: typeof r.messageId === "string" ? r.messageId.slice(0, 100) : undefined,
      promptVersion: typeof r.promptVersion === "string" ? r.promptVersion.slice(0, 64) : undefined,
    });
    if (!stored) log.warn(`[worker] report for unknown ref ${r.ref} ignored`);
    return send(stored ? 200 : 404, { stored });
  }

  let publicServer: Server | undefined;
  if (fb && store && line) {
    const feedbackDeps = { store, line, signingKey: fb.signingKey, userSalt: fb.userSalt, log, now };
    publicServer = createServer(async (req, res) => {
      const send = jsonSender(res);
      try {
        if (req.url !== "/line/webhook") return send(404, { error: "not found" });
        if (req.method !== "POST") return send(405, { error: "method not allowed" });
        const raw = await readBody(req, MAX_BODY_BYTES);
        if (raw === undefined) return send(413, { error: "payload too large" });
        if (!verifyLineSignature(fb.channelSecret, raw, header(req, LINE_SIGNATURE_HEADER))) return send(401, { error: "invalid signature" });
        let events: LineWebhookEvent[];
        try {
          const parsed = JSON.parse(raw.toString("utf8")) as { events?: unknown };
          events = Array.isArray(parsed.events) ? (parsed.events as LineWebhookEvent[]) : [];
        } catch {
          return send(400, { error: "invalid JSON" });
        }
        await handleLineEvents(feedbackDeps, events);
        store.pruneEvents(now() - LINE_EVENT_RETENTION_MS);
        return send(200, { ok: true });
      } catch (err) {
        log.error(`[feedback] webhook failed: ${sanitize(String(err))}`);
        return send(500, { error: "internal error" });
      }
    });
  }

  // Public dashboard (dashboard.<domain> via the tunnel): dashboard routes only,
  // and every request must carry a valid Cloudflare Access token for an allowed email.
  let dashboardServer: Server | undefined;
  if (cfg.dashboard) {
    const verifier = deps.accessVerifier ?? new AccessVerifier(cfg.dashboard.access);
    dashboardServer = createServer(async (req, res) => {
      const send = jsonSender(res);
      try {
        const result = await verifier.verify(accessToken(req.headers));
        if (!result.ok) {
          log.warn(`[dashboard] denied ${req.method} ${new URL(req.url ?? "/", "http://localhost").pathname}: ${result.reason}`);
          res.writeHead(403, { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" });
          return void res.end("Forbidden: sign in through Cloudflare Access with an allowed account.\n");
        }
        if (!serveDashboard(req, res, send, result.email)) send(404, { error: "not found" });
      } catch (err) {
        log.error(`[dashboard] request failed: ${sanitize(String(err))}`);
        send(500, { error: "internal error" });
      }
    });
  }

  const resolutionWatcher = store && airflow ? new ResolutionWatcher(store, airflow, { now, log }) : undefined;

  return { server, publicServer, dashboardServer, aggregator, investigate, store, resolutionWatcher };
}

function retryKey(seed: string): string {
  // LINE wants a UUID; derive a stable one so a retried push is not sent twice.
  const h = createHash("sha256").update(seed).digest("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-a${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

function jsonSender(res: ServerResponse) {
  return (status: number, body: unknown): void => {
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(JSON.stringify(body));
  };
}

/** OpenClaw session keys look like "agent:pipeline-investigator:hook:<uuid>"; keep them short and printable. */
function validSessionKey(v: unknown): string | undefined {
  return typeof v === "string" && v.length <= 200 && /^[A-Za-z0-9:._@/-]+$/.test(v) ? v : undefined;
}

export function parseEvent(body: unknown): FailureEvent {
  if (!body || typeof body !== "object") throw new Error("body must be a JSON object");
  const b = body as Record<string, unknown>;
  const str = (k: string, required: boolean): string | undefined => {
    const v = b[k];
    if (v === undefined || v === null || v === "") {
      if (required) throw new Error(`missing field ${k}`);
      return undefined;
    }
    if (typeof v !== "string" || v.length > 2_000) throw new Error(`field ${k} must be a string`);
    return v;
  };
  const tryNumber = b.try_number;
  return {
    dag_id: str("dag_id", true)!,
    task_id: str("task_id", true)!,
    run_id: str("run_id", true)!,
    try_number: typeof tryNumber === "number" && Number.isInteger(tryNumber) && tryNumber > 0 ? tryNumber : undefined,
    logical_date: str("logical_date", false),
    log_url: str("log_url", false),
    log_filepath: str("log_filepath", false)?.match(/^[^\0\r\n]+$/)?.[0],
    // Exceptions can be long; keep the head (sanitized again before use).
    exception: typeof b.exception === "string" ? b.exception.slice(0, 4_000) : undefined,
    // Only well-formed values reach the agent prompt.
    databricks_run_id: str("databricks_run_id", false)?.match(/^\d{1,20}$/)?.[0],
    databricks_run_url: str("databricks_run_url", false)?.match(/^https:\/\/[^\s<>"]+$/)?.[0],
  };
}

function header(req: IncomingMessage, name: string): string | undefined {
  const v = req.headers[name];
  return Array.isArray(v) ? v[0] : v;
}

function readBody(req: IncomingMessage, maxBytes: number): Promise<Buffer | undefined> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let tooBig = false;
    req.on("data", (c: Buffer) => {
      size += c.length;
      if (size > maxBytes) tooBig = true;
      else chunks.push(c);
    });
    req.on("end", () => resolve(tooBig ? undefined : Buffer.concat(chunks)));
    req.on("error", reject);
  });
}
