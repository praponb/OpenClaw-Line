import { createServer, type IncomingMessage, type Server } from "node:http";
import { AirflowClient, filterLog, sanitize, type TaskDefinition, type TaskInstance } from "@openclaw-line/shared";
import { buildIncidentMessage } from "./buildMessage.js";
import type { WorkerConfig } from "./config.js";
import { FailureAggregator, selectRootTask } from "./dedupe.js";
import type { Dispatcher } from "./dispatcher.js";
import { SIGNATURE_HEADER, TIMESTAMP_HEADER, verifySignature } from "./signature.js";
import type { FailureEvent, Incident } from "./types.js";

const MAX_BODY_BYTES = 64 * 1024;

export type Logger = Pick<Console, "info" | "warn" | "error">;

export type Worker = {
  server: Server;
  aggregator: FailureAggregator;
  investigate: (key: string, events: FailureEvent[]) => Promise<void>;
};

export function createWorker(cfg: WorkerConfig, dispatcher: Dispatcher, deps: { airflow?: AirflowClient; log?: Logger } = {}): Worker {
  const log = deps.log ?? console;
  const airflow = deps.airflow ?? (cfg.airflow ? new AirflowClient(cfg.airflow) : undefined);

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
      dagId: first.dag_id,
      runId: first.run_id,
      root: rootEvent,
      failedDescendants: sel.failedDescendants,
      upstreamFailed: sel.upstreamFailed,
      otherFailures: sel.otherFailures,
    };

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

    const message = buildIncidentMessage(incident);
    const result = await dispatcher.dispatch(incident, message);
    const summary = `root=${sel.root} descendants=${sel.failedDescendants.length} upstream_failed=${sel.upstreamFailed.length} events=${events.length}`;
    if (result.ok) log.info(`[worker] ${key}: dispatched (${summary}) ${result.detail}`);
    else log.error(`[worker] ${key}: dispatch FAILED (${summary}) ${result.detail}`);
  }

  const aggregator = new FailureAggregator({
    windowMs: cfg.dedupeWindowMs,
    ttlMs: cfg.dedupeTtlMs,
    onFlush: (key, events) =>
      investigate(key, events).catch((err) => log.error(`[worker] ${key}: investigation crashed: ${sanitize(String(err))}`)),
  });

  const server = createServer(async (req, res) => {
    const send = (status: number, body: unknown) => {
      res.writeHead(status, { "Content-Type": "application/json" });
      res.end(JSON.stringify(body));
    };
    try {
      if (req.method === "GET" && req.url === "/healthz") return send(200, { ok: true, pending: aggregator.pendingCount });
      if (req.url !== "/v1/airflow/failure") return send(404, { error: "not found" });
      if (req.method !== "POST") return send(405, { error: "method not allowed" });

      const raw = await readBody(req);
      if (raw === undefined) return send(413, { error: "payload too large" });
      if (!verifySignature(cfg.webhookSecret, raw, header(req, TIMESTAMP_HEADER), header(req, SIGNATURE_HEADER))) {
        return send(401, { error: "invalid signature" });
      }
      let event: FailureEvent;
      try {
        event = parseEvent(JSON.parse(raw));
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

  return { server, aggregator, investigate };
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
    // Exceptions can be long; keep the head (sanitized again before use).
    exception: typeof b.exception === "string" ? b.exception.slice(0, 4_000) : undefined,
  };
}

function header(req: IncomingMessage, name: string): string | undefined {
  const v = req.headers[name];
  return Array.isArray(v) ? v[0] : v;
}

function readBody(req: IncomingMessage): Promise<string | undefined> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let tooBig = false;
    req.on("data", (c: Buffer) => {
      size += c.length;
      if (size > MAX_BODY_BYTES) tooBig = true;
      else chunks.push(c);
    });
    req.on("end", () => resolve(tooBig ? undefined : Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}
