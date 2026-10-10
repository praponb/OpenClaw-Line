import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { WorkerConfig } from "./config.js";
import type { Completion } from "./feedbackStore.js";
import type { Incident } from "./types.js";

export type DispatchResult = {
  ok: boolean;
  detail: string;
  /** Terminal run facts from `waitForCompletion` (hook mode only). Never contains model output. */
  completion?: Completion;
};

export interface Dispatcher {
  dispatch(incident: Incident, message: string): Promise<DispatchResult>;
}

/**
 * Triggers one isolated OpenClaw agent run per incident via `POST /hooks/agent`.
 * No sessionKey is sent, so every incident gets a fresh session (spec §5
 * "Session Bleed"); the Idempotency-Key makes retries safe.
 *
 * `waitForCompletion` keeps the request open until the run settles, so the
 * worker learns whether the report reached LINE (feedback card) or not
 * (fallback message). A retry after a dropped connection replays the same
 * run's completion instead of starting a second run.
 */
export class HookDispatcher implements Dispatcher {
  constructor(
    private readonly cfg: WorkerConfig["openclaw"],
    private readonly deps: { fetch?: typeof fetch; sleep?: (ms: number) => Promise<void>; retries?: number } = {},
  ) {}

  async dispatch(incident: Incident, message: string): Promise<DispatchResult> {
    const body: Record<string, unknown> = {
      message,
      name: `incident ${incident.dagId}/${incident.root.task_id}`,
      agentId: this.cfg.agentId,
      sessionMode: "isolated",
      deliver: true,
      channel: this.cfg.channel,
      to: this.cfg.to,
      timeoutSeconds: this.cfg.timeoutSeconds,
      waitForCompletion: true,
    };
    if (this.cfg.accountId) body.accountId = this.cfg.accountId;
    if (this.cfg.model) body.model = this.cfg.model;

    const url = `${this.cfg.gatewayUrl.replace(/\/+$/, "")}/hooks/agent`;
    const retries = this.deps.retries ?? 3;
    const sleep = this.deps.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    let last = "";
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const res = await (this.deps.fetch ?? fetch)(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${this.cfg.hookToken}`,
            "Idempotency-Key": `incident:${incident.key}`,
          },
          body: JSON.stringify(body),
          // The run may take up to timeoutSeconds; allow for queueing and delivery.
          signal: AbortSignal.timeout((this.cfg.timeoutSeconds + 120) * 1000),
        });
        const text = await res.text();
        if (res.ok) return { ok: true, detail: `HTTP ${res.status} ${text.slice(0, 500)}`, completion: parseCompletion(text) };
        last = `HTTP ${res.status} ${text.slice(0, 500)}`;
        // 4xx (except 429) means the request itself is wrong: do not retry.
        if (res.status < 500 && res.status !== 429) return { ok: false, detail: last };
      } catch (err) {
        last = (err as Error).message;
      }
      if (attempt < retries) await sleep(1000 * 2 ** (attempt - 1));
    }
    return { ok: false, detail: last };
  }
}

function parseCompletion(text: string): Completion | undefined {
  try {
    const c = (JSON.parse(text) as { completion?: Record<string, unknown> }).completion;
    if (!c || typeof c !== "object") return undefined;
    const str = (v: unknown) => (typeof v === "string" ? v : undefined);
    return {
      status: str(c.status),
      delivered: typeof c.delivered === "boolean" ? c.delivered : undefined,
      replyDisposition: str(c.replyDisposition),
      deliveryError: str(c.deliveryError),
    };
  } catch {
    return undefined;
  }
}

/**
 * DRY_RUN: write the agent input to disk instead of calling the Gateway.
 * `demo/run_dry.sh` then runs the investigator on it with `openclaw agent`
 * and prints the report to the terminal (no LINE credentials needed).
 */
export class FileDispatcher implements Dispatcher {
  constructor(private readonly dir: string) {}

  async dispatch(incident: Incident, message: string): Promise<DispatchResult> {
    await mkdir(this.dir, { recursive: true });
    const file = join(this.dir, `${incident.key.replace(/[^A-Za-z0-9._-]+/g, "_")}.txt`);
    await writeFile(file, message, "utf8");
    return { ok: true, detail: `wrote ${file}` };
  }
}
