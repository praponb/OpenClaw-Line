import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { WorkerConfig } from "./config.js";
import type { Incident } from "./types.js";

export interface Dispatcher {
  dispatch(incident: Incident, message: string): Promise<{ ok: boolean; detail: string }>;
}

/**
 * Triggers one isolated OpenClaw agent run per incident via `POST /hooks/agent`.
 * No sessionKey is sent, so every incident gets a fresh session (spec §5
 * "Session Bleed"); the Idempotency-Key makes retries safe.
 */
export class HookDispatcher implements Dispatcher {
  constructor(
    private readonly cfg: WorkerConfig["openclaw"],
    private readonly deps: { fetch?: typeof fetch; sleep?: (ms: number) => Promise<void>; retries?: number } = {},
  ) {}

  async dispatch(incident: Incident, message: string): Promise<{ ok: boolean; detail: string }> {
    const body: Record<string, unknown> = {
      message,
      name: `incident ${incident.dagId}/${incident.root.task_id}`,
      agentId: this.cfg.agentId,
      sessionMode: "isolated",
      deliver: true,
      channel: this.cfg.channel,
      to: this.cfg.to,
      timeoutSeconds: this.cfg.timeoutSeconds,
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
          signal: AbortSignal.timeout(30_000),
        });
        const text = (await res.text()).slice(0, 500);
        if (res.ok) return { ok: true, detail: `HTTP ${res.status} ${text}` };
        last = `HTTP ${res.status} ${text}`;
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

/**
 * DRY_RUN: write the agent input to disk instead of calling the Gateway.
 * `demo/run_dry.sh` then runs the investigator on it with `openclaw agent`
 * and prints the report to the terminal (no LINE credentials needed).
 */
export class FileDispatcher implements Dispatcher {
  constructor(private readonly dir: string) {}

  async dispatch(incident: Incident, message: string): Promise<{ ok: boolean; detail: string }> {
    await mkdir(this.dir, { recursive: true });
    const file = join(this.dir, `${incident.key.replace(/[^A-Za-z0-9._-]+/g, "_")}.txt`);
    await writeFile(file, message, "utf8");
    return { ok: true, detail: `wrote ${file}` };
  }
}
