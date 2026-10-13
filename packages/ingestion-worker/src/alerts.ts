import { sanitize } from "@openclaw-line/shared";

/**
 * Alerting through Healthchecks.io (or any service with the same ping API).
 *
 * - Heartbeat check: the worker pings it every few minutes. Missed pings mean the
 *   worker, the server or its internet connection is down; a `/fail` ping means the
 *   worker runs but something it depends on is broken (gateway not ready, backup failed).
 * - LINE check: `/fail` when a LINE push fails, a success ping when one goes through.
 *
 * Healthchecks.io emails on every up/down change. Ping bodies carry incident refs and
 * error text only: no report text, DAG names or LINE user ids.
 */
export type AlertsConfig = { heartbeatUrl?: string; lineUrl?: string };

export type Logger = Pick<Console, "info" | "warn">;

const MAX_BODY_CHARS = 2_000;

export class Alerts {
  constructor(
    private readonly cfg: AlertsConfig,
    private readonly deps: { fetch?: typeof fetch; log?: Logger } = {},
  ) {}

  get enabled(): boolean {
    return Boolean(this.cfg.heartbeatUrl || this.cfg.lineUrl);
  }

  /** `problems` empty = healthy. */
  heartbeat(problems: string[]): Promise<void> {
    return this.ping(this.cfg.heartbeatUrl, problems.length > 0, problems.length ? problems.join("\n") : "ok");
  }

  lineFailed(detail: string): Promise<void> {
    return this.ping(this.cfg.lineUrl, true, detail);
  }

  lineOk(detail: string): Promise<void> {
    return this.ping(this.cfg.lineUrl, false, detail);
  }

  /** Never throws: a broken alert channel must not break the worker. */
  private async ping(url: string | undefined, fail: boolean, body: string): Promise<void> {
    if (!url) return;
    const target = `${url.replace(/\/+$/, "")}${fail ? "/fail" : ""}`;
    try {
      const res = await (this.deps.fetch ?? fetch)(target, {
        method: "POST",
        headers: { "Content-Type": "text/plain; charset=utf-8" },
        body: sanitize(body).slice(0, MAX_BODY_CHARS),
        signal: AbortSignal.timeout(10_000),
      });
      if (!res.ok) (this.deps.log ?? console).warn(`[alerts] ping returned HTTP ${res.status}`);
    } catch (err) {
      (this.deps.log ?? console).warn(`[alerts] ping failed: ${sanitize((err as Error).message)}`);
    }
  }
}

/** Gateway readiness for the heartbeat: `/readyz` is false while agents or the LINE channel are unusable. */
export async function gatewayProblem(gatewayUrl: string, deps: { fetch?: typeof fetch } = {}): Promise<string | undefined> {
  try {
    const res = await (deps.fetch ?? fetch)(`${gatewayUrl.replace(/\/+$/, "")}/readyz`, { signal: AbortSignal.timeout(10_000) });
    return res.ok ? undefined : `OpenClaw gateway not ready (HTTP ${res.status})`;
  } catch (err) {
    return `OpenClaw gateway unreachable: ${sanitize((err as Error).message)}`;
  }
}
