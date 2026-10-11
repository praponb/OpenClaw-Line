/**
 * "▶ Run pipeline" on the dashboard: starts one run of the Airflow DAG that
 * drives the Databricks job, so failures still go through the normal
 * callback -> investigator -> LINE path.
 *
 * Uses its own Airflow account (role "user", may create DAG runs). The
 * investigator and the worker's log reads keep the read-only "viewer" account;
 * this credential lives only in the worker. Rate-limited because every run
 * costs Databricks compute (and, if it fails, a model call and LINE messages).
 */

export type TriggerConfig = { baseUrl: string; username: string; password: string; dagId: string; dailyLimit: number };

export type TriggerResult =
  | { ok: true; dagRunId: string; fault: string; remainingToday: number }
  | { ok: false; status: number; error: string; retryAfterSeconds?: number };

export const FAULTS = ["none", "schema_drift", "null_amounts", "empty"] as const;
const COOLDOWN_MS = 60_000;
const BANGKOK_OFFSET_MS = 7 * 3600_000;
const bangkokDay = (ms: number) => new Date(ms + BANGKOK_OFFSET_MS).toISOString().slice(0, 10);

export class PipelineTrigger {
  private token: { value: string; expiresAt: number } | undefined;
  private lastAt = 0;
  private day = "";
  private count = 0;

  constructor(
    private readonly cfg: TriggerConfig,
    private readonly deps: { now: () => number; fetch?: typeof fetch },
  ) {}

  get dagId(): string {
    return this.cfg.dagId;
  }

  async trigger(fault: string): Promise<TriggerResult> {
    if (!(FAULTS as readonly string[]).includes(fault)) return { ok: false, status: 400, error: `unknown fault "${fault.slice(0, 40)}"` };
    const now = this.deps.now();
    if (bangkokDay(now) !== this.day) {
      this.day = bangkokDay(now);
      this.count = 0;
    }
    if (now - this.lastAt < COOLDOWN_MS) {
      const wait = Math.ceil((COOLDOWN_MS - (now - this.lastAt)) / 1000);
      return { ok: false, status: 429, error: `Please wait ${wait}s before starting another run.`, retryAfterSeconds: wait };
    }
    if (this.count >= this.cfg.dailyLimit) {
      return { ok: false, status: 429, error: `Daily limit of ${this.cfg.dailyLimit} dashboard runs reached. Try again tomorrow (Bangkok time).` };
    }
    try {
      const dag = encodeURIComponent(this.cfg.dagId);
      const info = await this.api<{ is_paused?: boolean }>("GET", `/dags/${dag}`);
      if (info.is_paused) return { ok: false, status: 409, error: `DAG ${this.cfg.dagId} is paused in Airflow; unpause it first.` };
      const active = await this.api<{ dag_runs?: { dag_run_id: string; state: string }[] }>("GET", `/dags/${dag}/dagRuns?state=queued&state=running&limit=5`);
      const busy = (active.dag_runs ?? []).find((r) => r.state === "running" || r.state === "queued");
      if (busy) return { ok: false, status: 409, error: `A run is already ${busy.state} (${busy.dag_run_id}). Wait for it to finish.` };
      // Count the attempt before triggering so double-clicks can't slip past the limits.
      this.lastAt = now;
      this.count++;
      const run = await this.api<{ dag_run_id?: string }>("POST", `/dags/${dag}/dagRuns`, { logical_date: null, conf: { inject_fault: fault } });
      return { ok: true, dagRunId: run.dag_run_id ?? "?", fault, remainingToday: this.cfg.dailyLimit - this.count };
    } catch (err) {
      return { ok: false, status: 502, error: (err as Error).message.slice(0, 300) };
    }
  }

  private async api<T>(method: "GET" | "POST", path: string, body?: unknown): Promise<T> {
    const doFetch = this.deps.fetch ?? fetch;
    const base = this.cfg.baseUrl.replace(/\/+$/, "");
    const res = await doFetch(`${base}/api/v2${path}`, {
      method,
      headers: { Authorization: `Bearer ${await this.bearer()}`, Accept: "application/json", ...(body ? { "Content-Type": "application/json" } : {}) },
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(15_000),
    });
    if (!res.ok) throw new Error(`Airflow API ${res.status} on ${method} ${path.split("?")[0]}: ${(await res.text().catch(() => "")).slice(0, 200)}`);
    return (await res.json()) as T;
  }

  private async bearer(): Promise<string> {
    if (this.token && this.token.expiresAt > this.deps.now() + 30_000) return this.token.value;
    const res = await (this.deps.fetch ?? fetch)(`${this.cfg.baseUrl.replace(/\/+$/, "")}/auth/token`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ username: this.cfg.username, password: this.cfg.password }),
      signal: AbortSignal.timeout(15_000),
    });
    if (!res.ok) throw new Error(`Airflow login for the dashboard trigger account failed (HTTP ${res.status})`);
    const t = (await res.json()) as { access_token?: string };
    if (!t.access_token) throw new Error("Airflow login returned no access_token");
    this.token = { value: t.access_token, expiresAt: this.deps.now() + 10 * 60_000 };
    return t.access_token;
  }
}
