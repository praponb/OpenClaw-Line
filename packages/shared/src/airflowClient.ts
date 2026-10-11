/**
 * Minimal read-only Airflow REST client.
 *
 * Only read endpoints are used. The single POST is the Airflow 3 token
 * exchange (`/auth/token`), which does not change any Airflow state. Use an
 * Airflow user with the `Viewer` role.
 *
 *   Airflow 3 -> /api/v2, bearer token obtained from /auth/token
 *   Airflow 2 -> /api/v1, HTTP basic auth
 */

export type AirflowApiVersion = "v1" | "v2";

export type AirflowClientOptions = {
  baseUrl: string;
  username: string;
  password: string;
  apiVersion?: AirflowApiVersion;
  timeoutMs?: number;
  fetch?: typeof fetch;
};

export type TaskInstance = {
  task_id: string;
  state: string | null;
  try_number: number;
  start_date: string | null;
  end_date: string | null;
  map_index?: number;
};

export type DagRun = {
  dag_run_id: string;
  state: string | null;
  start_date: string | null;
  end_date: string | null;
};

export type TaskDefinition = {
  task_id: string;
  downstream_task_ids: string[];
};

export class AirflowError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = "AirflowError";
  }
}

export class AirflowClient {
  private readonly base: string;
  private readonly version: AirflowApiVersion;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;
  private token?: { value: string; expiresAt: number };

  constructor(private readonly opts: AirflowClientOptions) {
    this.base = opts.baseUrl.replace(/\/+$/, "");
    this.version = opts.apiVersion ?? "v2";
    this.fetchImpl = opts.fetch ?? fetch;
    this.timeoutMs = opts.timeoutMs ?? 15_000;
  }

  /** Raw log text for one try of a task instance (structured logs are flattened). */
  async getTaskLog(dagId: string, runId: string, taskId: string, tryNumber?: number): Promise<{ text: string; tryNumber: number }> {
    const attempt = tryNumber ?? (await this.getTaskInstance(dagId, runId, taskId)).try_number;
    const n = Math.max(1, attempt);
    const path = `${this.runPath(dagId, runId)}/taskInstances/${enc(taskId)}/logs/${n}?full_content=true`;
    const body = await this.getJson<{ content: unknown }>(path);
    return { text: flattenLogContent(body.content), tryNumber: n };
  }

  async getTaskInstance(dagId: string, runId: string, taskId: string): Promise<TaskInstance> {
    return this.getJson<TaskInstance>(`${this.runPath(dagId, runId)}/taskInstances/${enc(taskId)}`);
  }

  async listTaskInstances(dagId: string, runId: string): Promise<TaskInstance[]> {
    const body = await this.getJson<{ task_instances: TaskInstance[] }>(`${this.runPath(dagId, runId)}/taskInstances?limit=1000`);
    return body.task_instances ?? [];
  }

  async listTasks(dagId: string): Promise<TaskDefinition[]> {
    const body = await this.getJson<{ tasks: TaskDefinition[] }>(`/dags/${enc(dagId)}/tasks`);
    return body.tasks ?? [];
  }

  /** Most recent runs of a DAG, newest first (read-only). */
  async listDagRuns(dagId: string, limit = 10): Promise<DagRun[]> {
    const body = await this.getJson<{ dag_runs: DagRun[] }>(`/dags/${enc(dagId)}/dagRuns?order_by=-start_date&limit=${limit}`);
    return body.dag_runs ?? [];
  }

  private runPath(dagId: string, runId: string): string {
    return `/dags/${enc(dagId)}/dagRuns/${enc(runId)}`;
  }

  private async getJson<T>(path: string): Promise<T> {
    const url = `${this.base}/api/${this.version}${path}`;
    const res = await this.request(url, {
      method: "GET",
      headers: { Accept: "application/json", Authorization: await this.authHeader() },
    });
    if (res.status === 401 && this.version === "v2") {
      // Token expired early; refresh once.
      this.token = undefined;
      const retry = await this.request(url, {
        method: "GET",
        headers: { Accept: "application/json", Authorization: await this.authHeader() },
      });
      return this.parse<T>(retry, url);
    }
    return this.parse<T>(res, url);
  }

  private async parse<T>(res: Response, url: string): Promise<T> {
    if (!res.ok) {
      const detail = (await res.text().catch(() => "")).slice(0, 300);
      throw new AirflowError(`Airflow API ${res.status} for ${stripOrigin(url)}: ${detail}`, res.status);
    }
    return (await res.json()) as T;
  }

  private async authHeader(): Promise<string> {
    if (this.version === "v1") {
      return "Basic " + Buffer.from(`${this.opts.username}:${this.opts.password}`).toString("base64");
    }
    if (this.token && this.token.expiresAt > Date.now() + 30_000) return `Bearer ${this.token.value}`;
    const res = await this.request(`${this.base}/auth/token`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ username: this.opts.username, password: this.opts.password }),
    });
    if (!res.ok) throw new AirflowError(`Airflow token exchange failed (${res.status})`, res.status);
    const body = (await res.json()) as { access_token?: string };
    if (!body.access_token) throw new AirflowError("Airflow token exchange returned no access_token");
    this.token = { value: body.access_token, expiresAt: jwtExpiry(body.access_token) ?? Date.now() + 10 * 60_000 };
    return `Bearer ${this.token.value}`;
  }

  private async request(url: string, init: RequestInit): Promise<Response> {
    try {
      return await this.fetchImpl(url, { ...init, signal: AbortSignal.timeout(this.timeoutMs) });
    } catch (err) {
      throw new AirflowError(`Airflow request failed for ${stripOrigin(url)}: ${(err as Error).message}`);
    }
  }
}

/**
 * Airflow 2 returns log `content` as a string. Airflow 3 returns a list of
 * structured entries ({timestamp, level, event, error_detail, ...}); render
 * them back into a classic text log, including Python-style tracebacks so
 * file/line evidence survives.
 */
export function flattenLogContent(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return content == null ? "" : JSON.stringify(content);
  const lines: string[] = [];
  for (const entry of content) {
    if (typeof entry === "string") {
      lines.push(entry);
      continue;
    }
    if (!entry || typeof entry !== "object") continue;
    const e = entry as Record<string, unknown>;
    const ts = typeof e.timestamp === "string" ? `[${e.timestamp}] ` : "";
    const level = typeof e.level === "string" ? `${e.level.toUpperCase()} - ` : "";
    const event = typeof e.event === "string" ? e.event : "";
    if (ts || level || event) lines.push(`${ts}${level}${event}`.trimEnd());
    if (Array.isArray(e.error_detail)) lines.push(...renderErrorDetail(e.error_detail));
  }
  return lines.join("\n");
}

type ErrorFrame = { filename?: string; lineno?: number; name?: string };
type ErrorDetail = { exc_type?: string; exc_value?: string; frames?: ErrorFrame[]; is_cause?: boolean };

function renderErrorDetail(details: unknown[]): string[] {
  const out: string[] = [];
  // Airflow lists chained exceptions outermost-first; Python prints causes first.
  for (const raw of [...details].reverse()) {
    const d = raw as ErrorDetail;
    out.push("Traceback (most recent call last):");
    for (const f of d.frames ?? []) {
      out.push(`  File "${f.filename ?? "?"}", line ${f.lineno ?? "?"}, in ${f.name ?? "?"}`);
    }
    out.push(`${d.exc_type ?? "Exception"}: ${d.exc_value ?? ""}`);
  }
  return out;
}

function enc(s: string): string {
  return encodeURIComponent(s);
}

function stripOrigin(url: string): string {
  try {
    const u = new URL(url);
    return u.pathname;
  } catch {
    return url;
  }
}

function jwtExpiry(token: string): number | undefined {
  const payload = token.split(".")[1];
  if (!payload) return undefined;
  try {
    const exp = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")).exp;
    return typeof exp === "number" ? exp * 1000 : undefined;
  } catch {
    return undefined;
  }
}
