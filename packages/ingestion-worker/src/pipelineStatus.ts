import { DatabricksClient, sanitize } from "@openclaw-line/shared";

/**
 * Databricks job status for the dashboard (GET /api/pipeline): the latest runs
 * of one job with their task states. Read-only service principal (CAN_VIEW on
 * the job). Cached briefly so dashboard refreshes don't hit the Jobs API each time.
 */

export type PipelineStatusConfig = { host: string; clientId: string; clientSecret: string; jobId: string };

export type PipelineTask = { key: string; state: string; startedAt: number | null; endedAt: number | null };

export type PipelineRun = {
  runId: string;
  state: string;
  startedAt: number | null;
  endedAt: number | null;
  durationSeconds: number | null;
  runDate: string | null;
  fault: string | null;
  trigger: string | null;
  url: string | null;
  tasks: PipelineTask[];
};

export type PipelineData =
  | { enabled: false }
  | { enabled: true; generatedAt: number; job: { id: string; name: string | null; url: string }; runs: PipelineRun[]; error?: string };

type ApiTask = {
  task_key: string;
  attempt_number?: number;
  start_time?: number;
  end_time?: number;
  depends_on?: { task_key: string }[];
  state?: { life_cycle_state?: string; result_state?: string };
};

type ApiRun = {
  run_id: number;
  run_name?: string;
  start_time?: number;
  end_time?: number;
  run_duration?: number;
  trigger?: string;
  run_page_url?: string;
  state?: { life_cycle_state?: string; result_state?: string };
  job_parameters?: { name: string; value?: string; default?: string }[];
  tasks?: ApiTask[];
};

const CACHE_MS = 20_000;

export class PipelineStatus {
  private readonly client: DatabricksClient;
  private cached: { at: number; data: PipelineData } | undefined;

  constructor(
    private readonly cfg: PipelineStatusConfig,
    private readonly deps: { fetch?: typeof fetch; now?: () => number } = {},
  ) {
    this.client = new DatabricksClient({ host: cfg.host, clientId: cfg.clientId, clientSecret: cfg.clientSecret, fetch: deps.fetch, now: deps.now });
  }

  async get(): Promise<PipelineData> {
    const now = (this.deps.now ?? Date.now)();
    if (this.cached && now - this.cached.at < CACHE_MS) return this.cached.data;
    const job = { id: this.cfg.jobId, name: null as string | null, url: `${this.cfg.host.replace(/\/+$/, "")}/jobs/${this.cfg.jobId}` };
    let data: PipelineData;
    try {
      const res = await this.client.get<{ runs?: ApiRun[] }>(`/api/2.2/jobs/runs/list?job_id=${encodeURIComponent(this.cfg.jobId)}&limit=10&expand_tasks=true`);
      const runs = (res.runs ?? []).map(toRun);
      job.name = res.runs?.[0]?.run_name ?? null;
      data = { enabled: true, generatedAt: now, job, runs };
    } catch (err) {
      data = { enabled: true, generatedAt: now, job, runs: [], error: sanitize((err as Error).message) };
    }
    this.cached = { at: now, data };
    return data;
  }
}

/** One state word per run/task: the result once finished, else the life-cycle state. */
function stateOf(s: ApiRun["state"]): string {
  return (s?.result_state ?? s?.life_cycle_state ?? "UNKNOWN").toUpperCase();
}

const ms = (v: number | undefined) => (v && v > 0 ? v : null);

function toRun(r: ApiRun): PipelineRun {
  const param = (name: string) => {
    const p = r.job_parameters?.find((x) => x.name === name);
    const v = p?.value ?? p?.default;
    return v ? v : null;
  };
  const startedAt = ms(r.start_time);
  const endedAt = ms(r.end_time);
  return {
    runId: String(r.run_id),
    state: stateOf(r.state),
    startedAt,
    endedAt,
    durationSeconds: r.run_duration ? Math.round(r.run_duration / 1000) : startedAt && endedAt ? Math.round((endedAt - startedAt) / 1000) : null,
    runDate: param("run_date"),
    fault: param("inject_fault"),
    trigger: r.trigger ?? null,
    url: r.run_page_url && /^https:\/\//.test(r.run_page_url) ? r.run_page_url : null,
    tasks: orderTasks(latestAttempts(r.tasks ?? [])).map((t) => ({ key: t.task_key, state: stateOf(t.state), startedAt: ms(t.start_time), endedAt: ms(t.end_time) })),
  };
}

function latestAttempts(tasks: ApiTask[]): ApiTask[] {
  const byKey = new Map<string, ApiTask>();
  for (const t of tasks) {
    const prev = byKey.get(t.task_key);
    if (!prev || (t.attempt_number ?? 0) >= (prev.attempt_number ?? 0)) byKey.set(t.task_key, t);
  }
  return [...byKey.values()];
}

/** Dependency order (the API returns tasks unordered); ties keep the key order. */
function orderTasks(tasks: ApiTask[]): ApiTask[] {
  const byKey = new Map(tasks.map((t) => [t.task_key, t]));
  const done = new Set<string>();
  const out: ApiTask[] = [];
  const visit = (t: ApiTask, seen: Set<string>) => {
    if (done.has(t.task_key) || seen.has(t.task_key)) return;
    seen.add(t.task_key);
    for (const d of t.depends_on ?? []) {
      const dep = byKey.get(d.task_key);
      if (dep) visit(dep, seen);
    }
    done.add(t.task_key);
    out.push(t);
  };
  for (const t of [...tasks].sort((a, b) => a.task_key.localeCompare(b.task_key))) visit(t, new Set());
  return out;
}
