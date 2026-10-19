import type { TaskDefinition, TaskInstance } from "@openclaw-line/shared";
import type { FailureEvent } from "./types.js";

/** Where the aggregator keeps its state so a worker restart neither drops nor repeats incidents. */
export interface DedupeStore {
  savePendingEvent(key: string, event: FailureEvent): void;
  /** Drops the run's buffered events and records it as investigated, in one transaction. */
  markFlushed(key: string, at: number): void;
  pruneFlushed(before: number): void;
  loadDedupeState(flushedSince: number): { pending: Map<string, FailureEvent[]>; flushed: Map<string, number> };
}

/**
 * DAG-level deduplication (spec §5 "Flapping / Cascading Alerts").
 *
 * Failure callbacks for the same (dag_id, run_id) are buffered for a debounce
 * window. When it closes, `onFlush` is called once with every buffered event.
 * A run that has already been flushed is ignored until `ttlMs` passes, so the
 * same run is never investigated twice.
 *
 * With a `store`, buffered events and flushed runs are persisted: `restore()`
 * re-arms the windows that were open when the worker stopped.
 */
export class FailureAggregator {
  private readonly pending = new Map<string, { events: FailureEvent[]; timer?: ReturnType<typeof setTimeout> }>();
  private readonly flushed = new Map<string, number>();
  private stopped = false;

  constructor(
    private readonly opts: {
      windowMs: number;
      ttlMs: number;
      onFlush: (key: string, events: FailureEvent[]) => void | Promise<void>;
      now?: () => number;
      maxTracked?: number;
      store?: DedupeStore;
    },
  ) {}

  static key(e: Pick<FailureEvent, "dag_id" | "run_id">): string {
    return `${e.dag_id}:${e.run_id}`;
  }

  /** True when pending windows survive a restart (no need to flush them on shutdown). */
  get persistent(): boolean {
    return this.opts.store !== undefined;
  }

  /** Returns what happened to the event, for logging and the HTTP response. */
  add(event: FailureEvent): "buffered" | "joined" | "duplicate" {
    const key = FailureAggregator.key(event);
    this.expire();
    if (this.flushed.has(key)) return "duplicate";

    this.opts.store?.savePendingEvent(key, event);
    const existing = this.pending.get(key);
    if (existing) {
      existing.events.push(event);
      return "joined";
    }
    this.pending.set(key, { events: [event], timer: this.arm(key, this.opts.windowMs) });
    return "buffered";
  }

  /** Reload persisted state after a restart; windows that already closed flush right away. Returns the re-armed run count. */
  restore(): number {
    const store = this.opts.store;
    if (!store) return 0;
    const { pending, flushed } = store.loadDedupeState(this.now() - this.opts.ttlMs);
    for (const [key, at] of flushed) this.flushed.set(key, at);
    let restored = 0;
    for (const [key, events] of pending) {
      if (this.pending.has(key) || !events.length) continue;
      const first = Math.min(...events.map((e) => e.receivedAt ?? 0));
      const remaining = Math.max(0, first + this.opts.windowMs - this.now());
      this.pending.set(key, { events, timer: this.arm(key, remaining) });
      restored++;
    }
    return restored;
  }

  async flush(key: string): Promise<void> {
    const entry = this.pending.get(key);
    if (!entry) return;
    clearTimeout(entry.timer);
    this.pending.delete(key);
    const at = this.now();
    this.flushed.set(key, at);
    this.opts.store?.markFlushed(key, at);
    this.opts.store?.pruneFlushed(at - this.opts.ttlMs);
    await this.opts.onFlush(key, entry.events);
  }

  async flushAll(): Promise<void> {
    await Promise.all([...this.pending.keys()].map((k) => this.flush(k)));
  }

  /**
   * Stop the window timers without flushing (persisted windows resume on the next start).
   * Events added afterwards are still saved, but start no investigation in this process.
   */
  stop(): void {
    this.stopped = true;
    for (const { timer } of this.pending.values()) clearTimeout(timer);
  }

  get pendingCount(): number {
    return this.pending.size;
  }

  private arm(key: string, ms: number): ReturnType<typeof setTimeout> | undefined {
    if (this.stopped) return undefined;
    const timer = setTimeout(() => void this.flush(key), ms);
    timer.unref?.();
    return timer;
  }

  private expire(): void {
    const cutoff = this.now() - this.opts.ttlMs;
    for (const [k, t] of this.flushed) {
      if (t < cutoff) this.flushed.delete(k);
    }
    const max = this.opts.maxTracked ?? 10_000;
    while (this.flushed.size > max) {
      const oldest = this.flushed.keys().next().value;
      if (oldest === undefined) break;
      this.flushed.delete(oldest);
    }
  }

  private now(): number {
    return this.opts.now?.() ?? Date.now();
  }
}

export type RootSelection = {
  root: string;
  failedDescendants: string[];
  upstreamFailed: string[];
  otherFailures: string[];
};

/**
 * Pick the task to root-cause: the earliest failed task whose upstream
 * ancestors did not fail. Every other failure is mapped as blast radius
 * (descendants of the root) or listed as an independent failure.
 *
 * `instances`/`tasks` come from the Airflow API; when unavailable we fall
 * back to the callback events alone (earliest received wins).
 */
export function selectRootTask(
  events: FailureEvent[],
  instances: TaskInstance[] = [],
  tasks: TaskDefinition[] = [],
): RootSelection {
  const failed = new Set<string>(events.map((e) => e.task_id));
  const upstreamFailed = new Set<string>();
  const endTime = new Map<string, number>();
  for (const ti of instances) {
    if (ti.state === "failed") failed.add(ti.task_id);
    if (ti.state === "upstream_failed") upstreamFailed.add(ti.task_id);
    const t = Date.parse(ti.end_date ?? ti.start_date ?? "");
    if (Number.isFinite(t)) endTime.set(ti.task_id, Math.min(t, endTime.get(ti.task_id) ?? Infinity));
  }

  // Build parent links from Airflow's downstream_task_ids.
  const parents = new Map<string, string[]>();
  const children = new Map<string, string[]>();
  for (const t of tasks) {
    children.set(t.task_id, t.downstream_task_ids ?? []);
    for (const d of t.downstream_task_ids ?? []) parents.set(d, [...(parents.get(d) ?? []), t.task_id]);
  }
  const ancestors = (id: string): Set<string> => walk(id, parents);
  const descendants = (id: string): Set<string> => walk(id, children);

  const eventOrder = new Map(events.map((e, i) => [e.task_id, i]));
  // Earliest end time first; tasks without timing fall back to callback arrival order.
  const byTime = (a: string, b: string) =>
    (endTime.get(a) ?? Infinity) - (endTime.get(b) ?? Infinity) ||
    (eventOrder.get(a) ?? Infinity) - (eventOrder.get(b) ?? Infinity) ||
    a.localeCompare(b);

  const candidates = [...failed].filter((id) => ![...ancestors(id)].some((a) => failed.has(a)));
  const pool = candidates.length ? candidates : [...failed];
  pool.sort(byTime);
  const root = pool[0] ?? events[0]!.task_id;

  const rootDesc = descendants(root);
  const failedDescendants: string[] = [];
  const otherFailures: string[] = [];
  for (const id of failed) {
    if (id === root) continue;
    if (rootDesc.has(id)) failedDescendants.push(id);
    else otherFailures.push(id);
  }
  return {
    root,
    failedDescendants: failedDescendants.sort(),
    upstreamFailed: [...upstreamFailed].sort(),
    otherFailures: otherFailures.sort(),
  };
}

function walk(start: string, edges: Map<string, string[]>): Set<string> {
  const seen = new Set<string>();
  const stack = [...(edges.get(start) ?? [])];
  while (stack.length) {
    const id = stack.pop()!;
    if (seen.has(id)) continue;
    seen.add(id);
    stack.push(...(edges.get(id) ?? []));
  }
  return seen;
}
