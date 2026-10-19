import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TaskDefinition, TaskInstance } from "@openclaw-line/shared";
import { FailureAggregator, selectRootTask } from "../src/dedupe.js";
import type { FailureEvent } from "../src/types.js";

const ev = (task_id: string, run_id = "r1"): FailureEvent => ({ dag_id: "daily_revenue_pipeline", task_id, run_id });

describe("FailureAggregator", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("collapses 5 failures in one run into a single flush", async () => {
    const onFlush = vi.fn();
    const agg = new FailureAggregator({ windowMs: 60_000, ttlMs: 3_600_000, onFlush });
    expect(agg.add(ev("a"))).toBe("buffered");
    for (const t of ["b", "c", "d", "e"]) expect(agg.add(ev(t))).toBe("joined");
    await vi.advanceTimersByTimeAsync(59_000);
    expect(onFlush).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(onFlush).toHaveBeenCalledTimes(1);
    expect(onFlush.mock.calls[0]![1]).toHaveLength(5);
  });

  it("ignores late callbacks for an already investigated run until the TTL passes", async () => {
    const onFlush = vi.fn();
    const agg = new FailureAggregator({ windowMs: 1_000, ttlMs: 10_000, onFlush });
    agg.add(ev("a"));
    await vi.advanceTimersByTimeAsync(1_000);
    expect(agg.add(ev("late"))).toBe("duplicate");
    await vi.advanceTimersByTimeAsync(10_001);
    expect(agg.add(ev("a"))).toBe("buffered");
  });

  it("keeps separate runs separate", async () => {
    const onFlush = vi.fn();
    const agg = new FailureAggregator({ windowMs: 1_000, ttlMs: 10_000, onFlush });
    agg.add(ev("a", "r1"));
    agg.add(ev("a", "r2"));
    await vi.advanceTimersByTimeAsync(1_000);
    expect(onFlush).toHaveBeenCalledTimes(2);
  });

  it("after stop() saves new callbacks but starts no investigation (worker shutting down)", async () => {
    const onFlush = vi.fn();
    const saved: string[] = [];
    const store = { savePendingEvent: (k: string) => saved.push(k), markFlushed: vi.fn(), pruneFlushed: vi.fn(), loadDedupeState: () => ({ pending: new Map(), flushed: new Map() }) };
    const agg = new FailureAggregator({ windowMs: 1_000, ttlMs: 10_000, onFlush, store });
    agg.add(ev("a", "r1"));
    agg.stop();
    expect(agg.add(ev("b", "r2"))).toBe("buffered");
    await vi.advanceTimersByTimeAsync(5_000);
    expect(onFlush).not.toHaveBeenCalled();
    expect(saved).toEqual(["daily_revenue_pipeline:r1", "daily_revenue_pipeline:r2"]);
    expect(agg.pendingCount).toBe(2);
  });
});

describe("selectRootTask", () => {
  const tasks: TaskDefinition[] = [
    { task_id: "extract", downstream_task_ids: ["validate", "stg"] },
    { task_id: "validate", downstream_task_ids: [] },
    { task_id: "stg", downstream_task_ids: ["fct"] },
    { task_id: "fct", downstream_task_ids: [] },
    { task_id: "fx_rates", downstream_task_ids: [] },
  ];
  const ti = (task_id: string, state: string, end: string | null): TaskInstance => ({ task_id, state, try_number: 1, start_date: end, end_date: end });

  it("picks the earliest failed task without failed ancestors, even if its callback arrived last", () => {
    const instances = [
      ti("extract", "failed", "2026-10-10T02:05:02Z"),
      ti("validate", "failed", "2026-10-10T02:05:10Z"),
      ti("stg", "upstream_failed", null),
      ti("fct", "upstream_failed", null),
    ];
    const sel = selectRootTask([ev("validate"), ev("extract")], instances, tasks);
    expect(sel).toEqual({ root: "extract", failedDescendants: ["validate"], upstreamFailed: ["fct", "stg"], otherFailures: [] });
  });

  it("lists independent failures separately", () => {
    const instances = [ti("extract", "failed", "2026-10-10T02:05:02Z"), ti("fx_rates", "failed", "2026-10-10T02:06:00Z")];
    const sel = selectRootTask([ev("fx_rates"), ev("extract")], instances, tasks);
    expect(sel.root).toBe("extract");
    expect(sel.otherFailures).toEqual(["fx_rates"]);
  });

  it("falls back to callback order without Airflow data", () => {
    expect(selectRootTask([ev("b"), ev("a")]).root).toBe("b");
  });
});
