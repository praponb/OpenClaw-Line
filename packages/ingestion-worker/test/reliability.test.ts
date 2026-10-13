import { mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Alerts, gatewayProblem } from "../src/alerts.js";
import { DailyBackup } from "../src/backup.js";
import { loadConfig } from "../src/config.js";
import { buildDashboardData, incidentCostUsd } from "../src/dashboard.js";
import { FailureAggregator } from "../src/dedupe.js";
import type { Dispatcher } from "../src/dispatcher.js";
import { FeedbackStore } from "../src/feedbackStore.js";
import { LineClient } from "../src/line.js";
import { createWorker } from "../src/server.js";
import type { FailureEvent } from "../src/types.js";

const env = {
  WEBHOOK_SECRET: "whsec_test",
  OPENCLAW_HOOK_TOKEN: "hook-token",
  LINE_TARGET_ID: "U0123456789abcdef0123456789abcdef",
  LINE_CHANNEL_ACCESS_TOKEN: "line-token",
  LINE_CHANNEL_SECRET: "line-secret",
  OPENCLAW_GATEWAY_URL: "http://gateway:18789",
};
const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
const ev = (task_id: string, receivedAt: number, run_id = "r1"): FailureEvent => ({ dag_id: "daily_revenue_pipeline", task_id, run_id, receivedAt });

/** Records Healthchecks pings: [path suffix ("" or "/fail"), body]. */
function pingRecorder() {
  const pings: [string, string][] = [];
  const fetch = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
    const u = String(url);
    pings.push([u.endsWith("/fail") ? "/fail" : "", String(init?.body ?? "")]);
    return new Response("OK", { status: 200 });
  });
  return { pings, fetch: fetch as unknown as typeof globalThis.fetch };
}

describe("persistent dedupe state", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("resumes a grouping window after a restart and still ignores runs already investigated", async () => {
    const store = new FeedbackStore(":memory:");
    let t = 1_000_000;
    const now = () => t;
    const first = vi.fn();
    const before = new FailureAggregator({ windowMs: 60_000, ttlMs: 3_600_000, onFlush: first, store, now });
    before.add(ev("extract", t, "run-a"));
    before.add(ev("validate", t, "run-a"));
    before.add(ev("extract", t, "run-b"));
    await before.flush("daily_revenue_pipeline:run-b");
    before.stop(); // worker stops 20 s into run-a's window

    t += 20_000;
    const onFlush = vi.fn();
    const after = new FailureAggregator({ windowMs: 60_000, ttlMs: 3_600_000, onFlush, store, now });
    expect(after.restore()).toBe(1);
    expect(after.add(ev("late", t, "run-b"))).toBe("duplicate");
    expect(after.add(ev("stg", t, "run-a"))).toBe("joined");

    await vi.advanceTimersByTimeAsync(39_000);
    expect(onFlush).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(onFlush).toHaveBeenCalledTimes(1);
    expect(onFlush.mock.calls[0]![1].map((e: FailureEvent) => e.task_id)).toEqual(["extract", "validate", "stg"]);
    expect(store.loadDedupeState(0).pending.size).toBe(0);
    expect(first).toHaveBeenCalledTimes(1);
  });

  it("flushes windows that closed while the worker was down right away, and forgets runs after the TTL", async () => {
    const store = new FeedbackStore(":memory:");
    let t = 1_000_000;
    const before = new FailureAggregator({ windowMs: 1_000, ttlMs: 10_000, onFlush: vi.fn(), store, now: () => t });
    before.add(ev("a", t, "old"));
    await before.flush("daily_revenue_pipeline:old");
    before.add(ev("a", t, "open"));
    before.stop();

    t += 60_000;
    const onFlush = vi.fn();
    const after = new FailureAggregator({ windowMs: 1_000, ttlMs: 10_000, onFlush, store, now: () => t });
    after.restore();
    await vi.advanceTimersByTimeAsync(0);
    expect(onFlush).toHaveBeenCalledTimes(1);
    expect(after.add(ev("a", t, "old"))).toBe("buffered"); // TTL passed: a new failure of that run counts again
  });
});

describe("Healthchecks alerts", () => {
  it("pings success or /fail, sanitizes the body and never throws", async () => {
    const { pings, fetch } = pingRecorder();
    const alerts = new Alerts({ heartbeatUrl: "https://hc-ping.com/hb/", lineUrl: "https://hc-ping.com/line" }, { fetch, log });
    await alerts.heartbeat([]);
    await alerts.heartbeat(["OpenClaw gateway not ready (HTTP 503)"]);
    await alerts.lineFailed("LINE push failed: token=abc123secret for sam.lee@example.com");
    await alerts.lineOk("Report INC-AAAAAA delivered");
    expect(pings.map(([p]) => p)).toEqual(["", "/fail", "/fail", ""]);
    expect(pings[0]![1]).toBe("ok");
    expect(pings[2]![1]).not.toContain("sam.lee@example.com");
    expect(fetch).toHaveBeenNthCalledWith(1, "https://hc-ping.com/hb", expect.anything());

    const broken = new Alerts({ heartbeatUrl: "https://hc-ping.com/hb" }, { fetch: (async () => Promise.reject(new Error("offline"))) as typeof globalThis.fetch, log });
    await expect(broken.heartbeat([])).resolves.toBeUndefined();
    await expect(broken.lineFailed("x")).resolves.toBeUndefined(); // no LINE URL: nothing sent
  });

  it("reports the gateway when /readyz is not 200 or unreachable", async () => {
    const ok = (async () => new Response("{}", { status: 200 })) as typeof fetch;
    const down = (async () => new Response("{}", { status: 503 })) as typeof fetch;
    expect(await gatewayProblem("http://gw:18789/", { fetch: ok })).toBeUndefined();
    expect(await gatewayProblem("http://gw:18789", { fetch: down })).toBe("OpenClaw gateway not ready (HTTP 503)");
    expect(await gatewayProblem("http://gw:18789", { fetch: (async () => Promise.reject(new Error("ECONNREFUSED"))) as typeof fetch })).toContain("unreachable");
  });

  it("only accepts https ping URLs", () => {
    expect(loadConfig({ ...env, HEALTHCHECK_URL: "https://hc-ping.com/abc" }).alerts).toEqual({ heartbeatUrl: "https://hc-ping.com/abc", lineUrl: undefined, heartbeatSeconds: 300 });
    expect(() => loadConfig({ ...env, HEALTHCHECK_LINE_URL: "http://hc-ping.com/abc" })).toThrow(/https/);
  });

  it("alerts the LINE check when a push fails or the gateway could not deliver the report", async () => {
    const { pings, fetch } = pingRecorder();
    const alerts = new Alerts({ lineUrl: "https://hc-ping.com/line" }, { fetch, log });
    let lineStatus = 200;
    const lineFetch = vi.fn(async () => new Response(lineStatus === 200 ? "{}" : '{"message":"Authentication failed"}', { status: lineStatus }));
    const line = new LineClient("line-token", { fetch: lineFetch as unknown as typeof globalThis.fetch });
    let completion = { status: "ok", delivered: false, replyDisposition: "visible", deliveryError: "LINE API 429" };
    const dispatcher: Dispatcher = { dispatch: async () => ({ ok: true, detail: "test", completion }) };
    const worker = createWorker(loadConfig(env), dispatcher, { store: new FeedbackStore(":memory:"), line, log, alerts });

    await worker.investigate("daily_revenue_pipeline:r1", [ev("extract", 1000)]);
    await vi.waitFor(() => expect(pings.length).toBeGreaterThanOrEqual(2));
    // gateway delivery failed (alert), then the worker's fallback notice went through (ok)
    expect(pings[0]).toEqual(["/fail", expect.stringMatching(/^Report INC-\w{6} was not delivered to LINE: LINE API 429$/)]);
    expect(pings[1]).toEqual(["", "LINE push ok"]);

    lineStatus = 401;
    completion = { status: "ok", delivered: true, replyDisposition: "visible", deliveryError: "" };
    await worker.investigate("daily_revenue_pipeline:r2", [ev("extract", 2000, "r2")]);
    await vi.waitFor(() => expect(pings.length).toBe(4));
    expect(pings[2]![0]).toBe("");
    expect(pings[3]).toEqual(["/fail", expect.stringContaining("LINE push failed: LINE /v2/bot/message/push HTTP 401")]);
  });
});

describe("worker health", () => {
  it("counts investigations in progress on /healthz and reports a broken gateway and backup", async () => {
    let release!: () => void;
    const dispatcher: Dispatcher = { dispatch: () => new Promise((r) => (release = () => r({ ok: true, detail: "test" }))) };
    const gw = (async () => new Response("{}", { status: 503 })) as typeof fetch;
    const worker = createWorker(loadConfig({ ...env, DEDUPE_WINDOW_S: "60" }), dispatcher, { store: new FeedbackStore(":memory:"), line: new LineClient("tok"), log, fetch: gw });
    await new Promise<void>((r) => worker.server.listen(0, "127.0.0.1", r));
    const port = (worker.server.address() as AddressInfo).port;
    try {
      worker.aggregator.add(ev("extract", Date.now()));
      const flushing = worker.aggregator.flushAll();
      await vi.waitFor(() => expect(release).toBeDefined());
      expect(await (await fetch(`http://127.0.0.1:${port}/healthz`)).json()).toEqual({ ok: true, pending: 0, investigating: 1 });
      release();
      await flushing;
      expect(await (await fetch(`http://127.0.0.1:${port}/healthz`)).json()).toMatchObject({ investigating: 0 });

      worker.backup!.lastError = "disk full";
      expect(await worker.healthProblems()).toEqual(["OpenClaw gateway not ready (HTTP 503)", "feedback.db backup failed: disk full"]);
    } finally {
      worker.server.close();
    }
  });
});

describe("DailyBackup", () => {
  let dir: string;
  beforeEach(() => (dir = mkdtempSync(join(tmpdir(), "fb-backup-"))));
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  it("writes one readable copy per Bangkok day and keeps the newest N", () => {
    const store = new FeedbackStore(join(dir, "feedback.db"));
    store.recordDispatch({ ref: "INC-AAAAAA", incidentKey: "k", dagId: "d", taskId: "t", runId: "r", dispatchedAt: 1 });
    const backups = join(dir, "backups");
    mkdirSync(backups);
    for (const day of ["2026-10-01", "2026-10-02", "2026-10-03"]) writeFileSync(join(backups, `feedback-${day}.db`), "");
    // 2026-10-12 23:30 UTC = 2026-10-13 06:30 Bangkok
    let t = Date.UTC(2026, 9, 12, 23, 30);
    const b = new DailyBackup(store, { dir: backups, keep: 2, now: () => t, log });
    expect(b.runIfDue()).toBe("written");
    expect(b.runIfDue()).toBe("skipped");
    expect(readdirSync(backups).sort()).toEqual(["feedback-2026-10-03.db", "feedback-2026-10-13.db"]);
    const copy = new FeedbackStore(join(backups, "feedback-2026-10-13.db"));
    expect(copy.getIncident("INC-AAAAAA")).toBeDefined();
    copy.close();

    t += 24 * 3600_000;
    expect(b.runIfDue()).toBe("written");
    expect(readdirSync(backups).sort()).toEqual(["feedback-2026-10-13.db", "feedback-2026-10-14.db"]);
    store.close();
  });

  it("records a failure for the heartbeat and clears it on the next success", () => {
    let fail = true;
    const store = {
      backupTo: (file: string) => {
        if (fail) throw new Error("SQLITE_FULL: disk full");
        writeFileSync(file, "");
      },
    };
    const b = new DailyBackup(store, { dir, keep: 3, log });
    expect(b.runIfDue()).toBe("failed");
    expect(b.lastError).toContain("disk full");
    fail = false;
    expect(b.runIfDue()).toBe("written");
    expect(b.lastError).toBeUndefined();
  });

  it("is on by default next to the database and can be turned off", () => {
    expect(loadConfig({ ...env, FEEDBACK_DB_PATH: "/data/feedback.db" }).feedback!.backup).toEqual({ dir: "/data/backups", keep: 14 });
    const off = createWorker(loadConfig({ ...env, FEEDBACK_BACKUP_KEEP: "0" }), { dispatch: async () => ({ ok: true, detail: "" }) }, { store: new FeedbackStore(":memory:"), line: new LineClient("tok"), log });
    expect(off.backup).toBeUndefined();
  });
});

describe("fallback model cost", () => {
  const kimi = { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3 };
  const gemini = { input: 0.75, output: 3.75, cacheRead: 0.075, cacheWrite: 0.75 };

  it("prices each model's calls at its own rate and shows which models answered", () => {
    const s = new FeedbackStore(":memory:");
    s.recordDispatch({ ref: "INC-AAAAAA", incidentKey: "k", dagId: "d", taskId: "t", runId: "r", dispatchedAt: 2000, model: "moonshot/kimi-k3" });
    s.setAgentRunId("INC-AAAAAA", "run-1");
    s.recordModelUsage({ runId: "run-1", provider: "moonshot", model: "kimi-k3", input: 1000, output: 0, cacheRead: 0, cacheWrite: 0 }, 3000);
    s.recordModelUsage({ runId: "run-1", provider: "google", model: "gemini-3.5-flash", input: 10_000, output: 2_000, cacheRead: 0, cacheWrite: 1_000 }, 4000);
    const usage = s.usageSince(0).get("INC-AAAAAA")!;
    expect(usage).toMatchObject({ input: 11_000, output: 2_000, calls: 2, model: "google/gemini-3.5-flash, moonshot/kimi-k3" });

    const fallback = { model: "google/gemini-3.5-flash", prices: gemini };
    const expected = (1000 * 3 + 10_000 * 0.75 + 2_000 * 3.75 + 1_000 * 0.75) / 1e6;
    expect(incidentCostUsd(usage, kimi, fallback)).toBeCloseTo(expected);
    expect(incidentCostUsd(usage, kimi, { model: "gemini-3.5-flash", prices: gemini })).toBeCloseTo(expected);
    expect(incidentCostUsd(usage, kimi, null)).toBeCloseTo((11_000 * 3 + 2_000 * 15 + 1_000 * 3) / 1e6);
    expect(incidentCostUsd(usage, null, fallback)).toBeNull();

    const d = buildDashboardData(s.incidentsSince(0), [], { days: 30, now: 5000, usage: s.usageSince(0), prices: kimi, fallbackPrices: fallback });
    expect(d.incidents[0]).toMatchObject({ model: "google/gemini-3.5-flash, moonshot/kimi-k3" });
    expect(d.incidents[0]!.costUsd).toBeCloseTo(expected);
    expect(d.modelCost.usd).toBeCloseTo(expected);
  });

  it("reads fallback prices only together with the fallback model", () => {
    const prices = { FALLBACK_MODEL_PRICE_INPUT_PER_MTOK: "0.75", FALLBACK_MODEL_PRICE_OUTPUT_PER_MTOK: "3.75", FALLBACK_MODEL_PRICE_CACHE_READ_PER_MTOK: "0.075" };
    expect(loadConfig({ ...env, ...prices }).fallbackPrices).toBeNull();
    expect(loadConfig({ ...env, ...prices, OPENCLAW_FALLBACK_MODEL: "google/gemini-3.5-flash" }).fallbackPrices).toEqual({ model: "google/gemini-3.5-flash", prices: gemini });
  });
});
