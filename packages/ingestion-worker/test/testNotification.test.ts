import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import type { Dispatcher } from "../src/dispatcher.js";
import { FeedbackStore } from "../src/feedbackStore.js";
import type { LineClient } from "../src/line.js";
import { createWorker } from "../src/server.js";
import { TestNotifier, isSameOriginAction } from "../src/testNotification.js";

// 2026-10-11 08:40 UTC = 15:40 Bangkok.
const T0 = Date.UTC(2026, 9, 11, 8, 40);

describe("TestNotifier", () => {
  it("pushes one text message in Bangkok time, then enforces cooldown and the daily limit", async () => {
    let t = T0;
    const line = { push: vi.fn(async () => {}) };
    const n = new TestNotifier(line, "C123", { now: () => t, cooldownMs: 30_000, dailyLimit: 2 });
    expect(await n.send()).toEqual({ ok: true, sentAt: T0, remainingToday: 1 });
    const [to, messages] = line.push.mock.calls[0] as unknown as [string, { type: string; text: string }[]];
    expect(to).toBe("C123");
    expect(messages[0]!.text).toContain("at 15:40 (Bangkok time)");
    expect(messages[0]!.text).toContain("This is not an incident.");
    t += 10_000;
    expect(await n.send()).toMatchObject({ ok: false, status: 429, retryAfterSeconds: 20 });
    t += 30_000;
    expect(await n.send()).toMatchObject({ ok: true, remainingToday: 0 });
    t += 60_000;
    expect(await n.send()).toMatchObject({ ok: false, status: 429, error: expect.stringContaining("Daily limit of 2") });
    t = Date.UTC(2026, 9, 11, 17, 5); // 00:05 next day in Bangkok
    expect(await n.send()).toMatchObject({ ok: true, remainingToday: 1 });
    expect(line.push).toHaveBeenCalledTimes(3);
  });

  it("reports LINE failures and missing configuration", async () => {
    const failing = new TestNotifier({ push: vi.fn(async () => { throw new Error("LINE API 429 monthly limit"); }) }, "C1", { now: () => T0 });
    expect(await failing.send()).toMatchObject({ ok: false, status: 502, error: "LINE push failed: LINE API 429 monthly limit" });
    expect(await new TestNotifier(undefined, "C1", { now: () => T0 }).send()).toMatchObject({ ok: false, status: 503 });
  });
});

describe("isSameOriginAction", () => {
  const ok = { "x-dashboard-action": "send-test", host: "dashboard.example.com" };
  it.each([
    ["page fetch", ok, true],
    ["same-origin with Origin", { ...ok, origin: "https://dashboard.example.com", "sec-fetch-site": "same-origin" }, true],
    ["missing header (plain form post)", { host: "dashboard.example.com" }, false],
    ["cross-site", { ...ok, "sec-fetch-site": "cross-site" }, false],
    ["foreign Origin", { ...ok, origin: "https://evil.example" }, false],
  ])("%s", (_name, headers, expected) => {
    expect(isSameOriginAction(headers)).toBe(expected);
  });
});

describe("/api/test-notification", () => {
  const servers: { close: () => void }[] = [];
  afterEach(() => servers.splice(0).forEach((s) => s.close()));
  const dispatcher: Dispatcher = { dispatch: vi.fn() };
  const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const env = {
    WEBHOOK_SECRET: "w",
    OPENCLAW_HOOK_TOKEN: "t",
    LINE_TARGET_ID: "C1",
    LINE_CHANNEL_ACCESS_TOKEN: "tok",
    LINE_CHANNEL_SECRET: "sec",
    FEEDBACK_DB_PATH: ":memory:",
    CF_ACCESS_TEAM_DOMAIN: "team.cloudflareaccess.com",
    CF_ACCESS_AUD: "aud",
    DASHBOARD_ALLOWED_EMAILS: "me@example.com",
  };

  async function listen(server: import("node:http").Server): Promise<number> {
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    servers.push(server);
    return (server.address() as AddressInfo).port;
  }

  it("sends on POST with the dashboard header, behind Cloudflare Access on the public listener", async () => {
    const line = { push: vi.fn(async () => {}) } as unknown as LineClient;
    const accessVerifier = { verify: vi.fn(async (token: string | undefined) => (token === "good" ? { ok: true as const, email: "me@example.com" } : { ok: false as const, reason: "missing" })) };
    const worker = createWorker(loadConfig(env), dispatcher, { store: new FeedbackStore(":memory:"), line, log, accessVerifier, now: () => T0 });
    const port = await listen(worker.dashboardServer!);
    const url = `http://127.0.0.1:${port}/api/test-notification`;

    expect((await fetch(url, { method: "POST", headers: { "X-Dashboard-Action": "send-test" } })).status).toBe(403); // no Access token
    const auth = { "Cf-Access-Jwt-Assertion": "good" };
    expect((await fetch(url, { method: "POST", headers: auth })).status).toBe(403); // no dashboard header
    expect((await fetch(url, { headers: { ...auth, "X-Dashboard-Action": "send-test" } })).status).toBe(405); // GET
    const res = await fetch(url, { method: "POST", headers: { ...auth, "X-Dashboard-Action": "send-test" } });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ ok: true, remainingToday: 9 });
    expect(line.push).toHaveBeenCalledTimes(1);
    expect(log.info).toHaveBeenCalledWith(expect.stringContaining("test notification by me@example.com: sent"));
    expect((await fetch(url, { method: "POST", headers: { ...auth, "X-Dashboard-Action": "send-test" } })).status).toBe(429); // cooldown
  });
});
