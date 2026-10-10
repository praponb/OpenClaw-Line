import { createHmac } from "node:crypto";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import type { Dispatcher, DispatchResult } from "../src/dispatcher.js";
import { decodePostback, encodePostback } from "../src/feedback.js";
import { FeedbackStore } from "../src/feedbackStore.js";
import { LineClient } from "../src/line.js";
import { createWorker } from "../src/server.js";
import { sign } from "../src/signature.js";
import type { FailureEvent } from "../src/types.js";

const env = {
  WEBHOOK_SECRET: "whsec_test",
  OPENCLAW_HOOK_TOKEN: "hook-token",
  LINE_TARGET_ID: "C0123456789abcdef0123456789abcdef",
  LINE_CHANNEL_ACCESS_TOKEN: "line-token",
  LINE_CHANNEL_SECRET: "chan-secret",
  FEEDBACK_SIGNING_KEY: "sign-key",
  FEEDBACK_USER_SALT: "salt",
  FEEDBACK_DB_PATH: ":memory:",
};

const EVENTS: FailureEvent[] = [
  { dag_id: "daily_revenue_pipeline", task_id: "extract_stripe_transactions", run_id: "manual__1", log_url: "http://af/log", receivedAt: 1_000 },
];

type Sent = { path: string; body: any; headers: Record<string, string> };

function setup(result: DispatchResult) {
  const sent: Sent[] = [];
  const lineFetch = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
    sent.push({ path: new URL(String(url)).pathname, body: JSON.parse(String(init?.body)), headers: init?.headers as Record<string, string> });
    return new Response("{}", { status: 200 });
  });
  const store = new FeedbackStore(":memory:");
  const dispatcher: Dispatcher = { dispatch: vi.fn(async () => result) };
  const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const worker = createWorker(loadConfig(env), dispatcher, {
    store,
    line: new LineClient("line-token", { fetch: lineFetch as typeof fetch }),
    log,
    now: () => 61_000,
  });
  return { worker, store, sent, log };
}

const servers: { close: () => void }[] = [];
afterEach(() => servers.splice(0).forEach((s) => s.close()));

async function listen(server: import("node:http").Server): Promise<number> {
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  servers.push(server);
  return (server.address() as AddressInfo).port;
}

function lineRequest(port: number, events: unknown[], secret = "chan-secret") {
  const raw = JSON.stringify({ destination: "Ubot", events });
  return fetch(`http://127.0.0.1:${port}/line/webhook`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-line-signature": createHmac("sha256", secret).update(raw).digest("base64") },
    body: raw,
  });
}

const postback = (data: string, id: string, userId = "U1") => ({
  type: "postback",
  webhookEventId: id,
  replyToken: `rt-${id}`,
  source: { type: "group", groupId: "C1", userId },
  postback: { data },
});

describe("after the investigation settles", () => {
  it("pushes the feedback card when the report was delivered", async () => {
    const { worker, store, sent } = setup({ ok: true, detail: "", completion: { status: "ok", delivered: true, replyDisposition: "visible" } });
    await worker.investigate("daily_revenue_pipeline:manual__1", EVENTS);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.path).toBe("/v2/bot/message/push");
    expect(sent[0]!.body.to).toBe(env.LINE_TARGET_ID);
    expect(sent[0]!.body.messages[0].type).toBe("flex");
    expect(sent[0]!.headers["X-Line-Retry-Key"]).toMatch(/^[0-9a-f-]{36}$/);
    const ref = sent[0]!.body.messages[0].altText.match(/INC-\w{6}/)[0];
    expect(store.getIncident(ref)).toMatchObject({ status: "ok", delivered: 1, first_seen_at: 1_000, task_id: "extract_stripe_transactions" });
  });

  it("pushes a fallback notice instead when delivery failed", async () => {
    const { worker, sent } = setup({ ok: true, detail: "", completion: { status: "ok", delivered: false, deliveryError: "delivery-failed", replyDisposition: "visible" } });
    await worker.investigate("daily_revenue_pipeline:manual__1", EVENTS);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.body.messages[0].type).toBe("text");
    expect(sent[0]!.body.messages[0].text).toContain("could not be delivered to LINE");
    expect(sent[0]!.body.messages[0].text).toContain("http://af/log");
  });

  it("pushes a fallback notice when the run errored or never started", async () => {
    const errored = setup({ ok: true, detail: "", completion: { status: "error", delivered: false } });
    await errored.worker.investigate("daily_revenue_pipeline:manual__1", EVENTS);
    expect(errored.sent[0]!.body.messages[0].text).toContain('status "error"');

    const down = setup({ ok: false, detail: "HTTP 503" });
    await down.worker.investigate("daily_revenue_pipeline:manual__1", EVENTS);
    expect(down.sent[0]!.body.messages[0].text).toContain("could not be started");
  });
});

describe("report capture endpoint", () => {
  it("stores the signed report for a known ref and rejects unsigned requests", async () => {
    const { worker, store, sent } = setup({ ok: true, detail: "", completion: { status: "ok", delivered: true } });
    await worker.investigate("daily_revenue_pipeline:manual__1", EVENTS);
    const ref = sent[0]!.body.messages[0].altText.match(/INC-\w{6}/)[0];
    const port = await listen(worker.server);
    const body = JSON.stringify({ ref, content: `🚨 Incident Summary\nRef: ${ref}`, success: true, messageId: "m1", promptVersion: "p1" });
    const ts = String(Math.floor(Date.now() / 1000));
    const url = `http://127.0.0.1:${port}/v1/openclaw/report`;
    const ok = await fetch(url, { method: "POST", headers: { "x-openclaw-timestamp": ts, "x-openclaw-signature": sign("whsec_test", ts, body) }, body });
    expect(ok.status).toBe(200);
    expect(store.getIncident(ref)).toMatchObject({ report_text: `🚨 Incident Summary\nRef: ${ref}`, prompt_version: "p1" });
    const bad = await fetch(url, { method: "POST", headers: { "x-openclaw-timestamp": ts, "x-openclaw-signature": "sha256=00" }, body });
    expect(bad.status).toBe(401);
  });
});

describe("LINE webhook", () => {
  async function ready() {
    const ctx = setup({ ok: true, detail: "", completion: { status: "ok", delivered: true } });
    await ctx.worker.investigate("daily_revenue_pipeline:manual__1", EVENTS);
    const ref: string = ctx.sent[0]!.body.messages[0].altText.match(/INC-\w{6}/)[0];
    ctx.sent.length = 0;
    const port = await listen(ctx.worker.publicServer!);
    return { ...ctx, ref, port };
  }

  it("records a 👎 tap, asks why, then records the reason", async () => {
    const { store, sent, ref, port } = await ready();
    const res = await lineRequest(port, [postback(encodePostback("sign-key", ref, { kind: "rating", rating: "down" }), "e1")]);
    expect(res.status).toBe(200);
    expect(sent[0]!.path).toBe("/v2/bot/message/reply");
    expect(sent[0]!.body.replyToken).toBe("rt-e1");
    const prompt = sent[0]!.body.messages[0];
    expect(prompt.text).toContain(`Recorded 👎 Not helpful for ${ref}`);
    const reasonData = prompt.quickReply.items[0].action.data;
    expect(decodePostback("sign-key", reasonData)?.action).toEqual({ kind: "reason", reason: "root_cause" });

    await lineRequest(port, [postback(reasonData, "e2")]);
    expect(sent[1]!.body.messages[0].text).toContain('Noted "Wrong root cause"');
    const fb = store.getFeedback(ref);
    expect(fb).toMatchObject([{ rating: "down", reason: "root_cause" }]);
    expect(fb[0]!.voter).not.toContain("U1");
  });

  it("ignores redeliveries, foreign postbacks and chat messages; rejects bad signatures", async () => {
    const { store, sent, ref, port } = await ready();
    const tap = postback(encodePostback("sign-key", ref, { kind: "rating", rating: "up" }), "e1");
    await lineRequest(port, [tap]);
    await lineRequest(port, [tap]); // LINE redelivery
    await lineRequest(port, [postback("fb1|INC-AAAAAA|r:up|forged", "e3"), { type: "message", webhookEventId: "e4", message: { type: "text", text: "hi" } }]);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.body.messages[0].text).toContain(`Recorded 👍 Helpful for ${ref}`);
    expect(store.getFeedback(ref)).toHaveLength(1);
    expect((await lineRequest(port, [tap], "wrong-secret")).status).toBe(401);
    expect((await lineRequest(port, [])).status).toBe(200); // LINE "Verify" request
  });

  it("answers taps for unknown incidents without recording them", async () => {
    const { store, sent, port } = await ready();
    await lineRequest(port, [postback(encodePostback("sign-key", "INC-ZZZZZZ", { kind: "rating", rating: "up" }), "e9")]);
    expect(sent[0]!.body.messages[0].text).toContain("unknown or expired incident");
    expect(store.getFeedback("INC-ZZZZZZ")).toHaveLength(0);
  });
});
