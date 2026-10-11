import { describe, expect, it, vi } from "vitest";
import { verifySignature } from "@openclaw-line/shared";
import { createReportForwarder, createUsageForwarder } from "../src/forwarder.js";

const cfg = { workerUrl: "http://worker:8080/v1/openclaw/report", secret: "whsec", channel: "line", promptVersion: "abc123" };
const REPORT = "🚨 Incident Summary: daily_revenue_pipeline / extract\n...\nRef: INC-7F3A2K";

function setup(status = 200) {
  const fetchMock = vi.fn(async () => new Response("{}", { status }));
  const log = { warn: vi.fn(), info: vi.fn() };
  return { fetchMock, log, forward: createReportForwarder(cfg, { fetch: fetchMock as typeof fetch, log }) };
}

describe("report-capture forwarder", () => {
  it("forwards reports carrying a Ref line, signed for the worker", async () => {
    const { fetchMock, forward } = setup();
    expect(await forward({ content: REPORT, success: true, messageId: "m1" }, { channelId: "line" })).toBe(true);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(cfg.workerUrl);
    const headers = init.headers as Record<string, string>;
    const body = String(init.body);
    expect(verifySignature("whsec", body, headers["x-openclaw-timestamp"], headers["x-openclaw-signature"])).toBe(true);
    expect(JSON.parse(body)).toEqual({ ref: "INC-7F3A2K", content: REPORT, success: true, messageId: "m1", promptVersion: "abc123" });
  });

  it("ignores other channels and messages without a Ref line", async () => {
    const { fetchMock, forward } = setup();
    expect(await forward({ content: REPORT }, { channelId: "slack" })).toBe(false);
    expect(await forward({ content: "hello" }, { channelId: "line" })).toBe(false);
    expect(await forward({ content: "Ref: INC-12" }, { channelId: "line" })).toBe(false);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("never throws when the worker is down", async () => {
    const log = { warn: vi.fn() };
    const forward = createReportForwarder(cfg, { fetch: (async () => { throw new Error("ECONNREFUSED"); }) as typeof fetch, log });
    expect(await forward({ content: REPORT }, undefined)).toBe(false);
    expect(log.warn).toHaveBeenCalledWith(expect.stringContaining("ECONNREFUSED"));
    const { forward: f500, log: log500 } = setup(500);
    expect(await f500({ content: REPORT }, { channelId: "line" })).toBe(false);
    expect(log500.warn).toHaveBeenCalledWith(expect.stringContaining("HTTP 500"));
  });
});

describe("usage forwarder", () => {
  const ucfg = { usageUrl: "http://worker:8080/v1/openclaw/usage", secret: "whsec", agentId: "pipeline-investigator" };
  it("forwards the investigator's token counts by run id, signed, without any text", async () => {
    const fetchMock = vi.fn(async () => new Response("{}", { status: 202 }));
    const forward = createUsageForwarder(ucfg, { fetch: fetchMock as typeof fetch, log: { warn: vi.fn() } });
    const event = { runId: "run-1", provider: "moonshot", model: "kimi-k3", usage: { input: 1200.4, output: 300, cacheRead: 5000, cacheWrite: -1 }, assistantTexts: ["secret report"] };
    expect(await forward(event, { agentId: "pipeline-investigator", sessionKey: "agent:pipeline-investigator:hook:abc" })).toBe(true);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(ucfg.usageUrl);
    const body = String(init.body);
    const headers = init.headers as Record<string, string>;
    expect(verifySignature("whsec", body, headers["x-openclaw-timestamp"], headers["x-openclaw-signature"])).toBe(true);
    expect(JSON.parse(body)).toEqual({ runId: "run-1", sessionKey: "agent:pipeline-investigator:hook:abc", provider: "moonshot", model: "kimi-k3", input: 1200, output: 300, cacheRead: 5000, cacheWrite: 0 });
    expect(body).not.toContain("secret report");
  });

  it("ignores other agents and events without usage or run id", async () => {
    const fetchMock = vi.fn(async () => new Response("{}"));
    const forward = createUsageForwarder(ucfg, { fetch: fetchMock as typeof fetch, log: { warn: vi.fn() } });
    expect(await forward({ runId: "r", usage: { input: 1 } }, { agentId: "main" })).toBe(false);
    expect(await forward({ runId: "r" }, { agentId: "pipeline-investigator" })).toBe(false);
    expect(await forward({ usage: { input: 1 } }, { agentId: "pipeline-investigator" })).toBe(false);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

