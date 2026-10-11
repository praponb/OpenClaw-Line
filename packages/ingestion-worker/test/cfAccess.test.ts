import { generateKeyPairSync, sign as rsaSign } from "node:crypto";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AccessVerifier, accessToken } from "../src/cfAccess.js";
import { loadConfig } from "../src/config.js";
import type { Dispatcher } from "../src/dispatcher.js";
import { FeedbackStore } from "../src/feedbackStore.js";
import { LineClient } from "../src/line.js";
import { createWorker } from "../src/server.js";

const TEAM = "praponb.cloudflareaccess.com";
const AUD = "aud-tag-123";
const NOW = 1_800_000_000_000;
const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const other = generateKeyPairSync("rsa", { modulusLength: 2048 });

function jwt(claims: Record<string, unknown>, opts: { kid?: string; key?: typeof privateKey; alg?: string } = {}): string {
  const b64 = (o: unknown) => Buffer.from(JSON.stringify(o)).toString("base64url");
  const h = b64({ alg: opts.alg ?? "RS256", kid: opts.kid ?? "k1", typ: "JWT" });
  const p = b64({ iss: `https://${TEAM}`, aud: [AUD], email: "Me@Example.com", exp: NOW / 1000 + 600, nbf: NOW / 1000 - 10, ...claims });
  return `${h}.${p}.${rsaSign("RSA-SHA256", Buffer.from(`${h}.${p}`), opts.key ?? privateKey).toString("base64url")}`;
}

function verifier() {
  const jwk = { ...publicKey.export({ format: "jwk" }), kid: "k1" };
  const fetchMock = vi.fn(async () => new Response(JSON.stringify({ keys: [jwk] }), { status: 200 }));
  return { fetchMock, v: new AccessVerifier({ teamDomain: TEAM, aud: AUD, allowedEmails: ["me@example.com"] }, { fetch: fetchMock as typeof fetch, now: () => NOW }) };
}

describe("AccessVerifier", () => {
  it("accepts a valid token for an allowed email and caches the signing keys", async () => {
    const { v, fetchMock } = verifier();
    expect(await v.verify(jwt({}))).toEqual({ ok: true, email: "me@example.com" });
    expect(await v.verify(jwt({}))).toMatchObject({ ok: true });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]![0]).toBe(`https://${TEAM}/cdn-cgi/access/certs`);
  });

  it("rejects missing, forged, expired, foreign or not-allowed tokens", async () => {
    const { v } = verifier();
    const reason = async (t: string | undefined) => {
      const r = await v.verify(t);
      return r.ok ? "ok" : r.reason;
    };
    expect(await reason(undefined)).toBe("missing Cloudflare Access token");
    expect(await reason("abc")).toBe("malformed token");
    expect(await reason(jwt({}, { key: other.privateKey }))).toBe("bad signature");
    expect(await reason(jwt({}, { kid: "unknown" }))).toBe("unknown signing key");
    expect(await reason(jwt({}, { alg: "HS256" }))).toBe("unsupported token algorithm");
    expect(await reason(jwt({ aud: ["someone-else"] }))).toBe("wrong audience");
    expect(await reason(jwt({ iss: "https://evil.cloudflareaccess.com" }))).toBe("wrong issuer");
    expect(await reason(jwt({ exp: NOW / 1000 - 3600 }))).toBe("token expired");
    expect(await reason(jwt({ email: "intruder@example.com" }))).toBe("email not allowed: intruder@example.com");
    expect(await reason(jwt({ email: undefined }))).toBe("email not allowed: none");
  });

  it("reads the token from the Access header or the CF_Authorization cookie", () => {
    expect(accessToken({ "cf-access-jwt-assertion": "a.b.c" })).toBe("a.b.c");
    expect(accessToken({ cookie: "x=1; CF_Authorization=d.e.f; y=2" })).toBe("d.e.f");
    expect(accessToken({})).toBeUndefined();
  });
});

describe("public dashboard listener", () => {
  const servers: { close: () => void }[] = [];
  afterEach(() => servers.splice(0).forEach((s) => s.close()));
  const base = { WEBHOOK_SECRET: "w", OPENCLAW_HOOK_TOKEN: "t", LINE_TARGET_ID: "C1", LINE_CHANNEL_ACCESS_TOKEN: "tok", LINE_CHANNEL_SECRET: "sec", FEEDBACK_DB_PATH: ":memory:" };
  const dispatcher: Dispatcher = { dispatch: vi.fn() };
  const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

  it("is not created unless Access is fully configured", () => {
    expect(loadConfig(base).dashboard).toBeUndefined();
    expect(loadConfig({ ...base, CF_ACCESS_TEAM_DOMAIN: TEAM, CF_ACCESS_AUD: AUD }).dashboard).toBeUndefined();
    const cfg = loadConfig({ ...base, CF_ACCESS_TEAM_DOMAIN: `https://${TEAM}/`, CF_ACCESS_AUD: AUD, DASHBOARD_ALLOWED_EMAILS: "Me@Example.com, b@x.io" });
    expect(cfg.dashboard).toEqual({ port: 8092, access: { teamDomain: TEAM, aud: AUD, allowedEmails: ["me@example.com", "b@x.io"] } });
    expect(createWorker(loadConfig(base), dispatcher, { log, store: new FeedbackStore(":memory:"), line: new LineClient("tok") }).dashboardServer).toBeUndefined();
  });

  it("serves only dashboard routes, and only with a valid Access token", async () => {
    const cfg = loadConfig({ ...base, CF_ACCESS_TEAM_DOMAIN: TEAM, CF_ACCESS_AUD: AUD, DASHBOARD_ALLOWED_EMAILS: "me@example.com" });
    const { v } = verifier();
    const worker = createWorker(cfg, dispatcher, { log, store: new FeedbackStore(":memory:"), line: new LineClient("tok"), accessVerifier: v, now: () => NOW });
    await new Promise<void>((r) => worker.dashboardServer!.listen(0, "127.0.0.1", r));
    servers.push(worker.dashboardServer!);
    const url = `http://127.0.0.1:${(worker.dashboardServer!.address() as AddressInfo).port}`;
    const good = { "cf-access-jwt-assertion": jwt({}) };

    expect((await fetch(`${url}/dashboard`)).status).toBe(403);
    expect((await fetch(`${url}/api/dashboard`, { headers: { "cf-access-jwt-assertion": jwt({ email: "x@y.z" }) } })).status).toBe(403);
    expect((await fetch(`${url}/dashboard`, { headers: good })).status).toBe(200);
    expect((await (await fetch(`${url}/api/dashboard`, { headers: good })).json()).enabled).toBe(true);
    // Internal endpoints are never reachable here, even when signed in.
    expect((await fetch(`${url}/v1/airflow/failure`, { method: "POST", headers: good, body: "{}" })).status).toBe(404);
    expect((await fetch(`${url}/v1/openclaw/report`, { method: "POST", headers: good, body: "{}" })).status).toBe(404);
    expect((await fetch(`${url}/healthz`, { headers: good })).status).toBe(404);
  });
});
