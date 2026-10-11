import type { LineClient } from "./line.js";

/**
 * "Test LINE" button on the dashboard: pushes one plain text message to the
 * report target so you can check delivery on your phone. Each test uses one
 * push from the LINE plan's monthly quota, so it is rate-limited.
 */

export type TestResult =
  | { ok: true; sentAt: number; remainingToday: number }
  | { ok: false; status: number; error: string; retryAfterSeconds?: number };

const BANGKOK_OFFSET_MS = 7 * 3600 * 1000;
const bangkokDay = (ms: number) => new Date(ms + BANGKOK_OFFSET_MS).toISOString().slice(0, 10);
const bangkokClock = (ms: number) => new Date(ms + BANGKOK_OFFSET_MS).toISOString().slice(11, 16);

export class TestNotifier {
  private lastSentAt = 0;
  private day = "";
  private sentToday = 0;

  constructor(
    private readonly line: Pick<LineClient, "push"> | undefined,
    private readonly to: string,
    private readonly opts: { now: () => number; cooldownMs?: number; dailyLimit?: number },
  ) {}

  async send(): Promise<TestResult> {
    if (!this.line || !this.to) return { ok: false, status: 503, error: "LINE delivery is not configured (LINE_CHANNEL_ACCESS_TOKEN / LINE_TARGET_ID)." };
    const now = this.opts.now();
    const cooldown = this.opts.cooldownMs ?? 30_000;
    const limit = this.opts.dailyLimit ?? 10;
    if (bangkokDay(now) !== this.day) {
      this.day = bangkokDay(now);
      this.sentToday = 0;
    }
    if (now - this.lastSentAt < cooldown) {
      const wait = Math.ceil((cooldown - (now - this.lastSentAt)) / 1000);
      return { ok: false, status: 429, error: `Please wait ${wait}s before sending another test.`, retryAfterSeconds: wait };
    }
    if (this.sentToday >= limit) {
      return { ok: false, status: 429, error: `Daily limit of ${limit} test messages reached (each one uses LINE push quota). Try again tomorrow (Bangkok time).` };
    }
    // Count the attempt before sending so double-clicks can't slip past the limit.
    this.lastSentAt = now;
    this.sentToday++;
    try {
      await this.line.push(this.to, [
        {
          type: "text",
          text: `🔔 Test notification\nSent from the incident dashboard at ${bangkokClock(now)} (Bangkok time).\nIf you can read this on your phone, LINE delivery works. This is not an incident.`,
        },
      ]);
    } catch (err) {
      return { ok: false, status: 502, error: `LINE push failed: ${(err as Error).message.slice(0, 200)}` };
    }
    return { ok: true, sentAt: now, remainingToday: limit - this.sentToday };
  }
}

/**
 * The button is the dashboard's only state-changing action. Browsers attach the
 * Cloudflare Access cookie to cross-site requests too, so require a custom header
 * (cross-origin pages can't send one without a CORS preflight we never answer)
 * and reject requests whose Origin or Sec-Fetch-Site says they came from elsewhere.
 */
export function isSameOriginAction(headers: Record<string, string | string[] | undefined>): boolean {
  const h = (name: string) => {
    const v = headers[name];
    return Array.isArray(v) ? v[0] : v;
  };
  if (h("x-dashboard-action") !== "send-test") return false;
  const site = h("sec-fetch-site");
  if (site && site !== "same-origin") return false;
  const origin = h("origin");
  if (origin) {
    try {
      if (new URL(origin).host !== h("host")) return false;
    } catch {
      return false;
    }
  }
  return true;
}
