import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";

export const LINE_SIGNATURE_HEADER = "x-line-signature";

/** LINE Messaging API message object (only the shapes this worker sends). */
export type LineMessage = Record<string, unknown> & { type: string };

/** Subset of a LINE webhook event used by the feedback handler. */
export type LineWebhookEvent = {
  type: string;
  webhookEventId?: string;
  timestamp?: number;
  replyToken?: string;
  source?: { type?: string; userId?: string; groupId?: string; roomId?: string };
  postback?: { data?: string };
  deliveryContext?: { isRedelivery?: boolean };
};

/** `x-line-signature`: base64 HMAC-SHA256 of the raw request body with the channel secret. */
export function verifyLineSignature(channelSecret: string, rawBody: Buffer, signature: string | undefined): boolean {
  if (!signature) return false;
  const expected = Buffer.from(createHmac("sha256", channelSecret).update(rawBody).digest("base64"));
  const given = Buffer.from(signature);
  return expected.length === given.length && timingSafeEqual(expected, given);
}

/** Minimal LINE Messaging API client: push (counts against quota) and reply (free). */
export class LineClient {
  private readonly baseUrl: string;
  private pushObserver: ((err?: Error) => void) | undefined;

  constructor(
    private readonly accessToken: string,
    private readonly deps: { fetch?: typeof fetch; baseUrl?: string } = {},
  ) {
    this.baseUrl = (deps.baseUrl ?? "https://api.line.me").replace(/\/+$/, "");
  }

  /** Push to a user/group. The retry key makes a retried push idempotent on LINE's side. */
  async push(to: string, messages: LineMessage[], retryKey: string = randomUUID()): Promise<void> {
    try {
      await this.post("/v2/bot/message/push", { to, messages }, { "X-Line-Retry-Key": retryKey });
    } catch (err) {
      this.pushObserver?.(err as Error);
      throw err;
    }
    this.pushObserver?.();
  }

  /** Called after every push (with the error when it failed); used for alerting. Replies are not observed. */
  observePushes(fn: (err?: Error) => void): void {
    this.pushObserver = fn;
  }

  /** This month's message quota and usage, as LINE counts them (read-only). */
  async quota(): Promise<{ limit: number | null; used: number }> {
    const get = async <T>(path: string): Promise<T> => {
      const res = await (this.deps.fetch ?? fetch)(`${this.baseUrl}${path}`, {
        headers: { Authorization: `Bearer ${this.accessToken}` },
        signal: AbortSignal.timeout(15_000),
      });
      if (!res.ok) throw new Error(`LINE API ${res.status} on ${path}`);
      return (await res.json()) as T;
    };
    const [q, c] = await Promise.all([get<{ type?: string; value?: number }>("/v2/bot/message/quota"), get<{ totalUsage?: number }>("/v2/bot/message/quota/consumption")]);
    return { limit: q.type === "limited" && typeof q.value === "number" ? q.value : null, used: c.totalUsage ?? 0 };
  }

  async reply(replyToken: string, messages: LineMessage[]): Promise<void> {
    await this.post("/v2/bot/message/reply", { replyToken, messages });
  }

  private async post(path: string, body: unknown, headers: Record<string, string> = {}): Promise<void> {
    const res = await (this.deps.fetch ?? fetch)(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${this.accessToken}`, ...headers },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(15_000),
    });
    // 409 on push = same X-Line-Retry-Key already accepted.
    if (!res.ok && res.status !== 409) {
      throw new Error(`LINE ${path} HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
    }
  }
}
