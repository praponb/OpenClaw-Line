import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { SIGNATURE_HEADER, TIMESTAMP_HEADER, sign } from "@openclaw-line/shared";

/** Last line of every investigator report (see AGENTS.md and the worker's incident message). */
export const REF_RE = /\bRef:\s*(INC-[0-9A-HJKMNP-TV-Z]{6})\b/;

export type ForwarderConfig = {
  workerUrl: string;
  /** HMAC secret shared with the worker (WEBHOOK_SECRET). */
  secret: string;
  channel: string;
  promptVersion?: string;
};

export type SentEvent = { content?: string; success?: boolean; messageId?: string };
export type SentContext = { channelId?: string };
type Log = { warn: (msg: string) => void; info?: (msg: string) => void };

/** Short content hash of the prompt file, so feedback can be compared across prompt edits. */
export function promptVersionOf(path: string | undefined): string | undefined {
  if (!path) return undefined;
  try {
    return createHash("sha256").update(readFileSync(path)).digest("hex").slice(0, 12);
  } catch {
    return undefined;
  }
}

/**
 * `message_sent` observer: forward investigator reports to the worker.
 * Observation only - never throws, never changes delivery.
 */
export function createReportForwarder(cfg: ForwarderConfig, deps: { fetch?: typeof fetch; log: Log; now?: () => number }) {
  return async (event: SentEvent, ctx: SentContext | undefined): Promise<boolean> => {
    if (ctx?.channelId && ctx.channelId !== cfg.channel) return false;
    const content = event.content ?? "";
    const ref = REF_RE.exec(content)?.[1];
    if (!ref) return false;
    const body = JSON.stringify({ ref, content, success: event.success !== false, messageId: event.messageId, promptVersion: cfg.promptVersion });
    const ts = String(Math.floor((deps.now ?? Date.now)() / 1000));
    try {
      const res = await (deps.fetch ?? fetch)(cfg.workerUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json", [TIMESTAMP_HEADER]: ts, [SIGNATURE_HEADER]: sign(cfg.secret, ts, body) },
        body,
        signal: AbortSignal.timeout(10_000),
      });
      if (!res.ok) {
        deps.log.warn(`[report-capture] ${ref}: worker returned HTTP ${res.status}`);
        return false;
      }
      deps.log.info?.(`[report-capture] ${ref}: report forwarded (${content.length} chars)`);
      return true;
    } catch (err) {
      deps.log.warn(`[report-capture] ${ref}: forward failed: ${(err as Error).message}`);
      return false;
    }
  };
}
