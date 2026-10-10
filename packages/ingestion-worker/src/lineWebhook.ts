import { decodePostback, ratingAck, reasonAck, reasonPrompt, unknownRefReply } from "./feedback.js";
import type { FeedbackStore } from "./feedbackStore.js";
import { voterId } from "./feedbackStore.js";
import type { LineClient, LineMessage, LineWebhookEvent } from "./line.js";

export type FeedbackDeps = {
  store: FeedbackStore;
  line: LineClient;
  signingKey: string;
  userSalt: string;
  log: Pick<Console, "info" | "warn" | "error">;
  now?: () => number;
};

/**
 * Handle verified LINE webhook events. Only our signed feedback postbacks are
 * acted on; every other event (chat messages, joins, ...) is ignored, because
 * the investigator is push-only.
 */
export async function handleLineEvents(deps: FeedbackDeps, events: LineWebhookEvent[]): Promise<void> {
  const now = deps.now ?? Date.now;
  for (const ev of events) {
    try {
      if (ev.type !== "postback") continue;
      const decoded = decodePostback(deps.signingKey, ev.postback?.data);
      if (!decoded) {
        deps.log.warn("[feedback] ignored postback with unknown or invalid data");
        continue;
      }
      const eventId = ev.webhookEventId ?? `${ev.timestamp}:${ev.postback?.data}`;
      if (!deps.store.claimEvent(eventId, now())) continue; // LINE redelivery

      const { ref, action } = decoded;
      let reply: LineMessage;
      if (!deps.store.getIncident(ref)) {
        reply = unknownRefReply(ref);
      } else {
        const voter = voterId(deps.userSalt, ev.source?.userId, eventId);
        if (action.kind === "rating") {
          deps.store.recordRating(ref, voter, action.rating, now());
          reply = action.rating === "up" ? ratingAck(ref, action.rating) : reasonPrompt(deps.signingKey, ref, action.rating);
        } else {
          deps.store.recordReason(ref, voter, action.reason, now());
          reply = reasonAck(ref, action.reason);
        }
        deps.log.info(`[feedback] ${ref}: ${action.kind === "rating" ? action.rating : `reason=${action.reason}`} voter=${voter}`);
      }
      if (ev.replyToken) await deps.line.reply(ev.replyToken, [reply]);
    } catch (err) {
      deps.log.error(`[feedback] event failed: ${(err as Error).message}`);
    }
  }
}
