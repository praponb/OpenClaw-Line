import { createHmac, timingSafeEqual } from "node:crypto";
import type { LineMessage } from "./line.js";

/**
 * Phase 3 feedback UI (spec §6): a Flex card with 👍 / 👎 / "Misleading"
 * postback buttons sent after each report, plus a quick-reply reason prompt
 * after a negative rating.
 */

export const RATINGS = { up: "👍 Helpful", down: "👎 Not helpful", misleading: "⚠️ Misleading" } as const;
export type Rating = keyof typeof RATINGS;

// Quick-reply labels: LINE limit is 20 characters.
export const REASONS = {
  root_cause: "Wrong root cause",
  blast_radius: "Missed blast radius",
  made_up: "Made-up fact",
  not_actionable: "Not actionable",
} as const;
export type Reason = keyof typeof REASONS;

export type FeedbackAction = { kind: "rating"; rating: Rating } | { kind: "reason"; reason: Reason };

const VERSION = "fb1";

/**
 * Postback data: `fb1|<ref>|<r:rating | why:reason>|<sig>`. The HMAC stops
 * anyone from forging votes for arbitrary refs; LINE's limit is 300 chars.
 */
export function encodePostback(signingKey: string, ref: string, action: FeedbackAction): string {
  const body = `${VERSION}|${ref}|${action.kind === "rating" ? `r:${action.rating}` : `why:${action.reason}`}`;
  return `${body}|${postbackSig(signingKey, body)}`;
}

export function decodePostback(signingKey: string, data: string | undefined): { ref: string; action: FeedbackAction } | undefined {
  if (!data) return undefined;
  const parts = data.split("|");
  if (parts.length !== 4 || parts[0] !== VERSION) return undefined;
  const [, ref, code, sig] = parts as [string, string, string, string];
  const expected = Buffer.from(postbackSig(signingKey, `${VERSION}|${ref}|${code}`));
  const given = Buffer.from(sig);
  if (expected.length !== given.length || !timingSafeEqual(expected, given)) return undefined;
  const [kind, value] = code.split(":", 2);
  if (kind === "r" && value && value in RATINGS) return { ref, action: { kind: "rating", rating: value as Rating } };
  if (kind === "why" && value && value in REASONS) return { ref, action: { kind: "reason", reason: value as Reason } };
  return undefined;
}

function postbackSig(signingKey: string, body: string): string {
  return createHmac("sha256", signingKey).update(body).digest("base64url").slice(0, 22);
}

export type CardIncident = { ref: string; dagId: string; taskId: string };

export function feedbackCard(signingKey: string, incident: CardIncident): LineMessage {
  const button = (rating: Rating, style: "primary" | "secondary") => ({
    type: "button",
    style,
    height: "sm",
    action: { type: "postback", label: RATINGS[rating], data: encodePostback(signingKey, incident.ref, { kind: "rating", rating }) },
  });
  const small = (text: string) => ({ type: "text", text, size: "xs", color: "#888888", wrap: true });
  return {
    type: "flex",
    altText: `Was this triage helpful? (${incident.ref})`,
    contents: {
      type: "bubble",
      body: {
        type: "box",
        layout: "vertical",
        spacing: "sm",
        contents: [
          { type: "text", text: "Was this triage helpful?", weight: "bold", size: "md", wrap: true },
          small(clip(`${incident.dagId} / ${incident.taskId}`, 120)),
          small(`Ref ${incident.ref}`),
        ],
      },
      footer: {
        type: "box",
        layout: "vertical",
        spacing: "sm",
        contents: [button("up", "primary"), button("down", "secondary"), button("misleading", "secondary")],
      },
    },
  };
}

/** Reply to a 👎 / ⚠️ tap: acknowledge and ask why, as quick-reply buttons. */
export function reasonPrompt(signingKey: string, ref: string, rating: Rating): LineMessage {
  return {
    type: "text",
    text: `Recorded ${RATINGS[rating]} for ${ref}. What was wrong?`,
    quickReply: {
      items: (Object.keys(REASONS) as Reason[]).map((reason) => ({
        type: "action",
        action: { type: "postback", label: REASONS[reason], data: encodePostback(signingKey, ref, { kind: "reason", reason }) },
      })),
    },
  };
}

export function ratingAck(ref: string, rating: Rating): LineMessage {
  return { type: "text", text: `Recorded ${RATINGS[rating]} for ${ref}. Thanks!` };
}

export function reasonAck(ref: string, reason: Reason): LineMessage {
  return { type: "text", text: `Noted "${REASONS[reason]}" for ${ref}. Thanks!` };
}

export function unknownRefReply(ref: string): LineMessage {
  return { type: "text", text: `Feedback for ${ref} could not be recorded: unknown or expired incident.` };
}

export type FallbackIncident = CardIncident & { runId: string; logUrl?: string };

/** Sent instead of the feedback card when the investigation or its delivery failed. */
export function fallbackMessage(incident: FallbackIncident, reason: string): LineMessage {
  const lines = [
    `⚠️ Pipeline investigator could not deliver a triage report (${incident.ref}).`,
    `DAG/task: ${incident.dagId} / ${incident.taskId}`,
    `Run: ${incident.runId}`,
    `Reason: ${reason}`,
    "Please check the task in Airflow.",
  ];
  if (incident.logUrl) lines.push(incident.logUrl);
  return { type: "text", text: clip(lines.join("\n"), 4_000) };
}

function clip(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + "…" : s;
}
