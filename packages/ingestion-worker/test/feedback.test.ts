import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";
import { REASONS, decodePostback, encodePostback, fallbackMessage, feedbackCard, reasonPrompt } from "../src/feedback.js";
import { quantile, renderFeedbackReport } from "../src/feedbackReport.js";
import { FeedbackStore, voterId } from "../src/feedbackStore.js";
import { INCIDENT_REF_RE, incidentRef } from "../src/incidentRef.js";
import { verifyLineSignature } from "../src/line.js";

const KEY = "signing-key";

describe("incidentRef", () => {
  it("is short, readable and stable per incident key", () => {
    const a = incidentRef("daily_revenue_pipeline:manual__2026-10-10");
    expect(a).toMatch(INCIDENT_REF_RE);
    expect(incidentRef("daily_revenue_pipeline:manual__2026-10-10")).toBe(a);
    expect(incidentRef("daily_revenue_pipeline:manual__2026-10-11")).not.toBe(a);
  });
});

describe("postback data", () => {
  it("round-trips ratings and reasons within LINE's 300-char limit", () => {
    const rating = encodePostback(KEY, "INC-7F3A2K", { kind: "rating", rating: "misleading" });
    expect(rating.length).toBeLessThanOrEqual(300);
    expect(decodePostback(KEY, rating)).toEqual({ ref: "INC-7F3A2K", action: { kind: "rating", rating: "misleading" } });
    const reason = encodePostback(KEY, "INC-7F3A2K", { kind: "reason", reason: "blast_radius" });
    expect(decodePostback(KEY, reason)).toEqual({ ref: "INC-7F3A2K", action: { kind: "reason", reason: "blast_radius" } });
  });

  it("rejects forged, tampered or foreign data", () => {
    const good = encodePostback(KEY, "INC-7F3A2K", { kind: "rating", rating: "up" });
    expect(decodePostback("other-key", good)).toBeUndefined();
    expect(decodePostback(KEY, good.replace("INC-7F3A2K", "INC-000000"))).toBeUndefined();
    expect(decodePostback(KEY, good.replace("r:up", "r:down"))).toBeUndefined();
    expect(decodePostback(KEY, "action=buy&item=1")).toBeUndefined();
    expect(decodePostback(KEY, undefined)).toBeUndefined();
  });
});

describe("LINE messages", () => {
  it("feedback card has the three spec buttons plus Resolved, all with signed postbacks", () => {
    const card = feedbackCard(KEY, { ref: "INC-7F3A2K", dagId: "daily_revenue_pipeline", taskId: "extract_stripe_transactions" }) as any;
    expect(card.type).toBe("flex");
    expect(card.altText).toContain("INC-7F3A2K");
    const buttons = card.contents.footer.contents.filter((c: any) => c.type === "button");
    expect(buttons.map((b: any) => b.action.label)).toEqual(["👍 Helpful", "👎 Not helpful", "⚠️ Misleading", "✅ Resolved"]);
    expect(buttons.map((b: any) => decodePostback(KEY, b.action.data)?.action)).toEqual([
      { kind: "rating", rating: "up" },
      { kind: "rating", rating: "down" },
      { kind: "rating", rating: "misleading" },
      { kind: "resolve" },
    ]);
    expect(decodePostback("other-key", buttons[3].action.data)).toBeUndefined();
    expect(JSON.stringify(card).length).toBeLessThan(30_000); // Flex bubble limit
  });

  it("reason prompt offers every reason as a quick reply within the 20-char label limit", () => {
    const msg = reasonPrompt(KEY, "INC-7F3A2K", "down") as any;
    const items = msg.quickReply.items;
    expect(items).toHaveLength(Object.keys(REASONS).length);
    for (const item of items) {
      expect(item.action.label.length).toBeLessThanOrEqual(20);
      expect(decodePostback(KEY, item.action.data)?.action.kind).toBe("reason");
    }
  });

  it("fallback names the incident and links to Airflow", () => {
    const msg = fallbackMessage({ ref: "INC-7F3A2K", dagId: "d", taskId: "t", runId: "r", logUrl: "http://af/log" }, "the report could not be delivered to LINE") as any;
    expect(msg.text).toContain("INC-7F3A2K");
    expect(msg.text).toContain("http://af/log");
  });
});

describe("verifyLineSignature", () => {
  it("accepts LINE's base64 HMAC of the raw body only", () => {
    const body = Buffer.from('{"events":[]}');
    const sig = createHmac("sha256", "chan-secret").update(body).digest("base64");
    expect(verifyLineSignature("chan-secret", body, sig)).toBe(true);
    expect(verifyLineSignature("other", body, sig)).toBe(false);
    expect(verifyLineSignature("chan-secret", Buffer.from('{"events":[1]}'), sig)).toBe(false);
    expect(verifyLineSignature("chan-secret", body, undefined)).toBe(false);
  });
});

describe("quantile", () => {
  it("averages the middle two values for an even count", () => {
    expect(quantile([65_000, 75_000], 0.5)).toBe(70_000);
    expect(quantile([10, 20, 30], 0.5)).toBe(20);
    expect(quantile([65_000, 75_000], 0.9)).toBe(74_000);
    expect(quantile([42], 0.9)).toBe(42);
    expect(quantile([], 0.5)).toBeUndefined();
  });
});

describe("FeedbackStore", () => {
  function seeded() {
    const store = new FeedbackStore(":memory:");
    store.recordDispatch({ ref: "INC-AAAAAA", incidentKey: "d:r", dagId: "d", taskId: "t", runId: "r", firstSeenAt: 1_000, dispatchedAt: 2_000, model: "moonshot/kimi-k3" });
    return store;
  }

  it("records completion and the captured report", () => {
    const store = seeded();
    store.recordCompletion("INC-AAAAAA", { status: "ok", delivered: true, replyDisposition: "visible" }, 70_000);
    expect(store.recordReport("INC-AAAAAA", { text: "🚨 report\nRef: INC-AAAAAA", sentAt: 69_000, messageId: "m1", promptVersion: "abc123" })).toBe(true);
    expect(store.recordReport("INC-ZZZZZZ", { text: "x", sentAt: 1 })).toBe(false);
    expect(store.getIncident("INC-AAAAAA")).toMatchObject({ status: "ok", delivered: 1, report_message_id: "m1", prompt_version: "abc123", first_seen_at: 1_000 });
  });

  it("keeps one vote per person; a later tap replaces the earlier one", () => {
    const store = seeded();
    const voter = voterId("salt", "U123", "evt1");
    store.recordRating("INC-AAAAAA", voter, "down", 1);
    store.recordReason("INC-AAAAAA", voter, "root_cause", 2);
    expect(store.getFeedback("INC-AAAAAA")).toMatchObject([{ rating: "down", reason: "root_cause" }]);
    store.recordRating("INC-AAAAAA", voter, "up", 3);
    expect(store.getFeedback("INC-AAAAAA")).toMatchObject([{ rating: "up", reason: null }]);
    store.recordRating("INC-AAAAAA", voterId("salt", "U999", "evt2"), "misleading", 4);
    expect(store.getFeedback("INC-AAAAAA")).toHaveLength(2);
  });

  it("hashes LINE user ids and claims each webhook event once", () => {
    expect(voterId("salt", "U123", "e")).not.toContain("U123");
    expect(voterId("salt", "U123", "e")).toBe(voterId("salt", "U123", "other"));
    expect(voterId("salt", undefined, "e1")).toBe("anon:e1");
    const store = seeded();
    expect(store.claimEvent("evt-1", 1)).toBe(true);
    expect(store.claimEvent("evt-1", 2)).toBe(false);
  });

  it("feedback report summarizes votes by prompt version and lists negative feedback", () => {
    const store = seeded();
    store.recordCompletion("INC-AAAAAA", { status: "ok", delivered: true }, 70_000);
    store.recordReport("INC-AAAAAA", { text: "🚨 Incident Summary: d / t\nRef: INC-AAAAAA", sentAt: 61_000, promptVersion: "v1hash" });
    store.recordRating("INC-AAAAAA", "u:1", "up", 1);
    store.recordRating("INC-AAAAAA", "u:2", "misleading", 2);
    store.recordReason("INC-AAAAAA", "u:2", "made_up", 3);
    const text = renderFeedbackReport(store.incidentsSince(0), store.feedbackSince(0), { days: 30 });
    expect(text).toContain("v1hash: 1 incidents, 👍 1 / 👎 0 / ⚠️ 1 (50% helpful)");
    expect(text).toContain("Made-up fact: 1");
    expect(text).toContain("--- INC-AAAAAA ⚠️ Misleading - Made-up fact");
    expect(text).toContain("🚨 Incident Summary: d / t");
    expect(text).toContain("(median / p90): 60s / 60s");
  });
});
