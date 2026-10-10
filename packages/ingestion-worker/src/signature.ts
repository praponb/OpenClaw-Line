import { createHmac, timingSafeEqual } from "node:crypto";

export const SIGNATURE_HEADER = "x-openclaw-signature";
export const TIMESTAMP_HEADER = "x-openclaw-timestamp";
const MAX_SKEW_S = 300;

/** HMAC-SHA256 over `${timestamp}.${rawBody}`, hex encoded, prefixed with `sha256=`. */
export function sign(secret: string, timestamp: string, rawBody: string): string {
  return "sha256=" + createHmac("sha256", secret).update(`${timestamp}.${rawBody}`).digest("hex");
}

export function verifySignature(
  secret: string,
  rawBody: string,
  timestamp: string | undefined,
  signature: string | undefined,
  nowS: number = Math.floor(Date.now() / 1000),
): boolean {
  if (!timestamp || !signature || !/^\d+$/.test(timestamp)) return false;
  if (Math.abs(nowS - Number(timestamp)) > MAX_SKEW_S) return false;
  const expected = Buffer.from(sign(secret, timestamp, rawBody));
  const given = Buffer.from(signature);
  return expected.length === given.length && timingSafeEqual(expected, given);
}
