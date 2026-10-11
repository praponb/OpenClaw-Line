/**
 * Credential & PII sanitizer (spec §5 "Credential & PII Leakage").
 *
 * Applied to every piece of external text (logs, diffs, exception strings)
 * before it reaches the model or leaves the ingestion layer. Each match is
 * replaced with a `[REDACTED:<kind>]` marker so the agent can still reason
 * about *where* a secret appeared without ever seeing it.
 */

type Rule = {
  kind: string;
  pattern: RegExp;
  /** Replacement; defaults to the full-match marker. */
  replace?: (match: string, ...groups: string[]) => string;
};

const marker = (kind: string) => `[REDACTED:${kind}]`;

// Order matters: more specific rules run first so that, for example, a JWT
// inside `Authorization: Bearer eyJ...` is reported as a bearer token.
const RULES: Rule[] = [
  {
    kind: "private_key",
    pattern: /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g,
  },
  {
    // scheme://user:password@host  ->  scheme://user:[REDACTED]@host
    kind: "conn_password",
    pattern: /\b([a-z][a-z0-9+.-]*:\/\/)([^\s:/@]+):([^\s@/]+)@/gi,
    replace: (_m, scheme: string, user: string) => `${scheme}${user}:${marker("conn_password")}@`,
  },
  {
    kind: "bearer",
    pattern: /\b(Bearer)\s+[A-Za-z0-9._~+/=-]{8,}/g,
    replace: (_m, word: string) => `${word} ${marker("bearer")}`,
  },
  {
    kind: "jwt",
    pattern: /\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\b/g,
  },
  { kind: "aws_access_key", pattern: /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/g },
  { kind: "stripe_key", pattern: /\b(?:sk|rk|pk)_(?:live|test)_[0-9A-Za-z]{8,}\b/g },
  { kind: "github_token", pattern: /\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b/g },
  { kind: "gitlab_token", pattern: /\bglpat-[A-Za-z0-9_-]{20,}\b/g },
  { kind: "slack_token", pattern: /\bxox[abprs]-[A-Za-z0-9-]{10,}\b/g },
  // Databricks personal access tokens (dapi…) and service principal OAuth secrets (dose…).
  { kind: "databricks_token", pattern: /\b(?:dapi|dose)[0-9a-f]{32}(?:-\d+)?\b/g },
  { kind: "anthropic_key", pattern: /\bsk-ant-[A-Za-z0-9_-]{20,}\b/g },
  // OpenAI-style `sk-...` keys (Moonshot/Kimi, OpenAI, ...).
  { kind: "api_key", pattern: /\bsk-[A-Za-z0-9]{32,}\b/g },
  {
    // key=value / key: value / "key": "value" for well-known secret keys.
    // The lookbehind skips URL paths and identifiers such as `/auth/token: ...`.
    kind: "secret",
    pattern:
      /((?<![/.])(?:["']?)(?:password|passwd|pwd|secret|client_secret|api[_-]?key|apikey|access[_-]?token|auth[_-]?token|token|private[_-]?key)(?:["']?)\s*[:=]\s*)(["']?)([^\s"',;}{]+)\2/gi,
    replace: (_m, prefix: string, quote: string) => `${prefix}${quote}${marker("secret")}${quote}`,
  },
  {
    kind: "email",
    pattern: /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g,
  },
  {
    // Thai national ID: 13 digits, first 1-8, optionally as 1-2345-67890-12-3, with a
    // valid check digit. Runs before card_number (13 digits can also pass Luhn).
    // A checksum-valid 13-digit number (about 1 in 10, e.g. some epoch-ms timestamps)
    // is masked too: privacy wins over a lost timestamp.
    kind: "thai_national_id",
    pattern: /(?<![\d-])[1-8][ -]?\d{4}[ -]?\d{5}[ -]?\d{2}[ -]?\d(?![\d-])/g,
    replace: (m) => (thaiIdValid(m.replace(/[ -]/g, "")) ? marker("thai_national_id") : m),
  },
  {
    // Thai phone numbers: +66 then 8-9 digits; mobile 0[689]x-xxx-xxxx; landline 02-xxx-xxxx / 0xx-xxx-xxx.
    kind: "thai_phone",
    pattern: /(?<![\w+])(?:\+66[ -]?\d(?:[ -]?\d){7,8}|0[689]\d[ -]?\d{3}[ -]?\d{4}|02[ -]?\d{3}[ -]?\d{4}|0[3-7]\d[ -]?\d{3}[ -]?\d{3})(?![\d-])/g,
  },
  {
    // 13-19 digit runs (optionally space/dash separated) that pass Luhn AND match a real
    // card brand's prefix and length. Luhn alone masked about 1 in 10 Databricks run/job
    // ids (15 digits), removing them from the evidence.
    kind: "card_number",
    pattern: /\b(?:\d[ -]?){12,18}\d\b/g,
    replace: (m) => {
      const digits = m.replace(/[ -]/g, "");
      return cardBrandLike(digits) && luhnValid(digits) ? marker("card_number") : m;
    },
  },
];

/** Visa, Mastercard, Amex, Discover, JCB and UnionPay prefixes with their lengths. */
function cardBrandLike(d: string): boolean {
  const n = d.length;
  const p4 = Number(d.slice(0, 4));
  if (d.startsWith("4")) return n === 13 || n === 16 || n === 19; // Visa
  if (/^5[1-5]/.test(d) || (p4 >= 2221 && p4 <= 2720)) return n === 16; // Mastercard
  if (/^3[47]/.test(d)) return n === 15; // Amex
  if (p4 >= 3528 && p4 <= 3589) return n >= 16 && n <= 19; // JCB
  if (/^(6011|65|64[4-9])/.test(d)) return n >= 16 && n <= 19; // Discover
  if (d.startsWith("62")) return n >= 16 && n <= 19; // UnionPay
  return false;
}

/** Check digit = (11 - (sum of the first 12 digits weighted 13..2) mod 11) mod 10. */
function thaiIdValid(digits: string): boolean {
  if (!/^[1-8]\d{12}$/.test(digits)) return false;
  let sum = 0;
  for (let i = 0; i < 12; i++) sum += (digits.charCodeAt(i) - 48) * (13 - i);
  return (11 - (sum % 11)) % 10 === digits.charCodeAt(12) - 48;
}

function luhnValid(digits: string): boolean {
  if (digits.length < 13 || digits.length > 19) return false;
  let sum = 0;
  let double = false;
  for (let i = digits.length - 1; i >= 0; i--) {
    let d = digits.charCodeAt(i) - 48;
    if (double) {
      d *= 2;
      if (d > 9) d -= 9;
    }
    sum += d;
    double = !double;
  }
  return sum % 10 === 0;
}

export function sanitize(text: string): string {
  let out = text;
  for (const rule of RULES) {
    out = out.replace(rule.pattern, (match: string, ...rest: unknown[]) => {
      if (!rule.replace) return marker(rule.kind);
      // String.replace passes (match, ...groups, offset, input[, namedGroups]);
      // the offset is the first numeric argument.
      const groups = rest.slice(0, rest.findIndex((g) => typeof g === "number")) as (string | undefined)[];
      return rule.replace(match, ...groups.map((g) => g ?? ""));
    });
  }
  return out;
}

/** Recursively sanitize every string inside a JSON-compatible value. */
export function sanitizeDeep<T>(value: T): T {
  if (typeof value === "string") return sanitize(value) as T;
  if (Array.isArray(value)) return value.map((v) => sanitizeDeep(v)) as T;
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, sanitizeDeep(v)])) as T;
  }
  return value;
}
