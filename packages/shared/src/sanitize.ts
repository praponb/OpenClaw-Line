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
    // 13-19 digit runs (optionally space/dash separated) that pass Luhn.
    kind: "card_number",
    pattern: /\b(?:\d[ -]?){12,18}\d\b/g,
    replace: (m) => (luhnValid(m.replace(/[ -]/g, "")) ? marker("card_number") : m),
  },
];

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
