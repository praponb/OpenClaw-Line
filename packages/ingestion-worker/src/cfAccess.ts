import { createPublicKey, verify as verifySignature, type JsonWebKey, type KeyObject } from "node:crypto";

/**
 * Verifies Cloudflare Access application tokens (the `Cf-Access-Jwt-Assertion`
 * header Cloudflare adds after login) so the public dashboard listener only
 * answers requests that passed Access, for allow-listed emails.
 * https://developers.cloudflare.com/cloudflare-one/identity/authorization-cookie/validating-json/
 */

export type AccessConfig = {
  /** e.g. "praponb.cloudflareaccess.com" */
  teamDomain: string;
  /** Application Audience (AUD) tag from the Access application. */
  aud: string;
  /** Lower-cased emails allowed in, in addition to Access's own policy. */
  allowedEmails: string[];
};

export type AccessResult = { ok: true; email: string } | { ok: false; reason: string };

const JWKS_TTL_MS = 60 * 60 * 1000;
const CLOCK_SKEW_S = 60;

export class AccessVerifier {
  private keys: { fetchedAt: number; byKid: Map<string, KeyObject> } | undefined;
  private readonly issuer: string;

  constructor(
    private readonly cfg: AccessConfig,
    private readonly deps: { fetch?: typeof fetch; now?: () => number } = {},
  ) {
    this.issuer = `https://${normalizeTeamDomain(cfg.teamDomain)}`;
  }

  async verify(token: string | undefined): Promise<AccessResult> {
    if (!token) return { ok: false, reason: "missing Cloudflare Access token" };
    const parts = token.split(".");
    if (parts.length !== 3) return { ok: false, reason: "malformed token" };
    const [h, p, s] = parts as [string, string, string];
    let header: { alg?: string; kid?: string };
    let claims: { aud?: string | string[]; iss?: string; exp?: number; nbf?: number; email?: string };
    try {
      header = JSON.parse(Buffer.from(h, "base64url").toString("utf8"));
      claims = JSON.parse(Buffer.from(p, "base64url").toString("utf8"));
    } catch {
      return { ok: false, reason: "malformed token" };
    }
    if (header.alg !== "RS256" || !header.kid) return { ok: false, reason: "unsupported token algorithm" };

    const key = await this.key(header.kid);
    if (!key) return { ok: false, reason: "unknown signing key" };
    if (!verifySignature("RSA-SHA256", Buffer.from(`${h}.${p}`), key, Buffer.from(s, "base64url"))) {
      return { ok: false, reason: "bad signature" };
    }

    const nowS = Math.floor((this.deps.now ?? Date.now)() / 1000);
    const auds = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
    if (!auds.includes(this.cfg.aud)) return { ok: false, reason: "wrong audience" };
    if (claims.iss !== this.issuer) return { ok: false, reason: "wrong issuer" };
    if (typeof claims.exp !== "number" || claims.exp + CLOCK_SKEW_S < nowS) return { ok: false, reason: "token expired" };
    if (typeof claims.nbf === "number" && claims.nbf - CLOCK_SKEW_S > nowS) return { ok: false, reason: "token not yet valid" };
    const email = typeof claims.email === "string" ? claims.email.toLowerCase() : "";
    if (!email || !this.cfg.allowedEmails.includes(email)) return { ok: false, reason: `email not allowed: ${email || "none"}` };
    return { ok: true, email };
  }

  private async key(kid: string): Promise<KeyObject | undefined> {
    const now = (this.deps.now ?? Date.now)();
    const fresh = this.keys && now - this.keys.fetchedAt < JWKS_TTL_MS;
    if (fresh && this.keys!.byKid.has(kid)) return this.keys!.byKid.get(kid);
    // Unknown kid or stale cache: refetch (Cloudflare rotates keys).
    try {
      const res = await (this.deps.fetch ?? fetch)(`${this.issuer}/cdn-cgi/access/certs`, { signal: AbortSignal.timeout(10_000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = (await res.json()) as { keys?: (JsonWebKey & { kid?: string })[] };
      const byKid = new Map<string, KeyObject>();
      for (const jwk of body.keys ?? []) if (jwk.kid && jwk.kty === "RSA") byKid.set(jwk.kid, createPublicKey({ key: jwk, format: "jwk" }));
      this.keys = { fetchedAt: now, byKid };
    } catch {
      // Keep any previous keys; verification fails closed below if the kid is unknown.
    }
    return this.keys?.byKid.get(kid);
  }
}

export function normalizeTeamDomain(d: string): string {
  return d.trim().replace(/^https?:\/\//, "").replace(/\/+$/, "");
}

/** Token from the header Cloudflare adds, or the CF_Authorization cookie as a fallback. */
export function accessToken(headers: Record<string, string | string[] | undefined>): string | undefined {
  const h = headers["cf-access-jwt-assertion"];
  if (typeof h === "string" && h) return h;
  const cookie = typeof headers.cookie === "string" ? headers.cookie : "";
  const m = /(?:^|;\s*)CF_Authorization=([^;]+)/.exec(cookie);
  return m?.[1];
}
