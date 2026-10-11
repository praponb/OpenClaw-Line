/**
 * Minimal read-only Databricks REST client.
 *
 * OAuth machine-to-machine: the only POST is the client-credentials token
 * exchange (`/oidc/v1/token`); every API call is a GET. Use a service
 * principal with CAN_VIEW on the job.
 */

export type DatabricksClientOptions = {
  host: string;
  clientId: string;
  clientSecret: string;
  timeoutMs?: number;
  fetch?: typeof fetch;
  now?: () => number;
};

const tokenCache = new Map<string, { token: string; expiresAt: number }>();

export class DatabricksClient {
  private readonly host: string;
  private readonly fetchFn: typeof fetch;
  private readonly now: () => number;
  private readonly timeoutMs: number;

  constructor(private readonly opts: DatabricksClientOptions) {
    this.host = opts.host.replace(/\/+$/, "");
    this.fetchFn = opts.fetch ?? fetch;
    this.now = opts.now ?? Date.now;
    this.timeoutMs = opts.timeoutMs ?? 20_000;
  }

  async get<T>(path: string): Promise<T> {
    const token = await this.token();
    const res = await this.fetchFn(`${this.host}${path}`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (!res.ok) throw new Error(`Databricks API ${res.status} on ${path.split("?")[0]}: ${(await res.text()).slice(0, 200)}`);
    return (await res.json()) as T;
  }

  private async token(): Promise<string> {
    const key = `${this.host}|${this.opts.clientId}`;
    const cached = tokenCache.get(key);
    if (cached && cached.expiresAt > this.now() + 60_000) return cached.token;
    const res = await this.fetchFn(`${this.host}/oidc/v1/token`, {
      method: "POST",
      headers: {
        Authorization: `Basic ${Buffer.from(`${this.opts.clientId}:${this.opts.clientSecret}`).toString("base64")}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: "grant_type=client_credentials&scope=all-apis",
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (!res.ok) throw new Error(`Databricks OAuth token request failed with HTTP ${res.status}`);
    const body = (await res.json()) as { access_token?: string; expires_in?: number };
    if (!body.access_token) throw new Error("Databricks OAuth response had no access_token");
    tokenCache.set(key, { token: body.access_token, expiresAt: this.now() + (body.expires_in ?? 3600) * 1000 });
    return body.access_token;
  }
}

/** Test hook. */
export function clearDatabricksTokenCache(): void {
  tokenCache.clear();
}
