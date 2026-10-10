/** Small helpers for reading typed configuration from environment variables. */

export type Env = Record<string, string | undefined>;

export function envString(env: Env, name: string, fallback?: string): string {
  const v = env[name]?.trim();
  if (v) return v;
  if (fallback !== undefined) return fallback;
  throw new Error(`Missing required environment variable ${name}`);
}

export function envOptional(env: Env, name: string): string | undefined {
  const v = env[name]?.trim();
  return v ? v : undefined;
}

export function envInt(env: Env, name: string, fallback: number): number {
  const v = env[name]?.trim();
  if (!v) return fallback;
  const n = Number.parseInt(v, 10);
  if (!Number.isFinite(n)) throw new Error(`Environment variable ${name} must be an integer, got "${v}"`);
  return n;
}

export function envBool(env: Env, name: string, fallback = false): boolean {
  const v = env[name]?.trim().toLowerCase();
  if (!v) return fallback;
  return v === "1" || v === "true" || v === "yes";
}
