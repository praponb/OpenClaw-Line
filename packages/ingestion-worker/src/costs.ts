import { sanitize } from "@openclaw-line/shared";
import type { LineClient } from "./line.js";
import type { MonthCompute, PipelineStatus } from "./pipelineStatus.js";

/**
 * Month-to-date costs that live outside the incident log: LINE's own count of
 * messages against the plan quota, and the Databricks job's runs and compute
 * time. Model token cost per incident comes from the incident log
 * (/api/dashboard). Cached so dashboard refreshes don't hit the APIs.
 */

export type CostsData = {
  generatedAt: number;
  /** Start of the current month in Bangkok time. */
  monthStart: number;
  line: { limit: number | null; used: number } | { error: string } | null;
  databricks: MonthCompute | { error: string } | null;
};

const CACHE_MS = 5 * 60_000;
const BANGKOK_OFFSET_MS = 7 * 3600_000;

export function bangkokMonthStart(now: number): number {
  const d = new Date(now + BANGKOK_OFFSET_MS);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) - BANGKOK_OFFSET_MS;
}

export class CostTracker {
  private cached: { at: number; data: CostsData } | undefined;

  constructor(
    private readonly deps: { line?: Pick<LineClient, "quota">; pipeline?: Pick<PipelineStatus, "month">; now: () => number },
  ) {}

  async get(): Promise<CostsData> {
    const now = this.deps.now();
    if (this.cached && now - this.cached.at < CACHE_MS) return this.cached.data;
    const monthStart = bangkokMonthStart(now);
    const settle = async <T>(p: Promise<T> | undefined): Promise<T | { error: string } | null> => {
      if (!p) return null;
      try {
        return await p;
      } catch (err) {
        return { error: sanitize((err as Error).message).slice(0, 200) };
      }
    };
    const [line, databricks] = await Promise.all([settle(this.deps.line?.quota()), settle(this.deps.pipeline?.month(monthStart))]);
    const data: CostsData = { generatedAt: now, monthStart, line, databricks };
    this.cached = { at: now, data };
    return data;
  }
}
