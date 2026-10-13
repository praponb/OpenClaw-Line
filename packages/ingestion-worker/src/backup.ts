import { existsSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { sanitize } from "@openclaw-line/shared";
import type { FeedbackStore } from "./feedbackStore.js";

const BANGKOK_OFFSET_MS = 7 * 3600_000;
const bangkokDay = (ms: number) => new Date(ms + BANGKOK_OFFSET_MS).toISOString().slice(0, 10);
const FILE_RE = /^feedback-(\d{4}-\d{2}-\d{2})\.db$/;

/**
 * One `VACUUM INTO` copy of the feedback log per Bangkok day
 * (`<dir>/feedback-YYYY-MM-DD.db`), keeping the newest `keep` files.
 * `runIfDue()` is cheap when today's copy exists, so it can run hourly and after
 * every restart without making extra copies.
 */
export class DailyBackup {
  /** Set when the last attempt failed; cleared by the next success. Reported by the heartbeat. */
  lastError: string | undefined;

  constructor(
    private readonly store: Pick<FeedbackStore, "backupTo">,
    private readonly opts: { dir: string; keep: number; now?: () => number; log?: Pick<Console, "info" | "error"> },
  ) {}

  runIfDue(): "skipped" | "written" | "failed" {
    const file = join(this.opts.dir, `feedback-${bangkokDay(this.now())}.db`);
    if (existsSync(file)) return "skipped";
    try {
      this.store.backupTo(file);
      const removed = this.prune();
      this.lastError = undefined;
      this.log.info(`[backup] wrote ${file}${removed ? `, removed ${removed} old backup(s)` : ""}`);
      return "written";
    } catch (err) {
      this.lastError = sanitize((err as Error).message);
      this.log.error(`[backup] failed: ${this.lastError}`);
      return "failed";
    }
  }

  /** Delete all but the newest `keep` daily files (by the date in the name). */
  private prune(): number {
    const files = readdirSync(this.opts.dir)
      .filter((f) => FILE_RE.test(f))
      .sort()
      .reverse();
    const old = files.slice(this.opts.keep);
    for (const f of old) rmSync(join(this.opts.dir, f), { force: true });
    return old.length;
  }

  private get log() {
    return this.opts.log ?? console;
  }

  private now(): number {
    return this.opts.now?.() ?? Date.now();
  }
}
