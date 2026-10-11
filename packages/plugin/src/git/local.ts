import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { CommitQuery, FileCommit, FileQuery, GitProvider } from "./types.js";
import { assertSafeRef } from "./types.js";

const execFileAsync = promisify(execFile);
const RECORD = "\x1e";
const FIELD = "\x1f";

/**
 * Reads history from a local clone (mounted read-only in the demo).
 * Only `git log` and `git cat-file -e` are executed, via execFile (no shell).
 */
export class LocalGitProvider implements GitProvider {
  readonly name = "local";

  constructor(private readonly opts: { repoPath: string }) {}

  async recentFileCommits(q: CommitQuery): Promise<FileCommit[]> {
    const args = [
      "-c",
      `safe.directory=${this.opts.repoPath}`,
      "-C",
      this.opts.repoPath,
      "log",
      "--no-color",
      "--no-ext-diff",
      `--since=${q.sinceIso}`,
      `--max-count=${q.limit}`,
      `--format=${RECORD}%H${FIELD}%an${FIELD}%aI${FIELD}%B${FIELD}`,
      "-p",
    ];
    if (q.ref) args.push(assertSafeRef(q.ref));
    args.push("--", q.filePath);
    const { stdout } = await execFileAsync("git", args, {
      maxBuffer: 16 * 1024 * 1024,
      timeout: 15_000,
      signal: q.signal,
      env: { PATH: process.env.PATH ?? "/usr/bin:/bin", GIT_CONFIG_NOSYSTEM: "1", GIT_TERMINAL_PROMPT: "0" },
    });
    return parseGitLog(stdout);
  }

  async fileExists(q: FileQuery): Promise<boolean> {
    const ref = q.ref ? assertSafeRef(q.ref) : "HEAD";
    try {
      await execFileAsync("git", ["-c", `safe.directory=${this.opts.repoPath}`, "-C", this.opts.repoPath, "cat-file", "-e", `${ref}:${q.filePath}`], {
        timeout: 15_000,
        signal: q.signal,
        env: { PATH: process.env.PATH ?? "/usr/bin:/bin", GIT_CONFIG_NOSYSTEM: "1", GIT_TERMINAL_PROMPT: "0" },
      });
      return true;
    } catch (err) {
      // Exit code 128 = not found at that ref; anything else (git missing, timeout) is a real error.
      if ((err as { code?: unknown }).code === 128 || (err as { code?: unknown }).code === 1) return false;
      throw err;
    }
  }
}

export function parseGitLog(stdout: string): FileCommit[] {
  return stdout
    .split(RECORD)
    .filter((r) => r.trim())
    .map((record) => {
      const [sha = "", author = "", date = "", message = "", patch = ""] = record.split(FIELD);
      return { sha, author, date, message: message.trim(), patch: patch.replace(/^\n+/, "").trimEnd() };
    });
}
