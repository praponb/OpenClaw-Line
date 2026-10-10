/**
 * Log pre-filter (spec §5 "Log Explosion").
 *
 * Giant Spark/dbt logs can exceed 100k tokens. We keep:
 *   - the trailing `tailLines` lines (default 300), and
 *   - any earlier line matching ERROR / CRITICAL / Traceback / FAILED / Exception,
 *     with a little context, plus complete Python traceback blocks,
 * then enforce a hard character cap so the payload stays small.
 */

export type LogFilterOptions = {
  tailLines?: number;
  contextLines?: number;
  maxChars?: number;
};

export type LogFilterResult = {
  text: string;
  totalLines: number;
  keptLines: number;
  truncated: boolean;
};

export const DEFAULT_TAIL_LINES = 300;
export const DEFAULT_MAX_CHARS = 24_000;
const SIGNAL = /\b(ERROR|CRITICAL|FATAL|FAILED|Traceback|Exception)\b|Error:/;
const TRACEBACK_START = /^\s*.*Traceback \(most recent call last\):/;
// A traceback ends at the first non-indented line after the exception line.
const MAX_TRACEBACK_LINES = 120;

export function filterLog(raw: string, opts: LogFilterOptions = {}): LogFilterResult {
  const tailLines = clampInt(opts.tailLines ?? DEFAULT_TAIL_LINES, 1, 5000);
  const context = clampInt(opts.contextLines ?? 2, 0, 20);
  const maxChars = clampInt(opts.maxChars ?? DEFAULT_MAX_CHARS, 1000, 200_000);

  const lines = raw.replace(/\r\n/g, "\n").split("\n");
  if (lines.length && lines[lines.length - 1] === "") lines.pop();
  const total = lines.length;
  const keep = new Uint8Array(total);

  const tailStart = Math.max(0, total - tailLines);
  for (let i = tailStart; i < total; i++) keep[i] = 1;

  for (let i = 0; i < tailStart; i++) {
    const line = lines[i]!;
    if (TRACEBACK_START.test(line)) {
      const end = tracebackEnd(lines, i);
      for (let j = Math.max(0, i - context); j <= end; j++) keep[j] = 1;
      i = end;
      continue;
    }
    if (SIGNAL.test(line)) {
      for (let j = Math.max(0, i - context); j <= Math.min(total - 1, i + context); j++) keep[j] = 1;
    }
  }

  const out: string[] = [];
  let kept = 0;
  let gap = false;
  let prev: string | undefined;
  for (let i = 0; i < total; i++) {
    if (!keep[i]) {
      gap = true;
      continue;
    }
    if (gap && out.length) out.push("... [lines omitted] ...");
    gap = false;
    const line = lines[i]!;
    // Collapse immediate duplicates (retry spam, repeated warnings).
    if (line === prev) continue;
    out.push(line);
    prev = line;
    kept++;
  }

  let text = out.join("\n");
  let truncated = kept < total;
  if (text.length > maxChars) {
    // Keep the end of the log: the final error is almost always there.
    text = "... [truncated to last " + maxChars + " chars] ...\n" + text.slice(text.length - maxChars);
    truncated = true;
  }
  return { text, totalLines: total, keptLines: kept, truncated };
}

function tracebackEnd(lines: string[], start: number): number {
  const limit = Math.min(lines.length - 1, start + MAX_TRACEBACK_LINES);
  for (let j = start + 1; j <= limit; j++) {
    const line = lines[j]!;
    // Frames are indented; the exception line ("KeyError: ...") is the first
    // non-indented, non-empty line after the header.
    if (line.trim() !== "" && !/^\s/.test(stripLogPrefix(line))) return j;
  }
  return limit;
}

/** Airflow prefixes each line with `[timestamp] {file.py:NN} LEVEL - `. */
function stripLogPrefix(line: string): string {
  return line.replace(/^\[[^\]]+\]\s+\{[^}]+\}\s+[A-Z]+\s+-\s?/, "");
}

function clampInt(n: number, min: number, max: number): number {
  if (!Number.isFinite(n)) return min;
  return Math.min(max, Math.max(min, Math.trunc(n)));
}
