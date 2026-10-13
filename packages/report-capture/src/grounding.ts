/**
 * Evidence grounding check (AGENTS.md rule 2): every commit SHA, file path and
 * table name in a report must appear in the incident message or a tool result.
 * Items that don't are listed on a warning line added to the report before delivery.
 */

const FILE_EXT = "py|sql|ya?ml|json|sh|scala|ipynb|toml|cfg|ini|txt|md";
const URL_RE = /\bhttps?:\/\/[^\s<>"')]+/g;
const PATH_RE = new RegExp(String.raw`(?:[\w.-]+\/)+[\w.-]+\.(?:${FILE_EXT})\b`, "g");
const SHA_RE = /\b[0-9a-f]{7,40}\b/g;
// schema.table or catalog.schema.table; identifiers start with a letter or underscore.
const TABLE_RE = /\b[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*){1,2}\b/g;
const NOT_TABLE_SUFFIX = new RegExp(String.raw`\.(?:${FILE_EXT}|com|net|org|io|me|ai|dev|app|co|th)$`, "i");
const MAX_ITEMS = 8;

export type Claims = { shas: string[]; paths: string[]; tables: string[] };

/**
 * SHAs, file paths and dotted table names mentioned in a report (URLs are ignored).
 * Table names are read from the Blast Radius section only: elsewhere dotted names are
 * mostly code (`subprocess.run`, `payload.get`) taken from tracebacks.
 */
export function extractClaims(report: string): Claims {
  const strip = (s: string) => s.replace(URL_RE, " ").replace(PATH_RE, " ");
  const text = report.replace(URL_RE, " ");
  const paths = unique(text.match(PATH_RE) ?? []);
  // A SHA needs a digit and a letter, so plain numbers (run ids) and words (e.g. "facade") don't count.
  const shas = unique((strip(report).match(SHA_RE) ?? []).filter((s) => /\d/.test(s) && /[a-f]/.test(s)));
  const blast = strip(blastRadius(report));
  const tables = unique((blast.match(TABLE_RE) ?? []).filter((t) => !NOT_TABLE_SUFFIX.test(t) && !/^(?:e\.g|i\.e)$/i.test(t)));
  return { shas, paths, tables };
}

/** The "⚠️ Blast Radius" section, up to the next emoji header (whole report if there is none). */
function blastRadius(report: string): string {
  const start = report.indexOf("⚠️ Blast Radius");
  if (start < 0) return report;
  const end = report.indexOf("🛠️", start);
  return report.slice(start, end < 0 ? undefined : end);
}

/** Claims that appear nowhere in the evidence (case-insensitive), at most 8. */
export function ungroundedClaims(report: string, evidence: string): string[] {
  const ev = evidence.toLowerCase();
  const { shas, paths, tables } = extractClaims(report);
  const missing: string[] = [];
  for (const sha of shas) if (!ev.includes(sha.toLowerCase())) missing.push(sha);
  for (const p of paths) if (!pathInEvidence(p, ev)) missing.push(p);
  for (const t of tables) if (!ev.includes(t.toLowerCase())) missing.push(t);
  return missing.slice(0, MAX_ITEMS);
}

/**
 * Reports use repository-relative paths ("dags/stripe_ingest/extract_stripe.py",
 * "databricks/src/03_silver_transactions.py") while evidence has absolute or
 * bundle paths ("/opt/airflow/dags/...", ".../files/src/03_silver_transactions").
 * A path counts when the evidence has it, or its last two segments without the extension.
 */
function pathInEvidence(path: string, ev: string): boolean {
  const p = path.toLowerCase();
  if (ev.includes(p)) return true;
  const tail = p.split("/").slice(-2).join("/").replace(/\.[^./]+$/, "");
  return tail.includes("/") && ev.includes(tail);
}

/** Add the warning line just above the final "Ref:" line (the ref must stay last). */
export function withGroundingWarning(report: string, missing: string[]): string {
  if (!missing.length) return report;
  const warning = `⚠️ Not found in the evidence (verify before acting): ${missing.join("; ")}`;
  const lines = report.replace(/\s+$/, "").split("\n");
  const refAt = lines.findLastIndex((l) => /^\s*Ref:\s*INC-/.test(l));
  if (refAt < 0) return `${report.replace(/\s+$/, "")}\n\n${warning}`;
  lines.splice(refAt, 0, warning, "");
  return lines.join("\n");
}

function unique(items: string[]): string[] {
  return [...new Set(items)];
}
