/**
 * In-memory evidence per investigator session, for the grounding check: the
 * incident message (from `llm_input`) and every tool result (`after_tool_call`).
 * The incident message carries `incident_ref: INC-…`, which links the session to
 * the report delivered later from a different session. Nothing is persisted or sent.
 */

const INCIDENT_REF_RE = /\bincident_ref:\s*(INC-[0-9A-HJKMNP-TV-Z]{6})\b/;
const MAX_CHARS_PER_SESSION = 2_000_000;
const MAX_SESSIONS = 50;
const TTL_MS = 2 * 3600 * 1000;

type Entry = { parts: string[]; chars: number; at: number; ref?: string };

export class EvidenceStore {
  private readonly sessions = new Map<string, Entry>();
  private readonly byRef = new Map<string, string>();

  constructor(private readonly now: () => number = Date.now) {}

  /** The model's input for a turn; the first one is the incident message. */
  addPrompt(session: string, prompt: string): void {
    const e = this.entry(session);
    const ref = INCIDENT_REF_RE.exec(prompt)?.[1];
    if (ref && !e.ref) {
      e.ref = ref;
      this.byRef.set(ref, session);
    }
    this.append(e, prompt);
  }

  addToolResult(session: string, result: unknown): void {
    this.append(this.entry(session), textOf(result));
  }

  /** All evidence for the session that investigated `ref`, or undefined when none was captured. */
  forRef(ref: string): string | undefined {
    this.expire();
    const session = this.byRef.get(ref);
    const e = session ? this.sessions.get(session) : undefined;
    return e?.parts.join("\n");
  }

  private entry(session: string): Entry {
    this.expire();
    let e = this.sessions.get(session);
    if (!e) {
      e = { parts: [], chars: 0, at: this.now() };
      this.sessions.set(session, e);
    }
    return e;
  }

  private append(e: Entry, text: string): void {
    if (!text || e.chars >= MAX_CHARS_PER_SESSION || e.parts.includes(text)) return;
    const part = text.slice(0, MAX_CHARS_PER_SESSION - e.chars);
    e.parts.push(part);
    e.chars += part.length;
  }

  private expire(): void {
    const cutoff = this.now() - TTL_MS;
    for (const [k, e] of this.sessions) {
      if (e.at >= cutoff && this.sessions.size <= MAX_SESSIONS) continue;
      this.sessions.delete(k);
      if (e.ref) this.byRef.delete(e.ref);
    }
  }
}

/** Every string inside a tool result (OpenClaw wraps text in content blocks). */
function textOf(v: unknown, depth = 0): string {
  if (typeof v === "string") return v;
  if (v === null || typeof v !== "object" || depth > 6) return "";
  const values = Array.isArray(v) ? v : Object.values(v);
  return values
    .map((x) => textOf(x, depth + 1))
    .filter(Boolean)
    .join("\n");
}
