import { describe, expect, it } from "vitest";
import { filterLog } from "../src/logFilter.js";

const noise = (n: number, prefix = "INFO - step") => Array.from({ length: n }, (_, i) => `${prefix} ${i}`);

describe("filterLog", () => {
  it("returns small logs untouched", () => {
    const raw = noise(10).join("\n");
    const r = filterLog(raw);
    expect(r.text).toBe(raw);
    expect(r.truncated).toBe(false);
  });

  it("keeps the tail plus earlier error lines with context", () => {
    const lines = [...noise(500), "ERROR - early failure in partition 7", ...noise(1000, "INFO - later")];
    const r = filterLog(lines.join("\n"), { tailLines: 300 });
    expect(r.text).toContain("ERROR - early failure in partition 7");
    expect(r.text).toContain("INFO - step 499"); // context line
    expect(r.text).toContain("INFO - later 999");
    expect(r.text).not.toContain("INFO - later 100\n");
    expect(r.text).toContain("... [lines omitted] ...");
    expect(r.truncated).toBe(true);
  });

  it("keeps a complete traceback outside the tail", () => {
    const tb = [
      "Traceback (most recent call last):",
      '  File "/opt/airflow/dags/stripe_ingest/extract_stripe.py", line 36, in to_staging_row',
      '    "payment_intent_id": payload["payment_intent_id"],',
      "KeyError: 'payment_intent_id'",
    ];
    const r = filterLog([...noise(50), ...tb, ...noise(400, "INFO - cleanup")].join("\n"), { tailLines: 100 });
    for (const line of tb) expect(r.text).toContain(line);
  });

  it("enforces the character cap, keeping the end", () => {
    const raw = Array.from({ length: 2000 }, (_, i) => `ERROR - line ${i} ${"x".repeat(80)}`).join("\n");
    const r = filterLog(raw, { maxChars: 5000 });
    expect(r.text.length).toBeLessThan(5100);
    expect(r.text).toContain("ERROR - line 1999");
    expect(r.truncated).toBe(true);
  });

  it("collapses repeated identical lines", () => {
    const r = filterLog(["retrying", "retrying", "retrying", "done"].join("\n"));
    expect(r.text).toBe("retrying\ndone");
  });
});
