import { afterEach, describe, expect, it, vi } from "vitest";
import { EvidenceStore } from "../src/evidence.js";
import { extractClaims, ungroundedClaims, withGroundingWarning } from "../src/grounding.js";
import plugin from "../src/index.js";

const INCIDENT = [
  "INCIDENT",
  "incident_ref: INC-7F3A2K",
  "dag_id: daily_revenue_pipeline",
  "airflow_log_url: https://airflow.praponb.com/dags/daily_revenue_pipeline/runs/x",
  '  File "/opt/airflow/dags/stripe_ingest/extract_stripe.py", line 36, in to_staging_row',
  "KeyError: 'payment_intent_id'",
].join("\n");
const TOOLS = [
  "commit 29cb822f4e1d0a7b9c3e5f6a7b8c9d0e1f2a3b4c by praponb: use direct key access",
  "Tables NOT refreshed by this run: lakehouse.silver.transactions; lakehouse.gold.daily_revenue",
  "source: /Workspace/Users/x/.bundle/openclaw_lakehouse/dev/files/src/03_silver_transactions",
  "downstream: analytics.fct_daily_revenue (CRITICAL)",
].join("\n");

const REPORT = (extra: string) =>
  [
    "🚨 Incident Summary: daily_revenue_pipeline / extract_stripe_transactions (ran 4s)",
    "📌 Root Cause (Facts):",
    "- KeyError: 'payment_intent_id' in dags/stripe_ingest/extract_stripe.py line 36",
    "- Commit 29cb822 by praponb switched to direct key access",
    "- Notebook databricks/src/03_silver_transactions.py, see https://example.cloud.databricks.com/jobs/1/runs/2",
    "⚠️ Blast Radius:",
    "- Impacted Downstream Tables/Models: analytics.fct_daily_revenue (CRITICAL, SLA 06:00 UTC); lakehouse.silver.transactions",
    extra,
    "",
    "Ref: INC-7F3A2K",
  ].join("\n");

describe("grounding check", () => {
  it("extracts SHAs, paths and tables but not URLs, numbers or file names", () => {
    const c = extractClaims(REPORT("- run 954973835355130, e.g. retry; see extract_stripe.py and api.line.me"));
    expect(c.shas).toEqual(["29cb822"]);
    expect(c.paths).toEqual(["dags/stripe_ingest/extract_stripe.py", "databricks/src/03_silver_transactions.py"]);
    expect(c.tables).toEqual(["analytics.fct_daily_revenue", "lakehouse.silver.transactions"]);
  });

  it("accepts repo-relative paths, short SHAs and any case; flags invented items", () => {
    const evidence = `${INCIDENT}\n${TOOLS}`;
    expect(ungroundedClaims(REPORT(""), evidence)).toEqual([]);
    expect(ungroundedClaims(REPORT("- Hypothesis: commit a1b2c3d touched dags/stripe_ingest/schema.py and ANALYTICS.FCT_DAILY_REVENUE feeds analytics.finance_kpis"), evidence)).toEqual([
      "a1b2c3d",
      "dags/stripe_ingest/schema.py",
      "analytics.finance_kpis",
    ]);
  });

  it("puts the warning just above the Ref line", () => {
    const out = withGroundingWarning(REPORT(""), ["a1b2c3d", "analytics.finance_kpis"]);
    const lines = out.split("\n");
    expect(lines.at(-1)).toBe("Ref: INC-7F3A2K");
    expect(lines.at(-3)).toBe("⚠️ Not found in the evidence (verify before acting): a1b2c3d; analytics.finance_kpis");
    expect(withGroundingWarning("text", [])).toBe("text");
  });
});

describe("EvidenceStore", () => {
  it("links a session to its incident ref and expires old sessions", () => {
    let t = 0;
    const store = new EvidenceStore(() => t);
    store.addPrompt("s1", INCIDENT);
    store.addToolResult("s1", { content: [{ type: "text", text: TOOLS }], details: { ok: true } });
    expect(store.forRef("INC-7F3A2K")).toContain("29cb822f");
    expect(store.forRef("INC-AAAAAA")).toBeUndefined();
    t += 3 * 3600 * 1000;
    expect(store.forRef("INC-7F3A2K")).toBeUndefined();
  });
});

describe("report-capture plugin hooks", () => {
  afterEach(() => vi.unstubAllGlobals());

  function register() {
    const handlers = new Map<string, (event: unknown, ctx: unknown) => unknown>();
    const api = {
      pluginConfig: { workerUrl: "http://worker:8080/v1/openclaw/report", channel: "line" },
      logger: { warn: vi.fn(), info: vi.fn() },
      on: (name: string, fn: (event: unknown, ctx: unknown) => unknown) => handlers.set(name, fn),
    };
    vi.stubEnv("WEBHOOK_SECRET", "whsec");
    const fetchMock = vi.fn(async () => new Response("{}", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    (plugin as unknown as { register: (a: typeof api) => void }).register(api);
    return { handlers, fetchMock, api };
  }

  it("warns in the delivered report and forwards the unmatched claims", async () => {
    const { handlers, fetchMock } = register();
    const agent = { agentId: "pipeline-investigator", sessionKey: "agent:pipeline-investigator:cron:j:run:r" };
    handlers.get("llm_input")!({ prompt: INCIDENT }, agent);
    handlers.get("after_tool_call")!({ toolName: "get_git_file_diff", params: {}, result: { content: [{ type: "text", text: TOOLS }] } }, { ...agent, toolName: "get_git_file_diff" });
    // another agent's tool results are not evidence
    handlers.get("after_tool_call")!({ toolName: "x", params: {}, result: "commit a1b2c3d" }, { agentId: "other", sessionKey: "agent:other:main", toolName: "x" });

    const report = REPORT("- Hypothesis: commit a1b2c3d broke it");
    const sending = handlers.get("message_sending")!({ to: "U1", content: report }, { channelId: "line" }) as { content: string };
    expect(sending.content).toContain("⚠️ Not found in the evidence (verify before acting): a1b2c3d");
    expect(sending.content.trimEnd().endsWith("Ref: INC-7F3A2K")).toBe(true);

    handlers.get("message_sent")!({ to: "U1", content: sending.content, success: true }, { channelId: "line" });
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const body = JSON.parse(String((fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1].body));
    expect(body).toMatchObject({ ref: "INC-7F3A2K", ungrounded: ["a1b2c3d"] });
  });

  it("leaves the report alone when every claim is grounded or no evidence was captured", async () => {
    const { handlers, fetchMock } = register();
    expect(handlers.get("message_sending")!({ to: "U1", content: REPORT("") }, { channelId: "line" })).toBeUndefined(); // no evidence: not checked
    handlers.get("message_sent")!({ to: "U1", content: REPORT(""), success: true }, { channelId: "line" });
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(JSON.parse(String((fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1].body)).ungrounded).toBeUndefined();

    const agent = { agentId: "pipeline-investigator", sessionKey: "s2" };
    handlers.get("llm_input")!({ prompt: INCIDENT }, agent);
    handlers.get("after_tool_call")!({ toolName: "t", params: {}, result: TOOLS }, { ...agent, toolName: "t" });
    expect(handlers.get("message_sending")!({ to: "U1", content: REPORT("") }, { channelId: "line" })).toBeUndefined();
  });
});
