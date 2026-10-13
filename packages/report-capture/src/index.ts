import { buildJsonPluginConfigSchema, definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { EvidenceStore } from "./evidence.js";
import { REF_RE, createReportForwarder, createUsageForwarder, promptVersionOf } from "./forwarder.js";
import { ungroundedClaims, withGroundingWarning } from "./grounding.js";

/**
 * Phase 3: capture each delivered investigator report so engineer feedback
 * (👍 / 👎 / Misleading) can be reviewed against the exact report text.
 * OpenClaw never returns model output to the hook caller, so this observes
 * `message_sent` and forwards reports carrying a `Ref: INC-…` line. It also
 * forwards the investigator's per-call token usage (`llm_output`) for cost
 * tracking, and checks that the SHAs, paths and table names in each report
 * appear in the investigator's evidence (warning line added before delivery).
 */
export default definePluginEntry({
  id: "report-capture",
  name: "Investigator Report Capture",
  description: "Forwards delivered pipeline-investigator reports to the ingestion worker's feedback log.",
  configSchema: buildJsonPluginConfigSchema({
    type: "object",
    additionalProperties: false,
    properties: {
      workerUrl: { type: "string" },
      usageUrl: { type: "string" },
      agentId: { type: "string" },
      promptFile: { type: "string" },
      channel: { type: "string" },
    },
    required: ["workerUrl"],
  }),
  register(api) {
    const pc = api.pluginConfig ?? {};
    const workerUrl = typeof pc.workerUrl === "string" ? pc.workerUrl : "";
    const secret = process.env.REPORT_CAPTURE_SECRET ?? process.env.WEBHOOK_SECRET ?? "";
    if (!workerUrl || !secret) {
      api.logger.warn("[report-capture] disabled: workerUrl config or WEBHOOK_SECRET env is missing");
      return;
    }
    const agentId = typeof pc.agentId === "string" ? pc.agentId : "pipeline-investigator";
    const channel = typeof pc.channel === "string" ? pc.channel : "line";
    const evidence = new EvidenceStore();
    const grounding = new Map<string, string[]>();
    const sessionOf = (ctx: { sessionKey?: string; runId?: string } | undefined) => ctx?.sessionKey ?? ctx?.runId;

    api.on("llm_input", (event, ctx) => {
      const session = sessionOf(ctx);
      if (session && ctx?.agentId === agentId && typeof event.prompt === "string") evidence.addPrompt(session, event.prompt);
    });
    api.on("after_tool_call", (event, ctx) => {
      const session = sessionOf(ctx);
      if (session && ctx?.agentId === agentId) evidence.addToolResult(session, event.error ? { error: event.error, result: event.result } : event.result);
    });
    // Before delivery: list claims missing from the evidence on a warning line. Never blocks delivery.
    api.on("message_sending", (event, ctx) => {
      if (ctx?.channelId && ctx.channelId !== channel) return;
      const ref = REF_RE.exec(event.content ?? "")?.[1];
      const ev = ref ? evidence.forRef(ref) : undefined;
      if (!ref || ev === undefined) return;
      try {
        const missing = ungroundedClaims(event.content, ev);
        grounding.set(ref, missing);
        if (grounding.size > 200) grounding.delete(grounding.keys().next().value!);
        if (!missing.length) return;
        api.logger.warn(`[report-capture] ${ref}: ${missing.length} claim(s) not found in the evidence: ${missing.join("; ")}`);
        return { content: withGroundingWarning(event.content, missing) };
      } catch (err) {
        api.logger.warn(`[report-capture] ${ref}: grounding check failed: ${(err as Error).message}`);
        return;
      }
    });

    const forward = createReportForwarder(
      {
        workerUrl,
        secret,
        channel,
        promptVersion: promptVersionOf(typeof pc.promptFile === "string" ? pc.promptFile : undefined),
      },
      { log: api.logger, ungrounded: (ref) => grounding.get(ref) },
    );
    api.on("message_sent", (event, ctx) => {
      void forward(event, ctx);
    });
    const forwardUsage = createUsageForwarder(
      {
        usageUrl: typeof pc.usageUrl === "string" ? pc.usageUrl : workerUrl.replace(/\/report$/, "/usage"),
        secret,
        agentId,
      },
      { log: api.logger },
    );
    api.on("llm_output", (event, ctx) => {
      void forwardUsage(event, ctx);
    });
  },
});
