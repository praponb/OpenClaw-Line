import { buildJsonPluginConfigSchema, definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { createReportForwarder, createUsageForwarder, promptVersionOf } from "./forwarder.js";

/**
 * Phase 3: capture each delivered investigator report so engineer feedback
 * (👍 / 👎 / Misleading) can be reviewed against the exact report text.
 * OpenClaw never returns model output to the hook caller, so this observes
 * `message_sent` and forwards reports carrying a `Ref: INC-…` line. It also
 * forwards the investigator's per-call token usage (`llm_output`) for cost
 * tracking.
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
    const forward = createReportForwarder(
      {
        workerUrl,
        secret,
        channel: typeof pc.channel === "string" ? pc.channel : "line",
        promptVersion: promptVersionOf(typeof pc.promptFile === "string" ? pc.promptFile : undefined),
      },
      { log: api.logger },
    );
    api.on("message_sent", (event, ctx) => {
      void forward(event, ctx);
    });
    const forwardUsage = createUsageForwarder(
      {
        usageUrl: typeof pc.usageUrl === "string" ? pc.usageUrl : workerUrl.replace(/\/report$/, "/usage"),
        secret,
        agentId: typeof pc.agentId === "string" ? pc.agentId : "pipeline-investigator",
      },
      { log: api.logger },
    );
    api.on("llm_output", (event, ctx) => {
      void forwardUsage(event, ctx);
    });
  },
});
