import { defineToolPlugin } from "openclaw/plugin-sdk/tool-plugin";
import { configSchema, resolveConfig } from "./config.js";
import { AIRFLOW_LOG_DESCRIPTION, airflowLogParams, getAirflowTaskLog } from "./tools/airflowLog.js";
import { DOWNSTREAM_IMPACT_DESCRIPTION, downstreamImpactParams, getDownstreamImpact } from "./tools/downstreamImpact.js";
import { GIT_DIFF_DESCRIPTION, getGitFileDiff, gitDiffParams } from "./tools/gitDiff.js";

/**
 * Read-only incident investigation tools (spec §2). All tools are optional so
 * they are only exposed to agents that explicitly allowlist them
 * (the `pipeline-investigator` agent).
 */
export default defineToolPlugin({
  id: "pipeline-investigator",
  name: "Pipeline Incident Investigator",
  description: "Read-only tools for triaging failed data pipelines: Airflow task logs, git diffs, and dbt lineage.",
  configSchema,
  tools: (tool) => [
    tool({
      name: "get_airflow_task_log",
      label: "Airflow task log",
      description: AIRFLOW_LOG_DESCRIPTION,
      parameters: airflowLogParams,
      optional: true,
      execute: (params, config, context) => {
        context.signal?.throwIfAborted();
        return getAirflowTaskLog(params, resolveConfig(config));
      },
    }),
    tool({
      name: "get_git_file_diff",
      label: "Git file diff",
      description: GIT_DIFF_DESCRIPTION,
      parameters: gitDiffParams,
      optional: true,
      execute: (params, config, context) => {
        context.signal?.throwIfAborted();
        return getGitFileDiff(params, resolveConfig(config), { signal: context.signal });
      },
    }),
    tool({
      name: "get_downstream_impact",
      label: "Downstream impact",
      description: DOWNSTREAM_IMPACT_DESCRIPTION,
      parameters: downstreamImpactParams,
      optional: true,
      execute: (params, config, context) => {
        context.signal?.throwIfAborted();
        return getDownstreamImpact(params, resolveConfig(config));
      },
    }),
  ],
});
