import { Type, type Static } from "typebox";
import { envInt, envOptional, envString, type Env } from "@openclaw-line/shared";

/**
 * Plugin config (`plugins.entries.pipeline-investigator.config`).
 * Every field is optional and falls back to an environment variable so that
 * secrets can stay out of the config file.
 */
export const configSchema = Type.Object(
  {
    airflowBaseUrl: Type.Optional(Type.String({ description: "Airflow webserver/API base URL. Env: AIRFLOW_BASE_URL." })),
    airflowApiVersion: Type.Optional(Type.Union([Type.Literal("v1"), Type.Literal("v2")], { description: "v2 for Airflow 3, v1 for Airflow 2. Env: AIRFLOW_API_VERSION." })),
    gitProvider: Type.Optional(Type.Union([Type.Literal("github"), Type.Literal("gitlab"), Type.Literal("local")], { description: "Env: GIT_PROVIDER." })),
    githubRepo: Type.Optional(Type.String({ description: "owner/repo. Env: GITHUB_REPO." })),
    githubApiUrl: Type.Optional(Type.String({ description: "Env: GITHUB_API_URL." })),
    gitlabProject: Type.Optional(Type.String({ description: "Project id or url-encoded path. Env: GITLAB_PROJECT." })),
    gitlabApiUrl: Type.Optional(Type.String({ description: "Env: GITLAB_API_URL." })),
    gitRef: Type.Optional(Type.String({ description: "Branch to inspect. Env: GIT_REF." })),
    localRepoPath: Type.Optional(Type.String({ description: "Read-only local clone for gitProvider=local. Env: LOCAL_REPO_PATH." })),
    dbtManifestPath: Type.Optional(Type.String({ description: "Path to compiled dbt manifest.json. Env: DBT_MANIFEST_PATH." })),
    lineageMapPath: Type.Optional(Type.String({ description: "Optional Airflow task -> dbt node map. Env: LINEAGE_MAP_PATH." })),
    maxLogChars: Type.Optional(Type.Integer({ minimum: 1000, description: "Env: MAX_LOG_CHARS." })),
  },
  { additionalProperties: false },
);

export type PluginConfig = Static<typeof configSchema>;

export type ResolvedConfig = {
  airflow: { baseUrl: string; apiVersion: "v1" | "v2"; username: string; password: string } | undefined;
  git:
    | { provider: "github"; repo: string; apiUrl: string; token: string | undefined; ref: string | undefined }
    | { provider: "gitlab"; project: string; apiUrl: string; token: string | undefined; ref: string | undefined }
    | { provider: "local"; repoPath: string; ref: string | undefined }
    | undefined;
  dbtManifestPath: string | undefined;
  lineageMapPath: string | undefined;
  maxLogChars: number;
};

/** Credentials are only ever read from the environment. */
export function resolveConfig(cfg: PluginConfig = {}, env: Env = process.env): ResolvedConfig {
  const airflowBaseUrl = cfg.airflowBaseUrl ?? envOptional(env, "AIRFLOW_BASE_URL");
  const apiVersion = (cfg.airflowApiVersion ?? envOptional(env, "AIRFLOW_API_VERSION") ?? "v2") as "v1" | "v2";
  const airflow = airflowBaseUrl
    ? {
        baseUrl: airflowBaseUrl,
        apiVersion,
        username: envString(env, "AIRFLOW_USERNAME", ""),
        password: envString(env, "AIRFLOW_PASSWORD", ""),
      }
    : undefined;

  const provider = cfg.gitProvider ?? envOptional(env, "GIT_PROVIDER");
  const ref = cfg.gitRef ?? envOptional(env, "GIT_REF");
  let git: ResolvedConfig["git"];
  if (provider === "github") {
    const repo = cfg.githubRepo ?? envOptional(env, "GITHUB_REPO");
    if (repo) {
      git = {
        provider,
        repo,
        apiUrl: cfg.githubApiUrl ?? envOptional(env, "GITHUB_API_URL") ?? "https://api.github.com",
        token: envOptional(env, "GITHUB_TOKEN"),
        ref,
      };
    }
  } else if (provider === "gitlab") {
    const project = cfg.gitlabProject ?? envOptional(env, "GITLAB_PROJECT");
    if (project) {
      git = {
        provider,
        project,
        apiUrl: cfg.gitlabApiUrl ?? envOptional(env, "GITLAB_API_URL") ?? "https://gitlab.com/api/v4",
        token: envOptional(env, "GITLAB_TOKEN"),
        ref,
      };
    }
  } else if (provider === "local") {
    const repoPath = cfg.localRepoPath ?? envOptional(env, "LOCAL_REPO_PATH");
    if (repoPath) git = { provider, repoPath, ref };
  }

  return {
    airflow,
    git,
    dbtManifestPath: cfg.dbtManifestPath ?? envOptional(env, "DBT_MANIFEST_PATH"),
    lineageMapPath: cfg.lineageMapPath ?? envOptional(env, "LINEAGE_MAP_PATH"),
    maxLogChars: cfg.maxLogChars ?? envInt(env, "MAX_LOG_CHARS", 24_000),
  };
}
