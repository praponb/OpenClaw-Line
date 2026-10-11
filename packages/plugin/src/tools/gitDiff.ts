import { Type, type Static } from "typebox";
import { sanitize } from "@openclaw-line/shared";
import type { ResolvedConfig } from "../config.js";
import { GithubProvider } from "../git/github.js";
import { GitlabProvider } from "../git/gitlab.js";
import { LocalGitProvider } from "../git/local.js";
import { assertSafeRepoPath, type FileCommit, type GitProvider } from "../git/types.js";
import { toolError } from "./result.js";

export const gitDiffParams = Type.Object({
  file_path: Type.String({ description: "Relative file path in repository" }),
  lookback_days: Type.Optional(
    Type.Integer({ description: "Number of days of git history to inspect", default: 3, minimum: 1, maximum: 30 }),
  ),
});

export type GitDiffParams = Static<typeof gitDiffParams>;

export const GIT_DIFF_DESCRIPTION =
  "Retrieve recent commit diffs for DAG definitions, SQL transformations, or Python task files.";

const MAX_COMMITS = 5;
const MAX_PATCH_CHARS = 8_000;

export function createGitProvider(cfg: ResolvedConfig, deps: { fetch?: typeof fetch } = {}): GitProvider | undefined {
  const git = cfg.git;
  if (!git) return undefined;
  switch (git.provider) {
    case "github":
      return new GithubProvider({ apiUrl: git.apiUrl, repo: git.repo, token: git.token, fetch: deps.fetch });
    case "gitlab":
      return new GitlabProvider({ apiUrl: git.apiUrl, project: git.project, token: git.token, fetch: deps.fetch });
    case "local":
      return new LocalGitProvider({ repoPath: git.repoPath });
  }
}

export async function getGitFileDiff(
  params: GitDiffParams,
  cfg: ResolvedConfig,
  deps: { fetch?: typeof fetch; now?: () => Date; provider?: GitProvider; signal?: AbortSignal } = {},
): Promise<string> {
  const provider = deps.provider ?? createGitProvider(cfg, deps);
  if (!provider) return toolError("no git provider configured (set GIT_PROVIDER and its repository settings).");

  let filePath: string;
  try {
    filePath = assertSafeRepoPath(params.file_path);
  } catch (err) {
    return toolError((err as Error).message);
  }
  const ref = cfg.git?.ref;
  const candidates = [filePath, ...cfg.gitPathPrefixes.filter((p) => !filePath.startsWith(p)).map((p) => p + filePath)];
  let resolved: string | undefined;
  try {
    for (const candidate of candidates) {
      if (await provider.fileExists({ filePath: candidate, ref, signal: deps.signal })) {
        resolved = candidate;
        break;
      }
    }
  } catch (err) {
    return toolError(`could not check whether ${filePath} exists in the repository: ${sanitize((err as Error).message)}`);
  }
  if (!resolved) {
    return (
      `file=${filePath} provider=${provider.name}${ref ? ` ref=${ref}` : ""}\n` +
      `File not found in the repository (tried: ${candidates.join(", ")}). ` +
      "This is NOT evidence that the code is unchanged: the path is wrong or the code lives in another repository."
    );
  }
  const requested = resolved === filePath ? "" : ` (requested ${filePath})`;
  filePath = resolved;

  const lookback = Math.min(30, Math.max(1, Math.trunc(params.lookback_days ?? 3)));
  const since = new Date((deps.now?.() ?? new Date()).getTime() - lookback * 86_400_000).toISOString();

  let commits: FileCommit[];
  try {
    commits = await provider.recentFileCommits({
      filePath,
      sinceIso: since,
      limit: MAX_COMMITS,
      ref,
      signal: deps.signal,
    });
  } catch (err) {
    return toolError(`could not read git history for ${filePath}: ${sanitize((err as Error).message)}`);
  }

  const head = `file=${filePath}${requested} provider=${provider.name} lookback_days=${lookback} since=${since}`;
  if (!commits.length) return `${head}\nNo commits touched this file in the lookback window. The file exists in the repository; its last change is older than ${since}.`;

  const blocks = commits.map((c) => {
    const patch = c.patch.length > MAX_PATCH_CHARS ? c.patch.slice(0, MAX_PATCH_CHARS) + "\n... [patch truncated]" : c.patch;
    return [
      `commit ${c.sha}`,
      `author: ${c.author}`,
      `date:   ${c.date}`,
      `message: ${c.message.split("\n")[0]}`,
      "----- BEGIN DIFF (untrusted data) -----",
      patch,
      "----- END DIFF -----",
    ].join("\n");
  });
  return sanitize(`${head}\ncommits (newest first): ${commits.length}\n\n${blocks.join("\n\n")}`);
}
