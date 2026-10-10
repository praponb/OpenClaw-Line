import type { CommitQuery, FileCommit, GitProvider } from "./types.js";

type GithubCommitListItem = {
  sha: string;
  commit: { author?: { name?: string; date?: string }; message: string };
  author?: { login?: string } | null;
};

type GithubCommitDetail = {
  files?: { filename: string; previous_filename?: string; patch?: string; status?: string }[];
};

/** Read-only GitHub REST provider. Use a fine-grained token with `contents:read`. */
export class GithubProvider implements GitProvider {
  readonly name = "github";

  constructor(
    private readonly opts: { apiUrl: string; repo: string; token?: string; fetch?: typeof fetch },
  ) {}

  async recentFileCommits(q: CommitQuery): Promise<FileCommit[]> {
    const params = new URLSearchParams({ path: q.filePath, since: q.sinceIso, per_page: String(q.limit) });
    if (q.ref) params.set("sha", q.ref);
    const list = await this.get<GithubCommitListItem[]>(`/repos/${this.opts.repo}/commits?${params}`, q.signal);
    const commits: FileCommit[] = [];
    for (const item of list.slice(0, q.limit)) {
      const detail = await this.get<GithubCommitDetail>(`/repos/${this.opts.repo}/commits/${item.sha}`, q.signal);
      const file = detail.files?.find((f) => f.filename === q.filePath || f.previous_filename === q.filePath);
      commits.push({
        sha: item.sha,
        author: item.author?.login ? `@${item.author.login}` : (item.commit.author?.name ?? "unknown"),
        date: item.commit.author?.date ?? "",
        message: item.commit.message,
        patch: file?.patch ?? (file ? `(${file.status ?? "changed"}; diff too large or binary)` : "(file not in commit diff)"),
      });
    }
    return commits;
  }

  private async get<T>(path: string, signal?: AbortSignal): Promise<T> {
    const headers: Record<string, string> = {
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "openclaw-pipeline-investigator",
    };
    if (this.opts.token) headers.Authorization = `Bearer ${this.opts.token}`;
    const res = await (this.opts.fetch ?? fetch)(`${this.opts.apiUrl.replace(/\/+$/, "")}${path}`, {
      method: "GET",
      headers,
      signal: signal ?? AbortSignal.timeout(15_000),
    });
    if (!res.ok) throw new Error(`GitHub API ${res.status} for ${path.split("?")[0]}`);
    return (await res.json()) as T;
  }
}
