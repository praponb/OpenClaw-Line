import type { CommitQuery, FileCommit, FileQuery, GitProvider } from "./types.js";

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

  async fileExists(q: FileQuery): Promise<boolean> {
    const path = q.filePath.split("/").map(encodeURIComponent).join("/");
    const res = await this.request(`/repos/${this.opts.repo}/contents/${path}${q.ref ? `?ref=${encodeURIComponent(q.ref)}` : ""}`, q.signal);
    if (res.ok) return true;
    if (res.status !== 404) throw new Error(`GitHub API ${res.status} for /repos/${this.opts.repo}/contents`);
    // A private repo also answers 404 without a valid token: make sure the repo itself is reachable.
    const repo = await this.request(`/repos/${this.opts.repo}`, q.signal);
    if (!repo.ok) throw new Error(`GitHub repository ${this.opts.repo} is not accessible (HTTP ${repo.status}); check GITHUB_REPO and GITHUB_TOKEN`);
    return false;
  }

  private async get<T>(path: string, signal?: AbortSignal): Promise<T> {
    const res = await this.request(path, signal);
    if (!res.ok) throw new Error(`GitHub API ${res.status} for ${path.split("?")[0]}`);
    return (await res.json()) as T;
  }

  private request(path: string, signal?: AbortSignal): Promise<Response> {
    const headers: Record<string, string> = {
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "openclaw-pipeline-investigator",
    };
    if (this.opts.token) headers.Authorization = `Bearer ${this.opts.token}`;
    return (this.opts.fetch ?? fetch)(`${this.opts.apiUrl.replace(/\/+$/, "")}${path}`, {
      method: "GET",
      headers,
      signal: signal ?? AbortSignal.timeout(15_000),
    });
  }
}
