import type { CommitQuery, FileCommit, FileQuery, GitProvider } from "./types.js";

type GitlabCommit = {
  id: string;
  author_name: string;
  authored_date: string;
  message: string;
};

type GitlabDiff = { old_path: string; new_path: string; diff: string };

/** Read-only GitLab REST provider. Use a token with the `read_api` scope. */
export class GitlabProvider implements GitProvider {
  readonly name = "gitlab";

  constructor(
    private readonly opts: { apiUrl: string; project: string; token?: string; fetch?: typeof fetch },
  ) {}

  async recentFileCommits(q: CommitQuery): Promise<FileCommit[]> {
    const params = new URLSearchParams({ path: q.filePath, since: q.sinceIso, per_page: String(q.limit) });
    if (q.ref) params.set("ref_name", q.ref);
    const list = await this.get<GitlabCommit[]>(`/repository/commits?${params}`, q.signal);
    const commits: FileCommit[] = [];
    for (const c of list.slice(0, q.limit)) {
      const diffs = await this.get<GitlabDiff[]>(`/repository/commits/${c.id}/diff`, q.signal);
      const file = diffs.find((d) => d.new_path === q.filePath || d.old_path === q.filePath);
      commits.push({
        sha: c.id,
        author: c.author_name,
        date: c.authored_date,
        message: c.message,
        patch: file?.diff ?? "(file not in commit diff)",
      });
    }
    return commits;
  }

  async fileExists(q: FileQuery): Promise<boolean> {
    const res = await this.request(`/repository/files/${encodeURIComponent(q.filePath)}?ref=${encodeURIComponent(q.ref ?? "HEAD")}`, q.signal);
    if (res.ok) return true;
    if (res.status !== 404) throw new Error(`GitLab API ${res.status} for /repository/files`);
    // A private project also answers 404 without a valid token: make sure the project is reachable.
    const project = await this.request("", q.signal);
    if (!project.ok) throw new Error(`GitLab project ${this.opts.project} is not accessible (HTTP ${project.status}); check GITLAB_PROJECT and GITLAB_TOKEN`);
    return false;
  }

  private async get<T>(path: string, signal?: AbortSignal): Promise<T> {
    const res = await this.request(path, signal);
    if (!res.ok) throw new Error(`GitLab API ${res.status} for ${path.split("?")[0]}`);
    return (await res.json()) as T;
  }

  private request(path: string, signal?: AbortSignal): Promise<Response> {
    const project = /^\d+$/.test(this.opts.project) ? this.opts.project : encodeURIComponent(this.opts.project);
    const headers: Record<string, string> = { Accept: "application/json" };
    if (this.opts.token) headers["PRIVATE-TOKEN"] = this.opts.token;
    const url = `${this.opts.apiUrl.replace(/\/+$/, "")}/projects/${project}${path}`;
    return (this.opts.fetch ?? fetch)(url, {
      method: "GET",
      headers,
      signal: signal ?? AbortSignal.timeout(15_000),
    });
  }
}
