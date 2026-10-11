import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { resolveConfig } from "../src/config.js";
import { getGitFileDiff } from "../src/tools/gitDiff.js";

const json = (b: unknown) => new Response(JSON.stringify(b), { status: 200 });

describe("get_git_file_diff: path safety", () => {
  const cfg = resolveConfig({ gitProvider: "local", localRepoPath: "/nonexistent" }, {});
  it.each(["../etc/passwd", "/etc/passwd", "dags/../../x", "--output=/tmp/x"])("rejects %s", async (p) => {
    expect(await getGitFileDiff({ file_path: p }, cfg)).toMatch(/^Tool error:/);
  });
  it("errors when no provider is configured", async () => {
    expect(await getGitFileDiff({ file_path: "a.py" }, resolveConfig({}, {}))).toMatch(/^Tool error: no git provider/);
  });
});

describe("get_git_file_diff: github", () => {
  it("lists commits for the path and returns that file's patch, sanitized", async () => {
    const fetchMock = vi.fn(async (url: string | URL | Request) => {
      const u = String(url);
      if (u.includes("/commits?")) {
        return json([{ sha: "a93f1d0c", commit: { author: { name: "Dev", date: "2026-10-09T10:00:00Z" }, message: "Bypass empty payloads\n\nbody" }, author: { login: "data-eng" } }]);
      }
      return json({
        files: [
          { filename: "other.py", patch: "@@ other" },
          { filename: "dags/stripe_ingest/extract_stripe.py", patch: '-x = payload.get("payment_intent_id")\n+x = payload["payment_intent_id"]\n+# contact ops@example.com' },
        ],
      });
    });
    const cfg = resolveConfig({ gitProvider: "github", githubRepo: "acme/pipes" }, { GITHUB_TOKEN: "ghp_x" });
    const out = await getGitFileDiff({ file_path: "dags/stripe_ingest/extract_stripe.py", lookback_days: 3 }, cfg, {
      fetch: fetchMock as typeof fetch,
      now: () => new Date("2026-10-10T03:00:00Z"),
    });
    expect(String(fetchMock.mock.calls[0]![0])).toBe("https://api.github.com/repos/acme/pipes/contents/dags/stripe_ingest/extract_stripe.py");
    const listUrl = new URL(String(fetchMock.mock.calls[1]![0]));
    expect(listUrl.pathname).toBe("/repos/acme/pipes/commits");
    expect(listUrl.searchParams.get("since")).toBe("2026-10-07T03:00:00.000Z");
    expect(listUrl.searchParams.get("path")).toBe("dags/stripe_ingest/extract_stripe.py");
    expect(out).toContain("commit a93f1d0c");
    expect(out).toContain("author: @data-eng");
    expect(out).toContain('+x = payload["payment_intent_id"]');
    expect(out).not.toContain("@@ other");
    expect(out).toContain("[REDACTED:email]");
    for (const [, init] of fetchMock.mock.calls as unknown as [string, RequestInit][]) expect(init.method).toBe("GET");
  });

  it("tries configured prefixes and says plainly when the file is not in the repository", async () => {
    const seen: string[] = [];
    const fetchMock = vi.fn(async (url: string | URL | Request) => {
      const u = String(url);
      seen.push(u);
      if (u.includes("/contents/")) return new Response("Not Found", { status: 404 });
      return json({ full_name: "acme/pipes" });
    });
    const cfg = resolveConfig({ gitProvider: "github", githubRepo: "acme/pipes", gitRef: "main" }, { GIT_PATH_PREFIXES: "airflow/, ../bad" });
    const out = await getGitFileDiff({ file_path: "dags/x.py" }, cfg, { fetch: fetchMock as typeof fetch });
    expect(seen).toEqual([
      "https://api.github.com/repos/acme/pipes/contents/dags/x.py?ref=main",
      "https://api.github.com/repos/acme/pipes",
      "https://api.github.com/repos/acme/pipes/contents/airflow/dags/x.py?ref=main",
      "https://api.github.com/repos/acme/pipes",
    ]);
    expect(out).toContain("File not found in the repository (tried: dags/x.py, airflow/dags/x.py)");
    expect(out).toContain("NOT evidence that the code is unchanged");
    expect(out).not.toContain("No commits touched");
  });

  it("treats an unreachable repository as a tool error, not a missing file", async () => {
    const fetchMock = vi.fn(async () => new Response("Not Found", { status: 404 }));
    const cfg = resolveConfig({ gitProvider: "github", githubRepo: "acme/private" }, {});
    const out = await getGitFileDiff({ file_path: "dags/x.py" }, cfg, { fetch: fetchMock as typeof fetch });
    expect(out).toMatch(/^Tool error: could not check whether dags\/x.py exists in the repository: GitHub repository acme\/private is not accessible \(HTTP 404\)/);
    expect(out).not.toContain("File not found");
  });
});

describe("get_git_file_diff: gitlab", () => {
  it("uses the project commits + diff endpoints", async () => {
    const fetchMock = vi.fn(async (url: string | URL | Request) => {
      const u = String(url);
      if (u.includes("/repository/commits?")) return json([{ id: "b1c2", author_name: "Dev", authored_date: "2026-10-09", message: "change" }]);
      return json([{ old_path: "models/a.sql", new_path: "models/a.sql", diff: "+select 1" }]);
    });
    const cfg = resolveConfig({ gitProvider: "gitlab", gitlabProject: "data/pipes" }, { GITLAB_TOKEN: "t" });
    const out = await getGitFileDiff({ file_path: "models/a.sql" }, cfg, { fetch: fetchMock as typeof fetch });
    expect(String(fetchMock.mock.calls[0]![0])).toContain("/projects/data%2Fpipes/repository/files/models%2Fa.sql?ref=HEAD");
    expect(String(fetchMock.mock.calls[1]![0])).toContain("/projects/data%2Fpipes/repository/commits?");
    expect(String(fetchMock.mock.calls[2]![0])).toContain("/projects/data%2Fpipes/repository/commits/b1c2/diff");
    expect(out).toContain("+select 1");
  });
});

describe("get_git_file_diff: local", () => {
  let repo: string;
  const git = (args: string[], date: string) =>
    execFileSync("git", ["-C", repo, ...args], {
      env: { ...process.env, GIT_AUTHOR_DATE: date, GIT_COMMITTER_DATE: date, GIT_AUTHOR_NAME: "Dev", GIT_AUTHOR_EMAIL: "dev@example.com", GIT_COMMITTER_NAME: "Dev", GIT_COMMITTER_EMAIL: "dev@example.com" },
    });

  beforeAll(() => {
    repo = mkdtempSync(join(tmpdir(), "oc-git-"));
    execFileSync("git", ["init", "-q", repo]);
    mkdirSync(join(repo, "dags"));
    mkdirSync(join(repo, "airflow/dags"), { recursive: true });
    writeFileSync(join(repo, "dags/x.py"), 'v = p.get("id")\n');
    writeFileSync(join(repo, "airflow/dags/y.py"), "y = 1\n");
    git(["add", "."], "2026-09-01T00:00:00Z");
    git(["commit", "-qm", "initial"], "2026-09-01T00:00:00Z");
    writeFileSync(join(repo, "dags/x.py"), 'v = p["id"]\n');
    git(["commit", "-qam", "use direct access"], "2026-10-09T12:00:00Z");
  });
  afterAll(() => rmSync(repo, { recursive: true, force: true }));

  it("returns only commits in the lookback window", async () => {
    const cfg = resolveConfig({ gitProvider: "local", localRepoPath: repo }, {});
    const out = await getGitFileDiff({ file_path: "dags/x.py", lookback_days: 3 }, cfg, { now: () => new Date("2026-10-10T03:00:00Z") });
    expect(out).toContain("commits (newest first): 1");
    expect(out).toContain("message: use direct access");
    expect(out).toContain('-v = p.get("id")');
    expect(out).toContain('+v = p["id"]');
    expect(out).not.toContain("message: initial");
  });

  it("says so when nothing changed", async () => {
    const cfg = resolveConfig({ gitProvider: "local", localRepoPath: repo }, {});
    const out = await getGitFileDiff({ file_path: "dags/x.py", lookback_days: 1 }, cfg, { now: () => new Date("2026-10-20T00:00:00Z") });
    expect(out).toContain("No commits touched this file");
    expect(out).toContain("The file exists in the repository");
  });

  it("reports a missing file instead of 'no commits'", async () => {
    const cfg = resolveConfig({ gitProvider: "local", localRepoPath: repo }, {});
    const out = await getGitFileDiff({ file_path: "databricks/src/03_silver_transactions.py" }, cfg);
    expect(out).toContain("File not found in the repository (tried: databricks/src/03_silver_transactions.py)");
  });

  it("resolves a path through a configured prefix", async () => {
    const cfg = resolveConfig({ gitProvider: "local", localRepoPath: repo, gitPathPrefixes: "airflow" }, {});
    const out = await getGitFileDiff({ file_path: "dags/y.py", lookback_days: 30 }, cfg, { now: () => new Date("2026-09-02T00:00:00Z") });
    expect(out).toContain("file=airflow/dags/y.py (requested dags/y.py)");
    expect(out).toContain("message: initial");
  });
});
