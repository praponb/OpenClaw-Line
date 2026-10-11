export type FileCommit = {
  sha: string;
  author: string;
  date: string;
  message: string;
  /** Unified diff for the requested file only. */
  patch: string;
};

export type CommitQuery = {
  filePath: string;
  sinceIso: string;
  limit: number;
  ref?: string;
  signal?: AbortSignal;
};

export type FileQuery = { filePath: string; ref?: string; signal?: AbortSignal };

export interface GitProvider {
  readonly name: string;
  recentFileCommits(query: CommitQuery): Promise<FileCommit[]>;
  /** Whether the path exists at the ref, so "no commits" is not mistaken for "unchanged". */
  fileExists(query: FileQuery): Promise<boolean>;
}

/** Repository-relative paths only: no absolute paths, traversal, or option injection. */
export function assertSafeRepoPath(filePath: string): string {
  const p = filePath.trim().replace(/^\.\/+/, "");
  if (!p) throw new Error("file_path is empty");
  if (p.startsWith("/") || /^[A-Za-z]:[\\/]/.test(p)) throw new Error("file_path must be relative to the repository root");
  if (p.split(/[\\/]+/).some((seg) => seg === "..")) throw new Error("file_path must not contain '..'");
  if (p.startsWith("-")) throw new Error("file_path must not start with '-'");
  if (/[\0\n\r]/.test(p)) throw new Error("file_path contains control characters");
  return p;
}

export function assertSafeRef(ref: string): string {
  if (!/^[A-Za-z0-9._/-]+$/.test(ref) || ref.startsWith("-") || ref.includes("..")) {
    throw new Error(`invalid git ref "${ref}"`);
  }
  return ref;
}
