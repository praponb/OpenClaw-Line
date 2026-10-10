#!/usr/bin/env node
// Stage the built plugin as a self-contained package directory that OpenClaw
// can load via `plugins.load.paths` (or `openclaw plugins install <dir>`):
//   <out>/dist, openclaw.plugin.json, package.json (runtime deps only), node_modules/typebox
// Usage: node scripts/stage-plugin.mjs <outDir>
import { execFileSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const src = join(root, "packages/plugin");
const out = resolve(process.argv[2] ?? join(root, "demo/.state/plugin/pipeline-investigator"));

if (!existsSync(join(src, "dist/index.js"))) {
  console.error("packages/plugin/dist/index.js missing - run `npm run build` first");
  process.exit(1);
}

rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });
cpSync(join(src, "dist"), join(out, "dist"), { recursive: true });
cpSync(join(src, "openclaw.plugin.json"), join(out, "openclaw.plugin.json"));

const pkg = JSON.parse(readFileSync(join(src, "package.json"), "utf8"));
delete pkg.devDependencies;
delete pkg.scripts;
writeFileSync(join(out, "package.json"), JSON.stringify(pkg, null, 2) + "\n");

execFileSync("npm", ["install", "--omit=dev", "--omit=peer", "--no-package-lock", "--no-audit", "--no-fund", "--ignore-scripts"], {
  cwd: out,
  stdio: "inherit",
});
console.log(`staged plugin at ${out}`);
