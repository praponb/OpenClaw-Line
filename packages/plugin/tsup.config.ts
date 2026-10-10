import { defineConfig } from "tsup";

// Bundle the shared workspace code into a single ESM entry so the plugin
// directory can be installed with `openclaw plugins install` on its own.
// `typebox` and `openclaw` stay external (runtime dependency / host).
export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm"],
  target: "node24",
  platform: "node",
  outDir: "dist",
  clean: true,
  sourcemap: true,
  dts: false,
  noExternal: ["@openclaw-line/shared"],
  external: ["typebox", /^openclaw(\/.*)?$/],
});
