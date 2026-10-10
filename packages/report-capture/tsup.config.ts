import { defineConfig } from "tsup";

// Single self-contained ESM entry; only the openclaw host stays external.
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
  external: [/^openclaw(\/.*)?$/],
});
