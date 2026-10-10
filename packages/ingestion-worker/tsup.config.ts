import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/main.ts", "src/report.ts"],
  format: ["esm"],
  target: "node24",
  platform: "node",
  outDir: "dist",
  clean: true,
  sourcemap: true,
  noExternal: ["@openclaw-line/shared"],
  // Keep "node:" specifiers: tsup would otherwise rewrite node:sqlite to a bare
  // "sqlite" package import (node:sqlite has no unprefixed form).
  removeNodeProtocol: false,
  // Copies public/dashboard.html next to the bundle (dist/dashboard.html).
  publicDir: "public",
});
