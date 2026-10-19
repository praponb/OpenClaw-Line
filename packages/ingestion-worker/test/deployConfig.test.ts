import { readFileSync } from "node:fs";
import JSON5 from "json5";
import { describe, expect, it } from "vitest";
import { parse as parseYaml } from "yaml";

/**
 * Guards on the deployment files, so an OpenClaw upgrade or a config edit can't quietly
 * widen the investigator's tools or put a port on the network.
 */
const repoFile = (path: string) => readFileSync(new URL(`../../../${path}`, import.meta.url), "utf8");

type AgentTools = { profile?: string; codeMode?: boolean; allow?: string[]; deny?: string[] };
const config = JSON5.parse(repoFile("openclaw/openclaw.json5"));
const investigator = config.agents.entries["pipeline-investigator"] as { tools: AgentTools };
const manifest = JSON.parse(repoFile("packages/plugin/openclaw.plugin.json")) as { contracts: { tools: string[] } };

describe("openclaw.json5", () => {
  it("keeps Code Mode and Tool Search off", () => {
    // Code Mode swaps the tools for a model-written JavaScript exec (node:vm is not a
    // security boundary); Tool Search hides their schemas behind tool_search/tool_call.
    expect(config.tools.codeMode).toBe(false);
    expect(config.tools.toolSearch).toBe(false);
    expect(investigator.tools.codeMode).toBe(false);
  });

  it("gives the investigator exactly the plugin's read-only tools", () => {
    // A profile is applied before `allow` and includes no plugin tools: the run would have none.
    expect(investigator.tools.profile).toBeUndefined();
    expect([...(investigator.tools.allow ?? [])].sort()).toEqual([...manifest.contracts.tools].sort());
    for (const group of ["group:runtime", "group:fs", "group:web", "group:sessions", "group:automation", "group:messaging"]) {
      expect(investigator.tools.deny).toContain(group);
    }
  });

  it("has no Moonshot provider", () => {
    expect(Object.keys(config.plugins?.entries ?? {})).not.toContain("moonshot");
    expect(Object.keys(config.models?.providers ?? {})).not.toContain("moonshot");
    expect(repoFile("demo/openclaw.Dockerfile")).not.toMatch(/moonshot/i);
  });
});

describe("compose files", () => {
  // Docker-published ports bypass ufw, so each one binds to loopback unless a
  // *_BIND_ADDRESS variable in demo/.env says otherwise.
  const LOOPBACK = /^(?:127\.0\.0\.1|\$\{[A-Z_]+_BIND_ADDRESS:-127\.0\.0\.1\}):/;

  it.each(["demo/docker-compose.yml", "airflow/production/docker-compose.yml"])("%s publishes ports on loopback only", (file) => {
    const services = parseYaml(repoFile(file)).services as Record<string, { ports?: unknown[] }>;
    const ports = Object.entries(services).flatMap(([name, s]) => (s.ports ?? []).map((p) => [name, p] as const));
    expect(ports.length).toBeGreaterThan(0);
    for (const [name, port] of ports) {
      const binding = typeof port === "string" ? port : `${(port as { host_ip?: string }).host_ip ?? ""}:`;
      expect(binding, `${file}: ${name}`).toMatch(LOOPBACK);
    }
  });
});
