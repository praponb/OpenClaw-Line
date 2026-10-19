# OpenClaw Gateway with the investigator baked in. Plugin + workspace are
# copied (not bind-mounted) so file ownership matches the `node` user and the
# plugin loader's ownership check passes.
# Stage the plugin first: `node scripts/stage-plugin.mjs` (see Makefile).
ARG OPENCLAW_IMAGE=ghcr.io/openclaw/openclaw:2026.10.2
FROM ${OPENCLAW_IMAGE}

USER root
# get_git_file_diff (provider=local) shells out to `git log`.
RUN command -v git >/dev/null 2>&1 || \
    (apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*) || \
    apk add --no-cache git
RUN mkdir -p /opt/openclaw /opt/openclaw-plugins && chown node:node /opt/openclaw /opt/openclaw-plugins
COPY --chown=node:node demo/.state/plugin/pipeline-investigator /opt/openclaw-plugins/pipeline-investigator
COPY --chown=node:node demo/.state/plugin/report-capture /opt/openclaw-plugins/report-capture
COPY --chown=node:node openclaw/workspace-investigator /opt/openclaw/workspace-investigator

USER node
# Official external plugin (LINE channel; the Vertex provider is bundled) plus the
# investigator + report-capture plugins. OpenClaw installs plugins next to the active config file
# (<config dir>/extensions and <config dir>/npm), so install them with a throwaway
# config in /opt/openclaw; the real config below lives in the same directory and
# finds them at runtime. The investigator must be *installed* (not loaded via
# plugins.load.paths): agent runs only pick tools from plugins with an install record.
# Pin the official plugins to the gateway version: unpinned installs pull the newest
# release, which can require a newer plugin API than this image (build fails).
ARG OPENCLAW_PLUGIN_VERSION=2026.10.2
RUN echo '{}' > /opt/openclaw/build.json5 \
 && OPENCLAW_CONFIG_PATH=/opt/openclaw/build.json5 openclaw plugins install @openclaw/line@${OPENCLAW_PLUGIN_VERSION} \
 && OPENCLAW_CONFIG_PATH=/opt/openclaw/build.json5 openclaw plugins install /opt/openclaw-plugins/pipeline-investigator --force --accept-capabilities \
 && OPENCLAW_CONFIG_PATH=/opt/openclaw/build.json5 openclaw plugins install /opt/openclaw-plugins/report-capture --force --accept-capabilities \
 && rm -f /opt/openclaw/build.json5*

COPY --chown=node:node openclaw/openclaw.json5 /opt/openclaw/openclaw.json5
ENV OPENCLAW_CONFIG_PATH=/opt/openclaw/openclaw.json5
