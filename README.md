# OpenClaw Pipeline Incident Investigator (LINE)

A read-only incident triage agent for data pipelines, built on [OpenClaw](https://docs.openclaw.ai) and powered by **Moonshot Kimi K3** (`moonshot/kimi-k3`). When an Airflow task fails, the investigator does two things:

- **Gathers evidence:** task logs, Databricks run errors, recent git diffs and dbt lineage.
- **Posts a report to LINE:** facts are kept separate from hypotheses, the blast radius is listed, and there are at most 3 next steps.

It never retries, fixes or changes anything. This implements roadmap Phases 1 and 2 of `spec.docx`, plus all §5 guardrails, and the Phase 3 feedback buttons (👍 / 👎 / "Misleading") with a feedback log for prompt tuning.

```
Airflow on_failure_callback ──HMAC POST──▶ ingestion worker ──POST /hooks/agent──▶ OpenClaw Gateway
                                           (dedupe per run,                         agent: pipeline-investigator
                                            log pre-filter,                         (isolated session, 4 read-only tools)
                                            sanitize)                                     │
                                                                                          ▼
                                                                                   LINE group (push)
```

## Layout

| Path | What |
|---|---|
| `packages/shared` | Sanitizer (secrets/PII), log pre-filter, read-only Airflow REST client (v1/v2) |
| `packages/plugin` | OpenClaw tool plugin (`defineToolPlugin`) with `get_airflow_task_log`, `get_databricks_run_error`, `get_git_file_diff` and `get_downstream_impact` |
| `packages/ingestion-worker` | Receives callbacks, verifies HMAC, dedupes per `(dag_id, run_id)`, picks the earliest failed task, pre-filters and sanitizes the log, then triggers `/hooks/agent`. Phase 3: sends the feedback card or a fallback notice, serves the LINE webhook, keeps the incident/feedback log (SQLite) |
| `packages/report-capture` | OpenClaw hook plugin: forwards each delivered report (matched by its `Ref: INC-…` line) to the worker's feedback log |
| `openclaw/openclaw.json5` | Gateway config: hooks, LINE channel, plugin, and the investigator agent with its tool policy |
| `openclaw/workspace-investigator/AGENTS.md` | System prompt and output schema (spec §3, adapted for LINE plain text) |
| `airflow/plugins/openclaw_callback.py` | `notify_openclaw`, the `on_failure_callback` (stdlib only, never raises) |
| `airflow/dags/` | Demo DAG `daily_revenue_pipeline`, which reproduces the spec's Stripe `KeyError`, and `databricks_daily_revenue`, which triggers the Databricks job |
| `databricks/` | Databricks Asset Bundle: the real revenue pipeline on Databricks (GCP), bronze/silver/gold on serverless |
| `demo/` | docker-compose stack (incl. Cloudflare tunnel), dbt manifest fixture, git-history seeder, dry-run and tunnel scripts |

## Read-only enforcement

1. **Agent tool policy.** The agent's `allow` list holds only the 4 plugin tools, plus `deny` for runtime, fs, web, memory, sessions, messaging, cron and gateway. The tools are `optional`, so no other agent sees them. Code Mode and Tool Search are off (`tools.codeMode: false`, `tools.toolSearch: false`), so the model calls the 4 tools directly instead of through a model-written JavaScript `exec` or a generic `tool_call`.
2. **No background runs.** Cron, the agent heartbeat, memory "dreaming" and skill review are disabled; the agent only runs when an incident arrives.
3. **Tools only read.** They make HTTP GET requests (plus the Airflow 3 and Databricks OAuth token exchanges) or run `git log` through `execFile`, with path-traversal checks.
4. **Read-only credentials.** Airflow uses a `viewer` role, Databricks a service principal with `CAN_VIEW` on the job only, GitHub a `contents:read` token, GitLab a `read_api` token. Airflow starts Databricks runs with a separate service principal (`CAN_MANAGE_RUN` only) that the gateway never sees. The repo and the dbt manifest are mounted `:ro`.
5. **Isolated sessions.** Each incident runs in its own isolated session. `hooks.allowRequestSessionKey` is `false`, so nothing carries over between incidents.

## Guardrails (spec §5)

| Challenge | Where |
|---|---|
| Log explosion | `shared/logFilter.ts` keeps the last 300 lines plus earlier `ERROR`/`CRITICAL`/`Traceback`/`FAILED` lines and full tracebacks, under a hard character cap. It runs in the worker and in the log tool. |
| Credential/PII leakage | `shared/sanitize.ts` masks JWTs, bearer tokens, connection-string passwords, key=value secrets, cloud and SaaS keys, emails and card numbers. It runs on every exception, log and diff before the model sees it. |
| Flapping/cascades | `ingestion-worker/dedupe.ts` waits a debounce window per run. It root-causes only the earliest failed task with no failed ancestor; the other failures become blast radius. Each run is handled once (TTL plus an idempotency key). |
| Session bleed | The worker never sends a `sessionKey`, so each run gets a fresh `isolated` session. |

## Develop

Requires Node ≥ 24.16 (OpenClaw's minimum) and Python 3.10+.

```bash
make install            # npm workspaces (includes the openclaw CLI as a dev dependency)
make build typecheck test
make validate-plugin    # openclaw plugins build --check + validate
pip install pytest && make test-py
```

## Demo (docker compose)

```bash
make demo-env    # creates demo/.env – fill in MOONSHOT_API_KEY (+ LINE_* for delivery)
make demo-dry    # full stack; prints the triage report in the terminal (no LINE needed)
make demo-up && make demo-trigger   # real run: report is pushed to LINE_TARGET_ID
make demo-tunnel   # Phase 3: Cloudflare tunnel + LINE webhook for the feedback buttons
make feedback-report               # review feedback for prompt tuning
make demo-dashboard                # live dashboard at http://localhost:8090/dashboard
make demo-logs / make demo-down
```

The demo DAG runs only when triggered (`schedule=None`): every run fails on purpose and costs a model call plus a LINE message.

What happens in the demo:
1. `extract_stripe_transactions` fails with `KeyError: 'payment_intent_id'` on a `charge.refunded` event.
2. `validate_extract_counts` also fails, so the worker receives 2 callbacks. They collapse into 1 investigation.
3. The seeded repo's last commit, "Bypass empty payloads…", changed `.get("payment_intent_id")` to `payload["payment_intent_id"]`.
4. The lineage lookup should list `fct_daily_revenue` (SLA 06:00) and `finance_reconciliation`, plus the "Executive Daily KPI (Looker)" and "Daily Net Inflow Alert" exposures.

### Model: Moonshot Kimi K3
- The model is `moonshot/kimi-k3`. It is served by OpenClaw's official `@openclaw/moonshot-provider` plugin, which is installed into the gateway image at build time together with `@openclaw/line`.
- Set `MOONSHOT_API_KEY` to a key from the Moonshot Open Platform. Kimi Coding keys are a separate provider and won't work.
- Set `MOONSHOT_BASE_URL` to `https://api.moonshot.ai/v1` (international) or `https://api.moonshot.cn/v1` (China).
- On the direct Moonshot route, K3 always reasons at `max` effort. Investigations are therefore slower and cost more per run, so the hook timeout defaults to 300 s (`OPENCLAW_TIMEOUT_S`).
- To switch models, change `OPENCLAW_MODEL` (for example `moonshot/kimi-k2.7-code`). Any other provider also needs its plugin and a `models.providers` entry.
- The Moonshot plugin also registers a Kimi `web_search` tool. The investigator's tool policy still denies `group:web`.
- Incident evidence goes to Moonshot's API after sanitization. Check that this fits your data-handling and residency rules, especially for the China endpoint.

### LINE setup
1. In the LINE Developers Console, create a Messaging API channel. Copy its channel access token and channel secret into `demo/.env`.
2. Add the bot to your on-call group and put the group id (`C` followed by 32 hex characters) in `LINE_TARGET_ID`.
3. The investigator only *pushes* messages. Inbound DMs and group chat are disabled in OpenClaw (`dmPolicy`/`groupPolicy: "disabled"`). The channel's webhook goes to the ingestion worker instead, for the feedback buttons (below).

### Feedback buttons (Phase 3)
After each delivered report, the worker pushes a card asking "Was this triage helpful?" with 👍 Helpful / 👎 Not helpful / ⚠️ Misleading buttons. A 👎 or ⚠️ tap gets quick-reply reasons (Wrong root cause, Missed blast radius, Made-up fact, Not actionable). If the run fails or the report can't be delivered, the worker pushes a short fallback notice with the Airflow link instead.

```
Airflow ─▶ worker ─/hooks/agent (waitForCompletion)─▶ OpenClaw ─push report─▶ LINE group
             │  ◀── {status, delivered} ────────────────┘   │ report-capture plugin
             │  ◀── report text (signed) ────────────────────┘
             ├─ push feedback card / fallback ─────────────────────────────────▶ LINE group
LINE ──POST /line/webhook──▶ Cloudflare tunnel ──▶ worker :8091 (verify, record, reply)
```

- **Setup:** `make demo-up`, then `make demo-tunnel`. This starts a Cloudflare quick tunnel (no account needed) and registers `https://<random>.trycloudflare.com/line/webhook` as the channel's webhook URL through the LINE API. "Use webhook" must be on in the LINE Developers Console (Messaging API tab); the script tells you if it is off.
- **Stable URL:** a quick-tunnel URL changes every time the tunnel restarts, so re-run `make demo-tunnel`. For a fixed hostname, create a named tunnel in Cloudflare Zero Trust with a public hostname pointing to `http://ingestion-worker:8091`, then set `CLOUDFLARE_TUNNEL_TOKEN` and `PUBLIC_WEBHOOK_BASE_URL` in `demo/.env`.
- **Review:** `make feedback-report` (or `DAYS=7 make feedback-report`) shows votes and the helpful rate by prompt version (a hash of `AGENTS.md`), reasons, triage time (first failure callback → report delivered), and every 👎 / ⚠️ with its report text. Use it to tune `AGENTS.md`, then compare the next prompt version.
- **Security:** only `/line/webhook` is reachable through the tunnel (a separate listener on port 8091). Requests must carry a valid `x-line-signature`; button data is HMAC-signed so votes can't be forged; LINE redeliveries are ignored. Voters are stored as salted hashes, never raw LINE user ids. Data lives in `demo/.state/feedback/feedback.db`.
- **Dashboard:** `make demo-dashboard` opens http://localhost:8090/dashboard, a live view of the same data. It shows the pipeline funnel, helpful rate, triage times, votes by prompt version, reasons, and every incident with its timeline and report. It refreshes every 30 s. It is served only on the worker's internal port, never through the tunnel, because it shows report text. The page uses a strict Content-Security-Policy and renders report text as plain text.
- **Dashboard on your own domain:** publish it as e.g. `dashboard.<domain>` through the named tunnel (service `http://ingestion-worker:8092`), protected by a Cloudflare Access application that allows only your email. The worker checks every request on port 8092 as well: the Access token's signature (against your team's published keys), audience, issuer and expiry, and that its email is in `DASHBOARD_ALLOWED_EMAILS`. Port 8092 serves only the dashboard, never the internal endpoints, and doesn't start unless `CF_ACCESS_TEAM_DOMAIN`, `CF_ACCESS_AUD` and `DASHBOARD_ALLOWED_EMAILS` are all set.
- **Airflow UI on your own domain:** add a second route to the same named tunnel, e.g. `airflow.<domain>` → `http://airflow:8080`, protected by its own Cloudflare Access application (allow only your email). On the route, turn on **Protect with Access** with that application's AUD, so the tunnel connector also rejects requests without a valid Access token. Set `AIRFLOW_PUBLIC_URL=https://airflow.<domain>` so login redirects and log links use it. Error 1033 after the Access login means the hostname is attached to a tunnel with no running connector: put the route on the tunnel your server runs.
- **Votes:** one per person per incident; a later tap replaces the earlier one. Taps and replies use LINE's free reply API; each incident uses 2 pushes (report + card).

## Deploy to a Linux server
Needs only Docker with the Compose plugin; Node runs inside a container for the build.
1. Copy the repo and `demo/.env` to the server, for example with `rsync -az --exclude node_modules --exclude 'packages/*/dist' --exclude demo/.state ./ user@server:OpenClaw-Line/`. On later syncs also exclude `demo/.env`, which holds server-only settings.
2. In the server's `demo/.env`, set `AIRFLOW_UID=$(id -u)`. Override `AIRFLOW_HOST_PORT`, `WORKER_HOST_PORT` or `WEBHOOK_HOST_PORT` if those ports are taken.
3. Run `make demo-prepare-docker`, then `docker compose --env-file demo/.env -f demo/docker-compose.yml up -d --build`, then `make demo-tunnel`.
4. Run the named tunnel on **one** machine only. Stop the old host's stack before starting the tunnel on the new one, otherwise Cloudflare splits LINE webhooks and dashboard traffic between them.

All services use `restart: unless-stopped`, so the stack comes back after a reboot. Airflow's history (metadata DB and task logs) lives in the `airflow-home` volume: `make demo-down` keeps it and `make demo-clean` deletes it. To open the Airflow login page from your network, set `AIRFLOW_BIND_ADDRESS` to the server's LAN IP and `AIRFLOW_PUBLIC_URL=http://<ip>:8080`, and use a strong `AIRFLOW_ADMIN_PASSWORD`. The other host ports bind to `127.0.0.1`; reach the local dashboard with `ssh -L 8090:localhost:<WORKER_HOST_PORT> user@server`.

## Databricks pipeline (GCP)
`databricks/` is a Databricks Asset Bundle with a real version of the revenue pipeline, job `daily_revenue_lakehouse` on serverless compute: `land_stripe_events` (synthetic Stripe-style events into a GCS-backed volume) → `bronze_ingest` (Auto Loader, fixed schema, unknown fields to `_rescued_data`) → `silver_transactions` (fails on schema drift or null amounts, dedupes, signs refunds) → `gold_daily_revenue` (per-day totals, refuses empty or negative days). It has no schedule; Airflow triggers it.

One-time setup by hand: a GCS bucket, a Unity Catalog storage credential and external location for it, catalog `lakehouse` (managed location `gs://<bucket>/_managed`) with schemas `bronze`, `silver`, `gold`, and external volume `lakehouse.bronze.landing` on `gs://<bucket>/landing`. File events on the external location are optional; without them Auto Loader lists the directory.

- `databricks auth login --host <workspace URL> --profile openclaw`, then `make lakehouse-deploy`.
- `make lakehouse-run` runs it once; `FAULT=schema_drift|null_amounts|empty` simulates upstream breakage for incident tests.
- Each run lands its own file and silver reads only that file (via task values), so a rerun after a bad delivery is clean. Silver and gold rewrite their date partition, so reruns are idempotent.

Airflow side: DAG `databricks_daily_revenue` (manual for now; the `inject_fault` param passes through) starts the job with `DatabricksRunNowOperator`. The Airflow image adds the Databricks provider (`demo/airflow.Dockerfile`). Two service principals, both granted in the bundle: `DATABRICKS_RUNNER_*` (`CAN_MANAGE_RUN`) become Airflow's `databricks_default` connection; `DATABRICKS_READER_*` (`CAN_VIEW`) go only to the gateway for `get_databricks_run_error`. On failure, `notify_openclaw` adds the Databricks run id from XCom, and the investigator reads the failed Databricks task's error and trace instead of the Airflow log, which only links to the run.

## Status

| Area | State |
|---|---|
| Phase 1 (callback, log tool, report to chat) | Done; delivered to LINE instead of Slack |
| Phase 2 (git diff, dbt lineage, facts vs. hypotheses, blast radius) | Done; verified with local git and the fixture manifest |
| Phase 3 (feedback buttons, feedback log, prompt-version tracking) | Done; triage time is measured, resolution time (MTTR) is not yet |
| Dashboard | Live view with Cloudflare Access for public hostnames |
| Deployment | Linux server with Docker; named Cloudflare tunnel for the LINE webhook, dashboard and Airflow UI |

Open items:
- MTTR: record when an incident is resolved (a "✅ Resolved" button, or detecting the task's next success).
- Real systems: production DAGs and a real Airflow viewer account (Airflow 2 path untested live), GitHub/GitLab provider with a read-only token, a production dbt manifest sync, the "data catalog" lineage option.
- Reliability: backup model, alerting when the worker is down or a LINE push fails, persistent dedupe state, SQLite feedback-log backups.
- Quality: live tests for "Insufficient evidence", prompt injection in logs, very large logs and big cascades; an automatic check that every SHA, path and name in a report appears in the tool results.
- CI (vitest + pytest on PRs), cost tracking per incident, and a data-residency review for sending sanitized evidence to Moonshot.
- Spec details: execution time in the summary line; the callback sends `log_url` rather than `log_filepath`.

## Production notes
- Set `gitProvider` to `github` or `gitlab` in `openclaw.json5`, and provide `GITHUB_TOKEN` or `GITLAB_TOKEN` through the environment.
- Mount the production `manifest.json` from your dbt runs. Use `lineage_map.json` to map Airflow task ids to dbt nodes when the names differ.
- Outside docker, install the provider and channel with `openclaw plugins install @openclaw/moonshot-provider` and `openclaw plugins install @openclaw/line`.
- Install the investigator plugin with `openclaw plugins install <staged dir> --force` (stage it with `node scripts/stage-plugin.mjs <dir>`), as the demo image does. Non-interactive installs also need `--accept-capabilities`.
- Keep `tools.codeMode: false` and `tools.toolSearch: false`. Code Mode auto-enables for preferred models such as Kimi K3 and replaces the tools with a model-written JavaScript `exec` running in `node:vm`, which is not a security boundary. Tool Search hides the tool schemas behind a generic `tool_call`. After each OpenClaw upgrade, check that a run still calls the tools directly.
- Don't set `tools.profile` on the investigator agent. Profiles are applied before `allow` and none of them include plugin tools, so runs fail with "No callable tools remain…".
- Keep the Gateway port private. Its token is an operator credential.
- Publish only the worker's public listener (`PUBLIC_PORT`, `/line/webhook`) through a named Cloudflare tunnel or your ingress. Keep the internal port (Airflow callbacks, report capture) private.
- The feedback log is a single-writer SQLite file: run one worker replica and back up `FEEDBACK_DB_PATH`.
