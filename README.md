# OpenClaw Pipeline Incident Investigator (LINE)

A read-only incident triage agent for data pipelines, built on [OpenClaw](https://docs.openclaw.ai) and powered by **Gemini 3.7 Flash on Vertex AI** (`google-vertex/gemini-3.7-flash`, EU multi-region). When an Airflow task fails, the investigator does two things:

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

Docs: [operations runbook](docs/operations.md) (deploy, settings, identities, schedules, pitfalls) · [work log](docs/changelog.md) · [data residency](docs/data-residency.md)

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
4. **Read-only credentials.** The investigator's Airflow account has the `viewer` role (the dashboard's optional "Run pipeline" account is separate and held only by the worker), Databricks a service principal with `CAN_VIEW` on the job only, GitHub a `contents:read` token, GitLab a `read_api` token. Airflow starts Databricks runs with a separate service principal (`CAN_MANAGE_RUN` only) that the gateway never sees. The repo and the dbt manifest are mounted `:ro`.
5. **Isolated sessions.** Each incident runs in its own isolated session. `hooks.allowRequestSessionKey` is `false`, so nothing carries over between incidents.

## Guardrails (spec §5)

| Challenge | Where |
|---|---|
| Log explosion | `shared/logFilter.ts` keeps the last 300 lines plus earlier `ERROR`/`CRITICAL`/`Traceback`/`FAILED` lines and full tracebacks, under a hard character cap. It runs in the worker and in the log tool. |
| Credential/PII leakage | `shared/sanitize.ts` masks JWTs, bearer tokens, connection-string passwords, key=value secrets, cloud and SaaS keys, emails and card numbers. It runs on every exception, log and diff before the model sees it. |
| Flapping/cascades | `ingestion-worker/dedupe.ts` waits a debounce window per run. It root-causes only the earliest failed task with no failed ancestor; the other failures become blast radius. Each run is handled once (TTL plus an idempotency key), also across worker restarts. |
| Session bleed | The worker never sends a `sessionKey`, so each run gets a fresh `isolated` session. |

## Develop

Requires Node ≥ 24.16 (OpenClaw's minimum) and Python 3.10+.

```bash
make install            # npm workspaces (includes the openclaw CLI as a dev dependency)
make build typecheck test
make validate-plugin    # openclaw plugins build --check + validate
pip install pytest && make test-py
```

CI (`.github/workflows/ci.yml`) runs the same checks on every PR and push to `main`: build, typecheck, `vitest`, the plugin manifest check, and `pytest` for `airflow/tests` and `databricks/tests`. It needs no secrets.

## Demo (docker compose)

```bash
make demo-env    # creates demo/.env – fill in GOOGLE_CLOUD_PROJECT + the Vertex key (+ LINE_* for delivery)
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

### Model: Gemini on Vertex AI
- The model is `google-vertex/gemini-3.7-flash`, served by OpenClaw's bundled `google-vertex` provider (no extra plugin). Google Cloud's Service Terms: evidence is not used to train or fine-tune models.
- Credentials: a service account with only `roles/aiplatform.user`. Its JSON key goes in `demo/.state/vertex/` (mode 600, mounted read-only at `/run/vertex`); set `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_CLOUD_PROJECT` and `GOOGLE_CLOUD_LOCATION`.
- Location: Gemini 3.x Flash is served on `global`, `us` and `eu` only (not `asia-southeast1`). The default `eu` is the EU multi-region, where Google commits to in-region ML processing. See [docs/data-residency.md](docs/data-residency.md).
- **Backup model:** when the primary fails (outage, rate limit) after OpenClaw's own retries, the run continues on `OPENCLAW_FALLBACK_MODEL` (default `google-vertex/gemini-3.6-flash`; listed under the investigator's `model.fallbacks`). Same provider and credentials, so a credential problem fails both and the worker sends its fallback notice. The fallback applies to that run only. The dashboard shows which model(s) answered each incident, and prices fallback calls with `FALLBACK_MODEL_PRICE_*`.
- To switch models, change `OPENCLAW_MODEL`. Another provider needs its plugin, a `models.providers` entry if it isn't bundled, and a check of its data terms before real data.
- Prices (`MODEL_PRICE_*`, `FALLBACK_MODEL_PRICE_*`): 0.75 / 3.75 / 0.075 USD per million tokens (input / output / cached) through 2026-12-31, doubling from 2027-01-01.

### Quality
- **Grounding check:** the `report-capture` plugin keeps the investigator's evidence in memory per session (the incident message from `llm_input`, every tool result from `after_tool_call`). Before the report goes to LINE (`message_sending`), it extracts each commit SHA, file path and dotted table name and looks for it in that evidence (repository-relative paths match the absolute or bundle paths in tracebacks). Anything not found is listed on one line above `Ref:`: "⚠️ Not found in the evidence (verify before acting): …". The list is stored with the incident and shown on the dashboard and in `make feedback-report`. Delivery is never blocked, and nothing is checked when no evidence was captured.
- **Live tests:** `make eval-live` (on the Docker host) triggers the manual-only test DAGs `finance_nightly_jobs` (scenarios `insufficient_evidence`, `prompt_injection`, `huge_log`; its source, commit messages and run ids give no hint of the test, since the investigator reads them) and `investigator_cascade` (one failure, 8 failing checks and 12 blocked builds), waits for each report and scores it: schema, Ref line, grounding, plus scenario checks (says "Insufficient evidence"; ignores injected instructions; finds the real error under 5 MB of log; one investigation of the right root task with the blast radius listed). Each scenario costs one investigation and 2 LINE pushes. `make eval-live SCENARIOS="cascade"` runs a subset.

### Reliability
- **Restarts don't drop or repeat incidents:** with the incident log on, failure callbacks still being grouped and the runs already investigated are kept in `feedback.db` (`dedupe_pending`, `dedupe_flushed`). A restarted worker resumes the open windows (a window that closed while it was down is investigated right away) and still ignores late callbacks for runs it has handled. An investigation already in progress during a restart isn't resumed: OpenClaw still delivers its report, but the feedback card is lost. `GET /healthz` shows `pending` and `investigating`.
- **Alerts (Healthchecks.io):** the worker pings `HEALTHCHECK_URL` every `HEALTHCHECK_INTERVAL_S` (300 s). Missing pings mean the worker, the server or its internet connection is down; a `/fail` ping means the gateway's `/readyz` isn't OK or the last backup failed. `HEALTHCHECK_LINE_URL` gets `/fail` when a LINE push (by the worker, or the gateway's report delivery) fails, and a success ping when one goes through. Healthchecks.io emails on each change. Ping bodies hold refs and error text only.
- **Backups:** once per Bangkok day the worker writes `feedback-YYYY-MM-DD.db` with SQLite `VACUUM INTO` (a consistent copy while it runs) to `FEEDBACK_BACKUP_DIR` (default `backups/` next to the database, i.e. `demo/.state/feedback/backups`) and keeps the newest `FEEDBACK_BACKUP_KEEP` (14; 0 = off). They live on the same disk, so they cover corruption and mistakes, not disk loss.

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
- **Cost tracking:** the report-capture plugin forwards the investigator's token usage per model call (`llm_output`, counts only, no text) to the worker, which links it to the incident through the agent run id from `/hooks/agent`. The dashboard's Costs card shows model tokens (and USD when `MODEL_PRICE_INPUT_PER_MTOK` / `MODEL_PRICE_OUTPUT_PER_MTOK` / `MODEL_PRICE_CACHE_READ_PER_MTOK` are set) for the selected range, LINE's own count of messages against the plan quota this month, and the Databricks job's runs and serverless compute minutes this month (USD for Databricks is on its bill; reading `system.billing` would need extra grants). Each incident's detail shows its tokens and cost.
- **MTTR:** the feedback card has a "✅ Resolved" button (signed postback; the first tap counts). The worker also checks Airflow every `RESOLUTION_POLL_S` (default 300 s, read-only viewer) and resolves an open incident when a later run of its DAG finished with the failed task in state `success`, at that run's end time. MTTR = first failure → resolved; the dashboard shows the median, open incidents, and how each was resolved, and `make feedback-report` prints it.
- **Run pipeline button:** "▶ Run pipeline" on the Databricks card starts the Airflow DAG `DASHBOARD_TRIGGER_DAG` (default `databricks_daily_revenue`), optionally with a test fault, so failures still reach the investigator and LINE. It uses a separate Airflow account `dashboard` (role `user`, password `AIRFLOW_TRIGGER_PASSWORD`) that only the worker holds; the investigator keeps the read-only `viewer` account. It refuses while a run is queued or running, allows one start per 60 s and `DASHBOARD_TRIGGER_DAILY_LIMIT` (default 20) per Bangkok day, and needs the page's own `X-Dashboard-Action` header.
- **Test LINE button:** the dashboard's "🔔 Test LINE" pushes one plain text message to `LINE_TARGET_ID` so you can check delivery on your phone. It needs the page's own `X-Dashboard-Action` header (no cross-site triggering), allows one test per 30 s and `DASHBOARD_TEST_DAILY_LIMIT` (default 10) per Bangkok day, since each test uses LINE push quota.
- **Votes:** one per person per incident; a later tap replaces the earlier one. Taps and replies use LINE's free reply API; each incident uses 2 pushes (report + card).

## Deploy to a Linux server
Needs only Docker with the Compose plugin; Node runs inside a container for the build.
1. Copy the repo and `demo/.env` to the server, for example with `rsync -az --exclude node_modules --exclude 'packages/*/dist' --exclude demo/.state ./ user@server:OpenClaw-Line/`. On later syncs also exclude `demo/.env`, which holds server-only settings.
2. In the server's `demo/.env`, set `AIRFLOW_UID=$(id -u)`. Override `AIRFLOW_HOST_PORT`, `WORKER_HOST_PORT` or `WEBHOOK_HOST_PORT` if those ports are taken.
3. Run `make demo-prepare-docker`, then `docker compose --env-file demo/.env -f demo/docker-compose.yml up -d --build`, then `make demo-tunnel`.
4. Run the named tunnel on **one** machine only. Stop the old host's stack before starting the tunnel on the new one, otherwise Cloudflare splits LINE webhooks and dashboard traffic between them.

All services use `restart: unless-stopped`, so the stack comes back after a reboot. Airflow's history (metadata DB and task logs) lives in the `airflow-home` volume: `make demo-down` keeps it and `make demo-clean` deletes it. To open the Airflow login page from your network, set `AIRFLOW_BIND_ADDRESS` to the server's LAN IP and `AIRFLOW_PUBLIC_URL=http://<ip>:8080`, and use a strong `AIRFLOW_ADMIN_PASSWORD`. The other host ports bind to `127.0.0.1`; reach the local dashboard with `ssh -L 8090:localhost:<WORKER_HOST_PORT> user@server`.

## Databricks pipeline (GCP)
`databricks/` is a Databricks Asset Bundle with a real version of the revenue pipeline, job `daily_revenue_lakehouse` on serverless compute: `land_stripe_events` (synthetic Stripe-style events into a Unity Catalog volume) → `bronze_ingest` (Auto Loader, fixed schema, unknown fields to `_rescued_data`) → `silver_transactions` (fails on schema drift or null amounts, dedupes, signs refunds) → `gold_daily_revenue` (per-day totals, refuses empty or negative days). It has no schedule; Airflow triggers it.

One-time setup by hand: catalog `lakehouse` with schemas `bronze`, `silver`, `gold`. Workspaces with Default Storage create a catalog on it only in the UI (Catalog → + → Create a catalog → Use default storage); otherwise give it a managed location on your own bucket. The bundle creates the managed volumes `lakehouse.bronze.landing` and `lakehouse.bronze.checkpoints`.

- `databricks auth login --host <workspace URL> --profile openclaw`, then `make lakehouse-deploy`.
- `make lakehouse-run` runs it once; `FAULT=schema_drift|null_amounts|empty` simulates upstream breakage for incident tests.
- Each run lands its own file and silver reads only that file (via task values), so a rerun after a bad delivery is clean. Silver and gold rewrite their date partition, so reruns are idempotent.

Airflow side: DAG `databricks_daily_revenue` (manual for now; the `inject_fault` param passes through) starts the job with `DatabricksRunNowOperator`. The Airflow image adds the Databricks provider (`demo/airflow.Dockerfile`). Two service principals, both granted in the bundle: `DATABRICKS_RUNNER_*` (`CAN_MANAGE_RUN`) become Airflow's `databricks_default` connection; `DATABRICKS_READER_*` (`CAN_VIEW`) go to the gateway for `get_databricks_run_error` and to the worker, whose dashboard shows the job's latest runs and task states (`/api/pipeline`, cached 20 s). On failure, `notify_openclaw` adds the Databricks run id from XCom, and the investigator reads the failed Databricks task's error and trace instead of the Airflow log, which only links to the run.

Blast radius for Databricks incidents comes from Unity Catalog lineage: `get_databricks_run_error` maps each task to its notebook id (the reader has `CAN_READ` on the bundle's files via the bundle's top-level `permissions`), reads table lineage for the job's catalog (`catalog` job parameter, or `DATABRICKS_LINEAGE_CATALOGS`), and lists the tables each task writes, the tables this run left stale, everything further downstream, and other consumers (other jobs, dashboards, queries). The reader needs only `BROWSE` on the catalog (metadata and lineage, no data): `databricks grants update catalog <catalog> --json '{"changes":[{"principal":"<reader application id>","add":["BROWSE"]}]}'`. Lineage reflects past successful runs, so a brand-new table appears after its first successful write.

## Status

| Area | State |
|---|---|
| Phase 1 (callback, log tool, report to chat) | Done; delivered to LINE instead of Slack |
| Phase 2 (git diff, dbt lineage, facts vs. hypotheses, blast radius) | Done; verified with local git and the fixture manifest |
| Phase 3 (feedback buttons, feedback log, prompt-version tracking) | Done; triage time and resolution time (MTTR) are measured |
| Dashboard | Live view with Cloudflare Access for public hostnames |
| Deployment | Linux server with Docker; named Cloudflare tunnel for the LINE webhook, dashboard and Airflow UI |
| Reliability | Backup model, Healthchecks.io alerts (live since 2026-10-17), dedupe state that survives restarts, daily feedback-log backups |
| Quality | Grounding check on every report; live tests for insufficient evidence, prompt injection, huge logs and a big cascade (`make eval-live`) |

Open items:
- Real systems: production DAGs and a real Airflow viewer account (Airflow 2 path untested live), a production dbt manifest sync, the "data catalog" lineage option. (Git evidence already comes from GitHub with a read-only token, and Databricks lineage from Unity Catalog.)
- Data residency: see [docs/data-residency.md](docs/data-residency.md). Retention, value-free pipeline errors and Thai PII masking are in place. Gemini API and Healthchecks.io terms were checked on 2026-10-17. The model runs on Vertex AI in the EU multi-region (no training on the evidence); record that transfer and the Databricks region as a cross-border transfer (compute and, since 2026-10-17, storage are in Mumbai) or move it.
- Retention: `REPORT_RETENTION_DAYS` (default 180) for incident report text; Airflow DAG `housekeeping` (daily 03:00 Bangkok) deletes task logs after `AIRFLOW_LOG_RETENTION_DAYS` (30); the Databricks job's `housekeeping` task deletes landing folders after `landing_retention_days` (90) and runs `VACUUM` (7 days). Airflow metadata older than `AIRFLOW_METADATA_RETENTION_DAYS` (90) is removed with `make airflow-db-clean`, run daily at 03:15 by the server's crontab (see the Makefile for the line). Airflow 3 tasks can't reach the metadata DB, so it isn't part of the DAG.

## Production notes
- Set `GIT_PROVIDER=github` (or `gitlab`) with `GITHUB_REPO`, `GIT_REF` and a read-only `GITHUB_TOKEN` (or `GITLAB_PROJECT`/`GITLAB_TOKEN`) in the environment; the default is the seeded demo repo. `GIT_PATH_PREFIXES` (e.g. `airflow/`) is tried when a path from a traceback is not in the repository, and the tool reports "File not found in the repository" rather than "no commits" when no candidate exists.
- Mount the production `manifest.json` from your dbt runs. Use `lineage_map.json` to map Airflow task ids to dbt nodes when the names differ.
- Outside docker, install the channel with `openclaw plugins install @openclaw/line` (the Vertex provider is bundled).
- Install the investigator plugin with `openclaw plugins install <staged dir> --force` (stage it with `node scripts/stage-plugin.mjs <dir>`), as the demo image does. Non-interactive installs also need `--accept-capabilities`.
- Keep `tools.codeMode: false` and `tools.toolSearch: false`. Code Mode auto-enables for some preferred models and replaces the tools with a model-written JavaScript `exec` running in `node:vm`, which is not a security boundary. Tool Search hides the tool schemas behind a generic `tool_call`. After each OpenClaw upgrade, check that a run still calls the tools directly.
- Don't set `tools.profile` on the investigator agent. Profiles are applied before `allow` and none of them include plugin tools, so runs fail with "No callable tools remain…".
- Keep the Gateway port private. Its token is an operator credential.
- Publish only the worker's public listener (`PUBLIC_PORT`, `/line/webhook`) through a named Cloudflare tunnel or your ingress. Keep the internal port (Airflow callbacks, report capture) private.
- **Airflow on another host:** set `INGEST_PORT` (compose publishes it as `INGEST_BIND_ADDRESS:INGEST_HOST_PORT`, default 8094). That listener serves only the HMAC-signed `POST /v1/airflow/failure`; the dashboard API and report routes stay on the internal port. Restrict it to the Airflow host: Docker-published ports bypass ufw's INPUT rules, so use the `DOCKER-USER` chain. An Airflow on the same server (`airflow/production/docker-compose.yml`, `make prod-airflow-up`) needs no LAN port: it reaches `ingestion-worker:8094` over the demo network. Point the investigator at that Airflow with `AIRFLOW_BASE_URL` and a Viewer account (`AIRFLOW_VIEWER_USERNAME`/`AIRFLOW_VIEWER_PASSWORD`), and set `COMPOSE_PROFILES=` so the demo Airflow container doesn't start.
- **Attach the callback without editing DAGs:** copy `airflow/plugins/openclaw_callback.py` into that Airflow's plugins folder and `airflow/production/airflow_local_settings.py` into its config folder. The cluster policy appends `notify_openclaw` to every task's `on_failure_callback` and keeps existing callbacks. Set `OPENCLAW_INGEST_URL` and `OPENCLAW_WEBHOOK_SECRET` on its scheduler and workers.
- **Model for real data:** Gemini on Vertex AI (`google-vertex/…`, service account key, `GOOGLE_CLOUD_PROJECT`/`GOOGLE_CLOUD_LOCATION`) is covered by the Google Cloud no-training terms. See `demo/.env.example` and [docs/data-residency.md](docs/data-residency.md) for the location choice.
- The feedback log is a single-writer SQLite file: run one worker replica. The daily backups stay on the same disk; copy `demo/.state/feedback/backups` off the server if you need protection from disk loss.
