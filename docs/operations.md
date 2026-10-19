# Operations runbook

How the deployed system runs, where things live, and the problems already hit and fixed. Times are Bangkok time. No secrets here: they live only in the server's `demo/.env` (mode 600).

## Where it runs

| Piece | Where |
|---|---|
| Docker Compose stack (`airflow`, `ingestion-worker`, `openclaw-gateway`, `tunnel`) | Ubuntu server `192.168.1.49`, user `prapon`, `~/OpenClaw-Line`. That directory is an **rsync copy** of the repo, not a git checkout. Server time zone is Asia/Bangkok. |
| Databricks workspace | `https://8259554454724321.1.gcp.databricks.com` (GCP, Unity Catalog metastore `asia-south1`, Mumbai), since 2026-10-17. The first workspace (`8259562386951628.8`) was deleted. CLI profile on the Mac: `openclaw` (`make lakehouse-deploy LAKEHOUSE_PROFILE=<profile>` for another). |
| Data | Catalog `lakehouse` on the workspace's Default Storage (Databricks-managed, same region as the workspace), schemas `bronze`, `silver`, `gold`; managed volumes `lakehouse.bronze.landing` (raw files) and `lakehouse.bronze.checkpoints`, both from the bundle. |
| Public hostnames (Cloudflare tunnel `582a2422-5a02-49de-94b4-a4fe7025c5be`, named "dashboard" in Cloudflare, Free plan) | `openclaw-feedback.praponb.com` → worker `:8091` (LINE webhook only); `dashboard.praponb.com` → worker `:8092` (Access app and worker JWT check); `airflow.praponb.com` → `airflow:8080` (demo Airflow; Access app with Protect with Access); `airflow-prod.praponb.com` → `airflow-prod:8080` (production Airflow, since 2026-10-19; its own Access app, AUD starts `14e1d63d`, Protect with Access on). Routes live under **Networking → Tunnels → dashboard → Routes** (**+ Add route → Published application**), not "Hostname routes". In the route's Access settings, a pasted AUD tag only counts after pressing Enter (it becomes a chip). |
| Mac | Development only. Its Compose stack is stopped. **Never start the Mac's tunnel while the server's runs**: Cloudflare would split webhook and dashboard traffic. The Mac's own `cloudflared` runs `batch2stream`, a different project. |

## Deploying a change

```bash
# from the Mac, on the branch to deploy
rsync -az --exclude node_modules --exclude 'packages/*/dist' --exclude demo/.state \
  --exclude demo/.env --exclude 'demo/.env.bak-*' --exclude databricks/.databricks ./ prapon@192.168.1.49:OpenClaw-Line/
ssh prapon@192.168.1.49 'cd OpenClaw-Line && make demo-prepare-docker && \
  docker compose --env-file demo/.env -f demo/docker-compose.yml up -d --build ingestion-worker openclaw-gateway'
make lakehouse-deploy   # Databricks job, from the Mac
```

- **Never overwrite the server's `demo/.env`**, and check that no investigation is running first: `curl localhost:8093/healthz` on the server (the worker's port 8080) should show `"investigating": 0`. Incidents still being grouped (`pending`) are saved and resume after the restart; an investigation in progress is not (its report reaches LINE, but the feedback card is lost).
- **The worker image copies a pre-built `dist/`,** so the server must run `make demo-prepare-docker` (Node runs in a container) before `--build`.
- **Before a change to the incident log schema,** make a fresh copy even though daily backups exist: `docker compose --env-file demo/.env -f demo/docker-compose.yml exec ingestion-worker node -e 'new (require("node:sqlite").DatabaseSync)("/data/feedback.db").exec("VACUUM INTO \x27/data/backups/pre-change.db\x27")'`. Delete `pre-change.db` once the deploy works: the daily cleanup only removes `feedback-YYYY-MM-DD.db` files.
- **New secrets go into the server's `demo/.env`.** The Mac's copy is never synced (rsync excludes it).
- **After a gateway restart,** Docker shows "health: starting" for about 3 minutes (see Pitfalls). Check readiness directly: `docker compose … exec openclaw-gateway curl -s localhost:18789/readyz`.
- **Airflow DAGs and plugins are bind-mounted;** rsync is enough for them. Restart Airflow only when its environment changes.

## Settings in the server's `demo/.env` (names only)

| Group | Variables |
|---|---|
| Model | `MOONSHOT_API_KEY`, `MOONSHOT_BASE_URL` (international), `OPENCLAW_MODEL=moonshot/kimi-k3`, `MODEL_PRICE_INPUT_PER_MTOK=3.00`, `MODEL_PRICE_OUTPUT_PER_MTOK=15.00`, `MODEL_PRICE_CACHE_READ_PER_MTOK=0.30` (official Kimi K3 prices) |
| Model on Vertex AI (since 2026-10-18) | `OPENCLAW_MODEL=google-vertex/gemini-3.7-flash`, `OPENCLAW_FALLBACK_MODEL=google-vertex/gemini-3.6-flash`, `GOOGLE_APPLICATION_CREDENTIALS=/run/vertex/openclaw-vertex.json` (key in `demo/.state/vertex/`, mode 600), `GOOGLE_CLOUD_PROJECT=gen-lang-client-0143309127`, `GOOGLE_CLOUD_LOCATION=eu` (EU multi-region, the owner's choice; Gemini 3.x is not in `asia-southeast1`, see Pitfalls); prices 0.75 / 3.75 / 0.075. The previous Kimi and AI Studio lines are in `demo/.env.bak-vertex`. |
| Backup model | `GEMINI_API_KEY` (Google AI Studio, billing on); `OPENCLAW_FALLBACK_MODEL` defaults to `google/gemini-3.8-flash`; `FALLBACK_MODEL_PRICE_INPUT_PER_MTOK=0.75`, `FALLBACK_MODEL_PRICE_OUTPUT_PER_MTOK=3.75`, `FALLBACK_MODEL_PRICE_CACHE_READ_PER_MTOK=0.075` (paid tier through 2026-12-31; **double from 2027-01-01**: 1.50 / 7.50 / 0.15) |
| Alerts | `HEALTHCHECK_URL` (heartbeat check), `HEALTHCHECK_LINE_URL` (LINE check); Healthchecks.io ping URLs |
| LINE | `LINE_CHANNEL_ACCESS_TOKEN`, `LINE_CHANNEL_SECRET`, `LINE_TARGET_ID` (a personal chat, `U…`) |
| Airflow | `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_VIEWER_PASSWORD` (investigator, read-only), `AIRFLOW_TRIGGER_PASSWORD` (dashboard Run pipeline account), `AIRFLOW_UID=1000`, `AIRFLOW_BIND_ADDRESS`, `AIRFLOW_PUBLIC_URL=https://airflow.praponb.com` |
| Databricks | `DATABRICKS_HOST`, `DATABRICKS_JOB_ID=74688628083610`, `DATABRICKS_RUNNER_CLIENT_ID/SECRET` (Airflow only), `DATABRICKS_READER_CLIENT_ID/SECRET` (gateway and worker). OAuth secrets expire **2028-10-16**. |
| Git evidence | `GIT_PROVIDER=github`, `GITHUB_REPO=praponb/OpenClaw-Line`, `GIT_REF=main`, `GIT_PATH_PREFIXES=airflow/`, `GITHUB_TOKEN` (fine-grained, this repo only, Contents read-only) |
| Cloudflare / dashboard | `CLOUDFLARE_TUNNEL_TOKEN`, `CF_ACCESS_TEAM_DOMAIN`, `CF_ACCESS_AUD`, `DASHBOARD_ALLOWED_EMAILS` |
| Other | `WEBHOOK_SECRET`, `OPENCLAW_GATEWAY_TOKEN`, `OPENCLAW_HOOK_TOKEN`, `WORKER_HOST_PORT=8093` (8090 is taken by another app) |

## Identities and permissions

| Identity | Can do |
|---|---|
| GCP service account `openclaw-vertex` (project `gen-lang-client-0143309127`) | `roles/aiplatform.user` only: calls Vertex AI models. JSON key on the server only. |
| Databricks `sp-airflow-runner` (`5373a47d…`) | `CAN_MANAGE_RUN` on the job. Granted in the bundle, not the UI. |
| Databricks `sp-openclaw-reader` (`d70ea6d2…`) | `CAN_VIEW` on the job plus `CAN_READ` on the bundle files (top-level bundle `permissions`), and `BROWSE` on catalog `lakehouse` (granted by CLI; it's the only grant on the catalog besides the owner, since the UI's default `BROWSE` for `account users` was removed on 2026-10-17). Data and volume reads return 403. Entitlements: the reader has `workspace-access` (needed for `jobs/runs/get-output` and the workspace API; `workspace-consume` returns 403 "does not have access to Databricks Workspace" there), the runner only `workspace-consume`. Without any entitlement every API call returns 403. |
| Airflow `investigator` (viewer) | Reads logs and runs; a run-create attempt returns 403. |
| Airflow `dashboard` (user) | Starts DAG runs; held by the worker only. |
| GitHub token | Read-only on this repo; write probes return 403. |

## Everyday operations

| Task | How |
|---|---|
| Run the pipeline | Dashboard → Databricks card → **▶ Run pipeline** (normal run or a test fault). No schedule by choice. CLI alternative without Airflow or LINE: `make lakehouse-run [FAULT=schema_drift]`. |
| Check LINE delivery | Dashboard → **🔔 Test LINE**. Uses one push from the monthly quota (300 on the current plan). |
| Feedback review | `make feedback-report` (prompt versions, votes, triage time, MTTR, grounding check). |
| Live investigator tests | On the server: `make eval-live` (4 scenarios, about $0.11 on Vertex and 8 LINE pushes) or `make eval-live SCENARIOS="huge_log"`. Prints PASS/FAIL per check, and WARN for a `Fact:` line that reads like a deduction ("would", "likely", "suggests"…; AGENTS.md rule 3). Warnings don't fail the run: read the line and decide. Last full run 2026-10-17 17:43: 30/30; `insufficient_evidence` alone 2026-10-18 06:05: 6/6, no WARN. A Databricks check: Run pipeline with fault `schema_drift` (one investigation). |
| Test the backup model | On the server, with `"investigating": 0`: `OPENCLAW_MODEL=google-vertex/gemini-9.9-flash COMPOSE_PROFILES=demo docker compose … up -d openclaw-gateway` (a shell variable overrides `demo/.env`; the file is not touched; a model that doesn't exist fails with `model_not_found`), wait for `/readyz`, `make eval-live SCENARIOS="prompt_injection"`, then `docker compose … up -d openclaw-gateway` without the variable and check `printenv OPENCLAW_MODEL` in the container. The dashboard shows `google-vertex/gemini-3.6-flash` for that incident. (A bad credential would fail both models: they share the Vertex provider.) |
| Change Airflow passwords | The simple auth manager has no change-password page; the passwords live in the **server's** `demo/.env`. Demo: edit `AIRFLOW_*_PASSWORD`, then `make state` (rewrites `passwords.json`), `docker compose … restart airflow`, and `docker compose … up -d ingestion-worker openclaw-gateway` (both read the viewer password from the environment). Production admin: edit `PROD_AIRFLOW_ADMIN_PASSWORD`, then `make prod-airflow-state` and `docker compose --env-file demo/.env -f airflow/production/docker-compose.yml restart api-server`. `AIRFLOW_VIEWER_PASSWORD` is shared, so a new viewer password needs both. Avoid spaces, quotes, backslashes, backticks, `$` and `#` (they break `demo/.env` or `passwords.json`). Check that `/auth/token` returns 201 for the new password and 401 for the old one. |
| Remove a deleted DAG's history | `docker compose … exec -T airflow airflow db clean --clean-before-timestamp "$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" --dag-ids <dag_id> --dry-run`, then again with `--skip-archive --yes` instead of `--dry-run`. Without `--dag-ids` this deletes every DAG's history. Errors for `deadline`, `asset_event` and `task_reschedule` are expected (skipped). |
| Resolve an incident | ✅ Resolved on the LINE card, or automatically when the DAG's next run succeeds (checked every 5 min). |
| An alert email arrives | Heartbeat **down** with no ping: the worker or server is off (`ssh` in, `docker compose ps`). Heartbeat **down** after a `/fail`: the email shows the reason (gateway not ready, backup failed). **LINE** check down: the email shows the LINE error (expired token, quota used up); it goes up again with the next successful push, or use 🔔 Test LINE. |
| Restore the incident log | Stop the worker, copy `demo/.state/feedback/backups/feedback-<day>.db` over `demo/.state/feedback/feedback.db` (delete `feedback.db-wal` and `feedback.db-shm`), start the worker. |
| Costs | Dashboard Costs card. About $0.06–0.10 per investigation at Kimi K3 prices; Databricks compute minutes this month; LINE messages against the quota. |

## Scheduled jobs

| When | What |
|---|---|
| 03:00 daily | Airflow DAG `housekeeping`: task logs older than 30 days. |
| 03:15 daily | Server crontab (user `prapon`): `make -s airflow-db-clean` (Airflow metadata older than 90 days); output in `demo/.state/airflow-db-clean.log`. The line sets `PATH=/usr/local/bin:…` because docker lives there. |
| Every worker day | Report text older than 180 days removed (`REPORT_RETENTION_DAYS`). |
| Once per Bangkok day (checked hourly and at start) | Worker copies `feedback.db` to `demo/.state/feedback/backups/feedback-YYYY-MM-DD.db`; keeps 14. |
| 08:00 daily | Server crontab: `make -s daily-summary` pushes a short summary of the last 24 h to LINE (incidents, delivered/failed, triage time, votes, model cost, and the incidents that need a look: not delivered, 👎/⚠️, claims not found in the evidence). One push a day (about 30 of the 300 free monthly pushes). Output in `demo/.state/daily-summary.log`. Preview without sending: `docker compose … exec -T ingestion-worker node dist/report.js --days 1 --summary`. |
| 03:25 daily | Server crontab: `make -s prod-airflow-db-clean` (production Airflow metadata older than 90 days). |
| Every 10 min | Server crontab: `git -C ~/prod-dags pull -q --ff-only` (production DAGs from `main`). |
| Every 5 min | Worker heartbeat to Healthchecks.io (gateway `/readyz` and last backup checked). |
| After every Databricks run | Job task `housekeeping`: landing folders older than 90 days, `VACUUM` on bronze, silver and gold (about 70 s). |

## Pitfalls and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Error 1033 after the Access login | The hostname's route was on a tunnel with no running connector. | Put the route on tunnel `582a2422…`. |
| Airflow tasks hang in "Pre Execute" | Tasks call the Execution API at `AIRFLOW__API__BASE_URL`; behind Cloudflare Access that returns a login redirect. | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8080/execution/`. |
| Gateway image build fails on plugin install | Unpinned `@openclaw/moonshot-provider` pulled a release needing a newer plugin API. | Pin official plugins to the gateway version (`OPENCLAW_PLUGIN_VERSION`, now 2026.10.2). |
| `run_after` has no `in_timezone` | Manual runs in Airflow 3 may have no logical date, and `run_after` is a plain datetime. | `+ macros.timedelta(hours=7)`. |
| `airflow db clean` fails inside a DAG | Airflow 3 tasks can't reach the metadata DB. | `make airflow-db-clean` from cron. |
| `llm_output` hook blocked | OpenClaw guards it for non-bundled plugins. | `plugins.entries.report-capture.hooks.allowConversationAccess: true`. |
| Token usage not linked to incidents | `/hooks/agent` returns a job id; `llm_output` runs in session `…:cron:<job>:run:<run>`, delivery in `…:main`. | Link through the `Ref: INC-…` in the report-writing model call. |
| Investigator said "code unchanged" | The git tool read the seeded demo repo and got "no commits". | GitHub provider plus a "File not found in the repository" result. |
| Serverless runs took ~12 min | `performance_target: STANDARD` waits about 5 min for compute per task. | `PERFORMANCE_OPTIMIZED` (about 2 min per run). |
| Serverless retried failed tasks | Auto-optimization. | `disable_auto_optimization: true` and `max_retries: 0` on each task. |
| Databricks run ids disappeared from evidence | The card-number rule masked Luhn-valid 15-digit ids. | Card rule needs a real brand prefix and length. |
| Unity Catalog table listings empty for the reader | Metadata-only access needs `include_browse=true`. | Listings pass `include_browse=true`; `BROWSE` alone is enough. |
| Traceback "line 33" didn't match the file | Notebook frames count from the top of the cell. | The tool maps them to file lines ("file line: … line 47"). |
| OpenClaw agent saw no tools | `tools.profile` filters before `allow`; plugins loaded via `load.paths` lack an install record. | No profile; install plugins in the image. Keep Code Mode and Tool Search off. |
| Bundled plugin broke `node:sqlite` | tsup strips `node:` prefixes. | Bundle smoke test or live run. |
| Gateway "health: starting" for ~3 min after a restart | The image's healthcheck runs every 180 s; its first runs hit OpenClaw's startup doctor ("state … undergoing offline maintenance"). | Wait for the next run, or check `/readyz` inside the container. |
| New keys had no effect on the server | They were added to the Mac's `demo/.env`, which deploys never copy. | Add secrets to the server's `demo/.env`. |
| Grounding check flagged `subprocess.run` as a table | Every dotted name counted as a table, including code names from tracebacks. | Table names are read from the Blast Radius section only. |
| Investigator said a test DAG "not found in the repository" | The git tool reads `GIT_REF=main`; the DAG was only on an unmerged branch. | Expected until the branch is merged. |
| Dashboard: "Databricks OAuth token request failed with HTTP 400" | The workspace was deleted (2026-10-17). | Move to another workspace: catalog, service principals, bundle deploy, server `.env` (see changelog 2026-10-17). |
| Creating catalog `lakehouse` failed: "Metastore storage root URL does not exist" | The account uses Default Storage; the API and CLI can't create a catalog on it. | Create the catalog in the UI (Use default storage), then remove the `BROWSE` it grants to `account users`. |
| Every service principal API call returned 403 "This API is disabled for users without …" | The new workspace's `users` group grants no entitlements. | Give the SP an entitlement. |
| Investigator got 403 "does not have access to Databricks Workspace" for a failed run | `workspace-consume` covers job and run reads but not `jobs/runs/get-output` or the workspace API. | Reader gets `workspace-access`. Test SPs with the exact calls the tools make. |
| Every investigation fails with "This model was not found" on Vertex; the worker sends only its fallback notice | Gemini 3.x Flash is not served in `asia-southeast1` (404 "Publisher model … was not found or your project does not have access to it"); it answers only on `global`, `us` and `eu` (checked 2026-10-18). | `GOOGLE_CLOUD_LOCATION=eu` (or `us`/`global`), then recreate the gateway. Probe a model first: `curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" https://aiplatform.eu.rep.googleapis.com/v1/projects/<project>/locations/eu/publishers/google/models/<model>:generateContent -d '{"contents":[{"role":"user","parts":[{"text":"ok"}]}]}'`. In zsh write `${m}:generateContent`: `$m:g…` is read as a modifier. |
| Doctor at gateway start: "Unknown model: moonshot/kimi-k3" (2026.10.2) | Doctor's tool-schema check runs before the Moonshot catalog has loaded. | Harmless: `openclaw models list --provider moonshot` lists `kimi-k3` once started, and runs use it. |
| Doctor at gateway start: "Remember across conversations is effectively enabled" (2026.10.2) | Unset `memory.search.rememberAcrossConversations` counts as on. | Set to `false` in `openclaw.json5`: each incident stands on its own evidence. |
| `insufficient_evidence` live test "failed" although the report was right | The test DAG's diff (docstring, `kill -9 $$`) and the `eval__…` run id told the investigator the failure was intentional. | Neutral test DAG `finance_nightly_jobs`, the failing client installed outside the repository, `manual__` run ids; add and delete files in separate commits (a rename's patch shows the old code). |
| Report wrote "insufficient evidence" under Hypothesis | The prompt didn't say that "what failed but not why" counts as undetermined. | `AGENTS.md` rule (prompt version `1f58cc72c93c`). |

## Dates to remember

| Date | What |
|---|---|
| 2027-01-01 | Vertex Gemini Flash prices double, for **both** models (primary `gemini-3.7-flash` and backup `gemini-3.6-flash`). In the server's `demo/.env` set `MODEL_PRICE_INPUT_PER_MTOK=1.50`, `MODEL_PRICE_OUTPUT_PER_MTOK=7.50`, `MODEL_PRICE_CACHE_READ_PER_MTOK=0.15` and the same three `FALLBACK_MODEL_PRICE_*` values, then recreate the worker (`… up -d ingestion-worker`, with `"investigating": 0`). Check the new prices on the Vertex AI pricing page first. |
| 2028-10-16 | Databricks service principal OAuth secrets expire (created 2026-10-17 for workspace `8259554454724321.1`). |

## Production Airflow

Decided 2026-10-18: production Airflow runs **on the home server**, as its own Compose project `openclaw-line-prod-airflow` (`airflow/production/docker-compose.yml`): Airflow 3.3.2 (newest), Postgres 16, LocalExecutor, separate api-server, scheduler, DAG processor and triggerer. The demo Airflow stays for `make eval-live`.

| Piece | Setting |
|---|---|
| DAGs, plugin, cluster policy | Read-only clone `~/prod-dags` of `https://github.com/praponb/OpenClaw-Line.git` (public, no credentials), `main`; mounted read-only (`airflow/dags`, `airflow/plugins`, `airflow/production` as the config folder). Crontab: `*/10 * * * * git -C $HOME/prod-dags pull -q --ff-only`. Only merged code runs in production. |
| Callbacks | `airflow_local_settings.py` appends `notify_openclaw` to every task that lacks it (matched by name: the plugin manager imports the plugin twice) (DAGs tagged `no-openclaw`, e.g. `housekeeping`, are skipped). They go to `http://ingestion-worker:8094/v1/airflow/failure`, the failure-only listener, over the demo stack's network `openclaw-line-demo_default`: no new LAN port, no ufw rule. |
| Accounts | Simple auth manager: `admin` (`PROD_AIRFLOW_ADMIN_PASSWORD`) and `investigator` (Viewer, same `AIRFLOW_VIEWER_PASSWORD` as the demo). No `dashboard` (user) account. |
| UI | Only `https://airflow-prod.praponb.com` (Cloudflare Access, only the owner's email; `PROD_AIRFLOW_PUBLIC_URL`). The host port binds to `127.0.0.1` (`PROD_AIRFLOW_BIND_ADDRESS=127.0.0.1`, `PROD_AIRFLOW_HOST_PORT`), as does the demo Airflow's (`AIRFLOW_BIND_ADDRESS=127.0.0.1`), so neither login page is on the LAN; without Cloudflare use `ssh -L 8081:localhost:8081 prapon@192.168.1.49`. New DAGs start paused. |
| Secrets in `demo/.env` | `PROD_AIRFLOW_ADMIN_PASSWORD`, `PROD_AIRFLOW_DB_PASSWORD`, `PROD_AIRFLOW_JWT_SECRET`, `PROD_AIRFLOW_FERNET_KEY`; Databricks runner as for the demo. |
| Retention | Task logs: `housekeeping` DAG (unpause it). Metadata: crontab `25 3 * * *` `make -s prod-airflow-db-clean`. |

DAG changes go live on the next `git pull` (the DAG processor re-parses). **A change to `airflow_local_settings.py` or the plugin needs a restart** (Airflow loads them once per process): `docker compose --env-file demo/.env -f airflow/production/docker-compose.yml restart scheduler dag-processor`.

Start or update: `make prod-airflow-up` (the demo stack must be up: shared network). Logs: `docker compose --env-file demo/.env -f airflow/production/docker-compose.yml logs -f scheduler`.

**Point the investigator at it (done 2026-10-18 15:44):** in `demo/.env` set `AIRFLOW_BASE_URL=http://airflow-prod:8080` and comment out `AIRFLOW_TRIGGER_PASSWORD` (Run pipeline button off), then recreate the worker and gateway. Checks: the viewer gets a token; creating a run with it returns 403; an unsigned POST from the scheduler container to `ingestion-worker:8094` returns 401 and `/api/dashboard` there 404; a manual `daily_revenue_pipeline` run in production (fails on purpose) brings one report to LINE.

**Live tests:** `make eval-live` sees `AIRFLOW_BASE_URL` in `demo/.env` and runs through `demo/with_demo_airflow.sh`: it recreates the worker and gateway reading the demo Airflow (`http://airflow:8080`), runs the tests, and recreates them from `demo/.env` again, also after a failure or Ctrl-C. Each restart waits for `"investigating": 0`. A production failure during the run would be investigated against the demo Airflow, so run it when production is quiet.

**Another Airflow host instead:** set `INGEST_BIND_ADDRESS` to the server's LAN IP (published port `INGEST_HOST_PORT`, default 8094) and point that Airflow's `OPENCLAW_INGEST_URL` at it. Docker-published ports bypass ufw's INPUT rules, so restrict them in the `DOCKER-USER` chain (or bind to an interface only that host reaches).

Rollback: comment out `AIRFLOW_BASE_URL` (back to the demo Airflow) and recreate the worker and gateway; stop production with `docker compose … -f airflow/production/docker-compose.yml stop`. The callback never fails a task.

## Before connecting real production data

See [data-residency.md](data-residency.md):
- Gemini API and Healthchecks.io terms checked 2026-10-17 (see data-residency.md): list Google as a processor with no fixed region. The Gemini key's project must keep billing on (free tier allows training).
- A written no-training agreement from Moonshot, or switch the model provider. Done 2026-10-18: Vertex AI in the EU multi-region (no-training terms); record the EU transfer (data-residency.md finding 7).
- Record the Databricks region (compute and storage in Mumbai since 2026-10-17) as a cross-border transfer, or move it (Singapore workspace, or a Bangkok bucket as the catalog's managed location).
- Send reports to a LINE group and add teammates to Access when others join.

## Open items

- OpenClaw 2026.10.3 and 2026.10.4 are available (pinned 2026.10.2). Upgrade: bump `OPENCLAW_IMAGE` / `OPENCLAW_PLUGIN_VERSION` in `demo/openclaw.Dockerfile` and the `openclaw` devDependency, tag the running image for rollback, then check Doctor's output, Code Mode, Tool Search, tool visibility and `make eval-live`.
