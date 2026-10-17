# Operations runbook

How the deployed system runs, where things live, and the problems already hit and fixed. Times are Bangkok time. No secrets here: they live only in the server's `demo/.env` (mode 600).

## Where it runs

| Piece | Where |
|---|---|
| Docker Compose stack (`airflow`, `ingestion-worker`, `openclaw-gateway`, `tunnel`) | Ubuntu server `192.168.1.49`, user `prapon`, `~/OpenClaw-Line`. That directory is an **rsync copy** of the repo, not a git checkout. Server time zone is Asia/Bangkok. |
| Databricks workspace | `https://8259554454724321.1.gcp.databricks.com` (GCP, Unity Catalog metastore `asia-south1`, Mumbai), since 2026-10-17. The first workspace (`8259562386951628.8`) was deleted. CLI profile on the Mac: `openclaw` (`make lakehouse-deploy LAKEHOUSE_PROFILE=<profile>` for another). |
| Data | Catalog `lakehouse` on the workspace's Default Storage (Databricks-managed, same region as the workspace), schemas `bronze`, `silver`, `gold`; managed volumes `lakehouse.bronze.landing` (raw files) and `lakehouse.bronze.checkpoints`, both from the bundle. |
| Public hostnames (Cloudflare tunnel `582a2422-5a02-49de-94b4-a4fe7025c5be`, named "dashboard" in Cloudflare, Free plan) | `openclaw-feedback.praponb.com` → worker `:8091` (LINE webhook only); `dashboard.praponb.com` → worker `:8092` (Access app and worker JWT check); `airflow.praponb.com` → `airflow:8080` (Access app with Protect with Access). Routes live under **Zero Trust → Networks → Tunnels & Mesh → dashboard → Published application routes**, not "Hostname routes". |
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
| Backup model | `GEMINI_API_KEY` (Google AI Studio, billing on); `OPENCLAW_FALLBACK_MODEL` defaults to `google/gemini-3.5-flash` (served as Gemini 3.6 Flash); `FALLBACK_MODEL_PRICE_INPUT_PER_MTOK=0.75`, `FALLBACK_MODEL_PRICE_OUTPUT_PER_MTOK=3.75`, `FALLBACK_MODEL_PRICE_CACHE_READ_PER_MTOK=0.075` (paid tier through 2026-12-31; **double from 2027-01-01**: 1.50 / 7.50 / 0.15) |
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
| Databricks `sp-airflow-runner` (`5373a47d…`) | `CAN_MANAGE_RUN` on the job. Granted in the bundle, not the UI. |
| Databricks `sp-openclaw-reader` (`d70ea6d2…`) | `CAN_VIEW` on the job plus `CAN_READ` on the bundle files (top-level bundle `permissions`), and `BROWSE` on catalog `lakehouse` (granted by CLI). Data and volume reads return 403. Both SPs have only the `workspace-consume` entitlement: without one, every API call returns 403 ("This API is disabled for users without …"). |
| Airflow `investigator` (viewer) | Reads logs and runs; a run-create attempt returns 403. |
| Airflow `dashboard` (user) | Starts DAG runs; held by the worker only. |
| GitHub token | Read-only on this repo; write probes return 403. |

## Everyday operations

| Task | How |
|---|---|
| Run the pipeline | Dashboard → Databricks card → **▶ Run pipeline** (normal run or a test fault). No schedule by choice. CLI alternative without Airflow or LINE: `make lakehouse-run [FAULT=schema_drift]`. |
| Check LINE delivery | Dashboard → **🔔 Test LINE**. Uses one push from the monthly quota (300 on the current plan). |
| Feedback review | `make feedback-report` (prompt versions, votes, triage time, MTTR, grounding check). |
| Live investigator tests | On the server: `make eval-live` (4 scenarios, about $0.40 and 8 LINE pushes) or `make eval-live SCENARIOS="huge_log"`. Prints PASS/FAIL per check. |
| Resolve an incident | ✅ Resolved on the LINE card, or automatically when the DAG's next run succeeds (checked every 5 min). |
| An alert email arrives | Heartbeat **down** with no ping: the worker or server is off (`ssh` in, `docker compose ps`). Heartbeat **down** after a `/fail`: the email shows the reason (gateway not ready, backup failed). **LINE** check down: the email shows the LINE error (expired token, quota used up); it goes up again with the next successful push, or use 🔔 Test LINE. |
| Restore the incident log | Stop the worker, copy `demo/.state/feedback/backups/feedback-<day>.db` over `demo/.state/feedback/feedback.db` (delete `feedback.db-wal` and `feedback.db-shm`), start the worker. |
| Costs | Dashboard Costs card. About $0.06–0.10 per investigation at Kimi K3 prices; Databricks compute minutes this month; LINE messages against the quota. |

## Scheduled jobs (retention)

| When | What |
|---|---|
| 03:00 daily | Airflow DAG `housekeeping`: task logs older than 30 days. |
| 03:15 daily | Server crontab (user `prapon`): `make -s airflow-db-clean` (Airflow metadata older than 90 days); output in `demo/.state/airflow-db-clean.log`. The line sets `PATH=/usr/local/bin:…` because docker lives there. |
| Every worker day | Report text older than 180 days removed (`REPORT_RETENTION_DAYS`). |
| Once per Bangkok day (checked hourly and at start) | Worker copies `feedback.db` to `demo/.state/feedback/backups/feedback-YYYY-MM-DD.db`; keeps 14. |
| Every 5 min | Worker heartbeat to Healthchecks.io (gateway `/readyz` and last backup checked). |
| After every Databricks run | Job task `housekeeping`: landing folders older than 90 days, `VACUUM` on bronze, silver and gold (about 70 s). |

## Pitfalls and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Error 1033 after the Access login | The hostname's route was on a tunnel with no running connector. | Put the route on tunnel `582a2422…`. |
| Airflow tasks hang in "Pre Execute" | Tasks call the Execution API at `AIRFLOW__API__BASE_URL`; behind Cloudflare Access that returns a login redirect. | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8080/execution/`. |
| Gateway image build fails on plugin install | Unpinned `@openclaw/moonshot-provider` pulled a release needing a newer plugin API. | Pin official plugins to the gateway version (`OPENCLAW_PLUGIN_VERSION=2026.9.9`). |
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

## Dates to remember

| Date | What |
|---|---|
| 2027-01-01 | Gemini 3.6 Flash prices double. Set `FALLBACK_MODEL_PRICE_INPUT_PER_MTOK=1.50`, `…_OUTPUT_PER_MTOK=7.50`, `…_CACHE_READ_PER_MTOK=0.15` in the server's `demo/.env` and restart the worker. |
| 2028-10-16 | Databricks service principal OAuth secrets expire (created 2026-10-17 for workspace `8259554454724321.1`). |

## Before connecting real production data

See [data-residency.md](data-residency.md):
- Check the Google Gemini API (backup model) and Healthchecks.io terms: data location and retention are not checked yet. The Gemini key's project must keep billing on.
- A written no-training agreement from Moonshot, or switch the model provider.
- Record or move the Databricks region (Mumbai, or recreate in Singapore).
- Send reports to a LINE group and add teammates to Access when others join.
