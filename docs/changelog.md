# Work log

Times are Bangkok time (UTC+7). PR numbers refer to `praponb/OpenClaw-Line`.

## 2026-10-10: investigator built and deployed

| PR | What |
|---|---|
| #1 | Phases 1–2 of `spec.docx`: Airflow `on_failure_callback` → ingestion worker (HMAC, dedupe per DAG run, root-task selection, log pre-filter, sanitizer) → OpenClaw agent `pipeline-investigator` (read-only tools: Airflow log, git diff, dbt lineage) → report to LINE. Model switched from Claude to Moonshot Kimi K3. |
| #2, #3 | Phase 3: LINE feedback card (👍 / 👎 / ⚠️ plus reasons), `report-capture` plugin, SQLite feedback log, `make feedback-report`, prompt-version tracking. |

Also that day:
- Stack moved from the Mac to the Ubuntu server `192.168.1.49` (Docker Compose; the Mac stack is stopped).
- Named Cloudflare tunnel with Access for the LINE webhook and the dashboard.
- Demo DAG `daily_revenue_pipeline` made manual-only.
- Pitfalls found: OpenClaw Code Mode and `tools.profile`, plugin install versus `load.paths`, tsup stripping `node:` prefixes. See [operations.md](operations.md#pitfalls-and-fixes).

## 2026-10-11: real pipeline, operations, privacy

| Time | PR | What |
|---|---|---|
| 10:20 | #4 | Live dashboard (`dashboard.praponb.com`, Cloudflare Access plus worker JWT check), Linux-server support, Airflow history volume and LAN bind. |
| — | — | `airflow.praponb.com` fixed: error 1033 came from the route sitting on no running tunnel. Route moved to tunnel `582a2422…`; **Protect with Access** turned on; unused tunnels `airflow` and `openclaw-tunnel` deleted. |
| 14:57 | #5 | **Databricks on GCP pipeline**: bundle `databricks/`, job `daily_revenue_lakehouse` (land → bronze with Auto Loader → silver → gold, serverless) with `inject_fault` for incident tests; Airflow DAG `databricks_daily_revenue`; investigator tool `get_databricks_run_error`; two service principals (runner `CAN_MANAGE_RUN`, reader `CAN_VIEW`); dashboard card for Databricks runs. Fixes: Airflow tasks hung on the public base URL behind Access (now `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`), OpenClaw plugins pinned to 2026.9.9, Bangkok `run_date` template. |
| 15:23 | #6 | Git evidence from the real GitHub repo (fine-grained read-only token); the tool says "File not found in the repository" instead of "no commits"; `GIT_PATH_PREFIXES=airflow/`. |
| 15:47 | #7 | **Unity Catalog lineage** for the blast radius (reader has `BROWSE` on catalog `lakehouse` and `CAN_READ` on the bundle files); dashboard **Test LINE** button. |
| 16:05 | #8 | **CI** (GitHub Actions): build, typecheck, vitest, plugin manifest check, pytest. |
| 16:05 | #9 | **MTTR**: ✅ Resolved button in LINE plus automatic detection of the next successful run; MTTR on the dashboard and in `make feedback-report`. |
| 16:52 | #10 | **Cost tracking**: model tokens per incident (report-capture `llm_output`), LINE quota, Databricks compute minutes; Kimi K3 prices configured. |
| 16:52 | #11 | Dashboard **▶ Run pipeline** button (Airflow account `dashboard`, role `user`), chosen instead of a daily schedule. |
| 17:11 | #12 | Cost link fixed: usage is matched to the incident through the `Ref: INC-…` in the report-writing model call (session keys and run ids differ for webhook runs). |
| 17:18 | #13 | **Data-residency review**: [data-residency.md](data-residency.md). |
| 17:31 | #14 | Privacy: pipeline errors carry field names and counts only; sanitizer masks Thai national IDs and phone numbers; card rule needs a real brand prefix (Luhn alone hid about 1 in 10 Databricks run ids). |
| 17:44 | #15 | Retention: report text 180 days, Airflow logs 30 days, landing files 90 days plus `VACUUM`; provider terms checked; old secret backups deleted. |
| 17:47 | #16 | Airflow metadata cleanup cron (03:15) and Cloudflare Free plan recorded. |
| — | #17 | Report details: Databricks traceback lines mapped to file lines, task duration on the summary line, callback `log_filepath`. |

Live tests that day (all through Airflow → Databricks → investigator → LINE):

| Incident | What it tested |
|---|---|
| INC-8C1TJY | A real template bug; auto-resolved by MTTR after 14 min. |
| INC-B6YRC8 | First break-it run. Its report wrongly said "code unchanged", which led to #6. |
| INC-0CG413 | Break-it rerun; cited commit `29cb822`. |
| INC-1WN78Q, INC-YHK57T | Cost tracking; about $0.06–0.10 per investigation. |

## 2026-10-13: reliability

| PR | What |
|---|---|
| — | Work now happens on branch `b_pbv_main` (recreated from `main`), with PRs from it to `main`. |
| — | **Reliability**: backup model Gemini 3.5 Flash, served as 3.6 Flash (`model.fallbacks`, priced separately on the dashboard); Healthchecks.io heartbeat (worker or server down, gateway not ready, backup failed) and LINE-push alerts; dedupe state saved in `feedback.db`, so a restart resumes grouping and doesn't re-investigate; daily `VACUUM INTO` backups of `feedback.db` (14 kept); `/healthz` shows `investigating`. |
