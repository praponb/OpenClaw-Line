# Data residency review

Reviewed 2026-10-11 against the deployed system; provider terms checked the same day (sources at the end) (home server 192.168.1.49 in Bangkok, `main` at PR #12). Each fact is marked:
- ✅ **verified** from code, config or a live API
- ❓ **to verify** with the provider: their terms or console, which this review can't see

This is an engineering review, not legal advice. Use it as input for a Thai PDPA assessment.

## Summary

| # | Finding | Severity |
|---|---|---|
| 1 | **Databricks processes the pipeline in India.** The workspace's Unity Catalog metastore is `asia-south1` (Mumbai). Raw files and managed tables are stored in Bangkok (`asia-southeast3`), but every read and write happens on compute in Mumbai. **Decision (2026-10-11): stay in Mumbai while the data is synthetic; revisit before real data** (see recommendation 1). | High for real data |
| 2 | ~~**Pipeline error messages carry data values.**~~ ✅ Fixed 2026-10-11: silver now reports field names and counts only. Previously: Silver's schema-drift error includes a sample of the rescued row (`{"amount_cents":33800,…}`) and the null-amount error lists event ids. These reach Moonshot, LINE and the dashboard. With real data, that could be personal data. | High for real data |
| 3 | **Evidence sent to the model provider leaves Thailand and may be used for training.** Sanitized logs, errors, diffs and table names go to Moonshot's international API (`api.moonshot.ai`), operated by Moonshot AI Pte. Ltd. with servers in **Singapore**. Its terms allow using customer content to improve the service, including training, unless agreed otherwise in writing (enterprise arrangement). Data is kept while the account is active. | **High for real data** |
| 4 | **The sanitizer misses some Thai personal data.** Thai national IDs and phone numbers are masked since 2026-10-11; names, addresses and bank account numbers still aren't. | Medium (was: no Thai identifiers at all) |
| 5 | ~~**Nothing is ever deleted.**~~ ✅ Retention set 2026-10-11: report text 180 days, Airflow logs 30 days, Airflow metadata 90 days (daily cron on the server), landing files 90 days, Delta `VACUUM` 7 days. | Done |
| 6 | **Credentials are well scoped**: all least-privilege and read-only except the two "write" accounts (Airflow runner, dashboard trigger), which can only start runs. ✅ The old `.env` and database backups were deleted on 2026-10-11. | Low |

Today the pipeline only handles **synthetic** Stripe-style events (`databricks/src/stripe_events.py`), so nothing here is personal data yet. The findings matter **before real production data is connected**.

## Where data goes

```
                Bangkok home server (Docker)                       External services
 ┌──────────────────────────────────────────────────────┐
 │ Airflow ── callback ──▶ ingestion worker ──▶ OpenClaw│── evidence + prompt ─▶ Moonshot (Kimi K3)   ❓ region
 │   │  task logs, run metadata   │  incident log        │◀─ report ────────────
 │   │                            │  (SQLite)            │── report + card ────▶ LINE (personal chat)  ❓ region
 │   │                            │                      │◀─ feedback taps ─────
 │   └── starts job ──────────────┼──────────────────────│── run / read ───────▶ Databricks, asia-south1 (Mumbai) ✅
 │                                │                      │       └─ reads/writes ▶ GCS bucket, asia-southeast3 (Bangkok) ✅
 │ investigator tools ────────────┼──────────────────────│── read code ────────▶ GitHub (private repo)
 └────────────────────────────────┼──────────────────────┘
                                  └── dashboard, LINE webhook, Airflow UI ─▶ Cloudflare tunnel (edges BKK/SIN) ✅
```

## Destinations

| Destination | What it receives | Location | Retention | Controls in place |
|---|---|---|---|---|
| **Moonshot (Kimi K3)** — Moonshot AI Pte. Ltd., Singapore | System prompt (`AGENTS.md`); incident message (DAG/task/run ids, sanitized exception, sanitized log excerpt); tool results: Airflow log tail (≤ 24k chars), Databricks error and trace, git diffs (≤ 5 commits × 8k chars), Unity Catalog and dbt table names. | International endpoint `api.moonshot.ai` ✅; "secure servers located in Singapore", may be transferred elsewhere with "appropriate safeguards" ✅ | Input kept "while your account is active"; content may be used for training unless agreed otherwise in writing ✅ | Sanitizer on every piece of evidence; evidence fenced as untrusted; read-only tools; isolated session per incident; max 6 tool calls. |
| **Anthropic (Claude Sonnet 5.5, backup model)** | The same as Moonshot, but only for runs where Kimi K3 failed. | ❓ Not checked yet; Anthropic processes API traffic in the US by default. | ❓ Check the commercial terms before real data (Anthropic says it doesn't train on API data by default). | Same controls as Moonshot; used only when the primary fails. |
| **Healthchecks.io** | Heartbeat pings every 5 min; ping bodies: incident refs (`INC-…`), sanitized error text (gateway, backup or LINE errors). No report text, DAG names or LINE user ids. | ❓ Not checked yet | ❓ Ping log of the check | Bodies sanitized and capped at 2,000 characters. |
| **LINE** | Model-written report (error text, file paths, commit SHA and GitHub login, table names); feedback card; fallback notices; dashboard test messages. Inbound: feedback taps with the LINE user id. | LY Corporation (Japan). Its policy names data centers in Japan and the US for Japanese users and lists Thailand among group transfer destinations; the storage location for Thai users is not stated ❓ | Message content kept "for a minimum period", then automatically deleted from LINE's servers, and not used beyond delivery without extra consent ✅; copies stay on users' devices | Target is a **personal chat** (`U…` id) ✅, not a group. Voter ids stored only as a salted hash (`u:` + 16 hex chars) ✅. Webhook signature verified. Signed postbacks. |
| **Databricks (GCP)** | Pipeline data (landing files, bronze/silver/gold tables); run metadata and logs; notebook source (from the bundle). | Workspace/metastore `asia-south1`, Mumbai ✅ | Run history up to 60 days (Databricks docs) ✅; landing files 90 days and `VACUUM` 7 days via the job's housekeeping task ✅ | Service principals: runner `CAN_MANAGE_RUN`, reader `CAN_VIEW` + `BROWSE` (metadata only; data and volume reads return 403) ✅. Serverless compute. |
| **Google Cloud Storage** | Raw landing files (`landing/`), managed Delta tables (`_managed/`). | `asia-southeast3`, Bangkok ✅ | `landing/` date folders deleted after 90 days by the housekeeping task; `_managed/` follows `VACUUM` ✅ | Public access prevention on; access only through the Unity Catalog storage credential. |
| **GitHub** | Read by the investigator: commit metadata and diffs of `praponb/OpenClaw-Line`. No pipeline data is sent to GitHub. | US (GitHub) | Repository history | Fine-grained token limited to this one repo with Contents read-only; write probes return 403 ✅. |
| **Cloudflare** | Dashboard pages (incident reports, run status), Airflow UI, LINE webhook traffic; Access login email. TLS terminates at Cloudflare's edge, so Cloudflare can see this traffic in clear text. | Edges `bkk08`, `sin12`, `sin15` ✅ | **Free plan** (confirmed by the owner, 2026-10-11): Zero Trust Access logs kept 24 hours; admin audit logs 18 months; authentication logs 6 months ✅. | Access (one-time PIN, one allowed email) plus worker-side JWT check; Protect-with-Access on the Airflow route ✅. |

## Data at rest on the home server (Bangkok)

| Store | Content | Retention |
|---|---|---|
| `demo/.state/feedback/feedback.db` (SQLite) | Incidents (ids, timestamps, full report text, prompt version, resolution), feedback (hashed voter, rating, reason), model token usage. | Report text removed after `REPORT_RETENTION_DAYS` (default 180); timings, votes, resolution and cost stay. `line_events` pruned after 7 days ✅. |
| Airflow volume `openclaw-line-demo_airflow-home` | Metadata DB, task logs (may contain data values printed by tasks). | `housekeeping` DAG, daily 03:00 Bangkok: task logs after 30 days ✅. Metadata after 90 days: server crontab runs `make airflow-db-clean` daily at 03:15 Bangkok (installed 2026-10-11; output in `demo/.state/airflow-db-clean.log`) ✅. It isn't in the DAG because Airflow 3 tasks can't reach the metadata DB. |
| `demo/.env` | All secrets (Moonshot key, LINE token, Databricks OAuth secrets, GitHub token, Airflow passwords, Cloudflare tunnel token). | Mode 600 ✅; old backups deleted 2026-10-11 ✅. |
| `demo/.state/feedback/backups/` | Daily copies of `feedback.db` (same content). | Newest 14 daily files (`FEEDBACK_BACKUP_KEEP`) ✅; older report text disappears from the backups within 14 days of its removal from the live database. |
| `demo/.state/incidents` | Dry-run incident messages (only written in `DRY_RUN` mode). | Until deleted. |

## Credentials

| Credential | Held by | Can do |
|---|---|---|
| Moonshot API key | Gateway | Call the model API (billing). |
| Anthropic API key | Gateway | Call the backup model (billing). |
| Healthchecks.io ping URLs | Worker | Mark the two checks up or down (anyone with a URL can). |
| LINE channel token and secret | Gateway and worker | Push/reply messages; read quota. |
| Airflow `investigator` (viewer) | Worker, gateway | Read DAGs, runs and logs; **can't** start runs (verified 403). |
| Airflow `dashboard` (user) | Worker only | Start DAG runs (Run pipeline button). |
| Airflow `admin` | You | Everything (UI login). |
| Databricks `sp-airflow-runner` | Airflow only | `CAN_MANAGE_RUN` on one job. |
| Databricks `sp-openclaw-reader` | Gateway and worker | `CAN_VIEW` on the job, `CAN_READ` on its notebooks, `BROWSE` on catalog `lakehouse`. |
| GitHub fine-grained token | Gateway | Read one repository. |
| Cloudflare tunnel token | Tunnel container | Run the connector for the named tunnel. |
| Webhook and feedback signing secrets | Worker, gateway, Airflow | Sign callbacks, reports, usage, postbacks. |

OAuth secrets for the Databricks service principals expire on 2027-10-11.

## Sanitizer coverage

`packages/shared/src/sanitize.ts` masks these before evidence reaches the model, the logs or LINE: private keys, connection-string passwords, bearer tokens, JWTs, AWS keys, Stripe keys, GitHub/GitLab/Slack tokens, Anthropic and OpenAI-style keys, Databricks PATs and OAuth secrets, `key=value` secrets, emails, card numbers (Luhn plus a real brand prefix and length; Luhn alone used to mask about 1 in 10 Databricks run ids), Thai national IDs (13 digits, plain or `1-2345-67890-12-3`, check digit validated), and Thai phone numbers (`+66…`, mobile `0[689]x-xxx-xxxx`, landlines `02-xxx-xxxx` / `0xx-xxx-xxx`).

A checksum-valid 13-digit number that isn't a Thai ID (e.g. some epoch-millisecond timestamps) is masked too; privacy wins over a lost timestamp.

**Not covered:** names, addresses, bank account numbers, and other free-text values that tasks print or put into exceptions.

## Recommendations

In order. Items 1–4 should happen before real production data is connected.

1. **Where Databricks processes data.** Databricks doesn't offer a Bangkok workspace region.
   - **Decision 2026-10-11: keep Mumbai while the data is synthetic.** Migrating means a new workspace, catalog, credentials, service principals and secrets, which isn't worth it yet.
   - **Before real data:** either record Mumbai as a cross-border transfer in the PDPA assessment, or recreate the workspace in `asia-southeast1` (Singapore). Singapore is closer to Bangkok, to the Cloudflare edges and to Moonshot's servers, with less cross-region traffic; the bundle redeploys with a new host.
   - Either way, keep the GCS bucket in Bangkok for data at rest.
2. ✅ **Keep data values out of pipeline errors.** Done for the lakehouse job (`databricks/src/quality.py`: "Unexpected fields: amount_cents", counts only). Apply the same rule to any production task code: exceptions travel to the model, LINE and the dashboard.
3. ✅ **Extend the sanitizer for Thai personal data:** national ID and phone numbers are done. Add bank account formats you use when real data arrives.
4. **Before real data, get a written no-training agreement from Moonshot, or switch provider.** Checked 2026-10-11:
   - Moonshot's terms let it use API content to improve the service, including training, unless agreed otherwise in writing. Restrictions require contacting Moonshot about an enterprise arrangement.
   - Data is stored in Singapore and kept while the account is active.
   - Synthetic data is fine as is. For real data, ask Moonshot for a written training opt-out and DPA, or change `OPENCLAW_MODEL` (one line) to a provider with no-training and regional terms.
5. ✅ **Retention periods**, set 2026-10-11:
   - worker: report text after `REPORT_RETENTION_DAYS` (180)
   - Airflow `housekeeping` DAG: logs after 30 days; metadata after 90 days with `make airflow-db-clean` from the server crontab (daily 03:15 Bangkok)
   - the job's `housekeeping` task: landing date folders after `landing_retention_days` (90), plus `VACUUM` (7 days) on bronze, silver and gold
6. ✅ **Old secret and database backups deleted** on the server (2026-10-11).
7. **When moving to a team:** send reports to a LINE group with known members (the target is currently a personal chat), and add those people's emails to the Cloudflare Access policy and `DASHBOARD_ALLOWED_EMAILS`.

## Provider checks (2026-10-11)

- [x] **Moonshot:** servers in Singapore; input kept while the account is active; content may be used for training unless agreed in writing; no self-serve zero retention. A **DPA or no-training agreement still needs to be requested** before real data.
- [x] **Kimi K3 prices** confirmed on Moonshot's price page: input $3.00, cached input $0.30, output $15.00 per MTok; cache writes $3.00 (5 min) or $6.00 (1 h). They match the configured values. Cache writes are estimated at $3.00 because the usage data doesn't say which TTL was used.
- [~] **LINE:** message content kept for a minimum period then auto-deleted, not used beyond delivery. The storage location for Thai users is not published; ask LY Corporation if the PDPA assessment needs it.
- [x] **Databricks:** run history up to 60 days. Region `asia-south1` from the Unity Catalog metastore (a workspace and its metastore share a region).
- [x] **Cloudflare:** the account is on the **Free plan** (owner, 2026-10-11), so Access logs are kept 24 hours; admin audit logs 18 months; authentication logs 6 months.

## Sources

- Moonshot (Kimi) OpenPlatform terms (last updated July 30, 2026): https://platform.kimi.ai/docs/agreement/modeluse
- Kimi OpenPlatform privacy policy (last updated April 30, 2025): https://platform.kimi.ai/docs/agreement/userprivacy
- Kimi API pricing: https://platform.kimi.ai/docs/pricing/chat
- LY Corporation privacy policy (last updated July 31, 2026): https://www.lycorp.co.jp/en/company/privacypolicy/
- Databricks, monitor job runs (60-day history): https://docs.databricks.com/gcp/en/jobs/monitor.html
- Cloudflare Zero Trust logs (retention by plan): https://developers.cloudflare.com/cloudflare-one/insights/logs
- Cloudflare Access audit logs: https://developers.cloudflare.com/cloudflare-one/insights/logs/audit-logs
