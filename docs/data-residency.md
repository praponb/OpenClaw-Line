# Data residency review

Reviewed 2026-10-11 against the deployed system (home server 192.168.1.49 in Bangkok, `main` at PR #12). Each fact is marked:
- ✅ **verified** from code, config or a live API
- ❓ **to verify** with the provider: their terms or console, which this review can't see

This is an engineering review, not legal advice. Use it as input for a Thai PDPA assessment.

## Summary

| # | Finding | Severity |
|---|---|---|
| 1 | **Databricks processes the pipeline in India.** The workspace's Unity Catalog metastore is `asia-south1` (Mumbai). Raw files and managed tables are stored in Bangkok (`asia-southeast3`), but every read and write happens on compute in Mumbai. | High for real data |
| 2 | **Pipeline error messages carry data values.** Silver's schema-drift error includes a sample of the rescued row (`{"amount_cents":33800,…}`) and the null-amount error lists event ids. These reach Moonshot, LINE and the dashboard. With real data, that could be personal data. | High for real data |
| 3 | **Evidence sent to the model provider leaves Thailand.** Sanitized logs, errors, diffs and table names go to Moonshot's international API (`api.moonshot.ai`). Processing location, retention and training use are ❓. | Medium |
| 4 | **The sanitizer misses Thai identifiers.** It masks secrets, emails and card numbers, but not Thai national IDs, phone numbers, names or addresses. | Medium |
| 5 | **Nothing is ever deleted.** No retention is set for the incident log (report text), Airflow task logs, GCS landing files or Databricks tables. | Medium |
| 6 | **Credentials are well scoped**: all least-privilege and read-only except the two "write" accounts (Airflow runner, dashboard trigger), which can only start runs. Old `.env` backups with secrets remain on the server. | Low |

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
| **Moonshot (Kimi K3)** | System prompt (`AGENTS.md`); incident message (DAG/task/run ids, sanitized exception, sanitized log excerpt); tool results: Airflow log tail (≤ 24k chars), Databricks error and trace, git diffs (≤ 5 commits × 8k chars), Unity Catalog and dbt table names. | International endpoint `api.moonshot.ai` ✅; data-center location ❓ | ❓ (API data retention and training use per Moonshot's terms) | Sanitizer on every piece of evidence; evidence fenced as untrusted; read-only tools; isolated session per incident; max 6 tool calls. |
| **LINE** | Model-written report (error text, file paths, commit SHA and GitHub login, table names); feedback card; fallback notices; dashboard test messages. Inbound: feedback taps with the LINE user id. | LY Corporation ❓; chat history stays on users' devices and LINE's servers | ❓ (LINE's retention); messages stay in the chat until deleted | Target is a **personal chat** (`U…` id) ✅, not a group. Voter ids stored only as a salted hash (`u:` + 16 hex chars) ✅. Webhook signature verified. Signed postbacks. |
| **Databricks (GCP)** | Pipeline data (landing files, bronze/silver/gold tables); run metadata and logs; notebook source (from the bundle). | Workspace/metastore `asia-south1`, Mumbai ✅ | Run history per Databricks defaults ❓; tables and files: never deleted (finding 5) | Service principals: runner `CAN_MANAGE_RUN`, reader `CAN_VIEW` + `BROWSE` (metadata only; data and volume reads return 403) ✅. Serverless compute. |
| **Google Cloud Storage** | Raw landing files (`landing/`), managed Delta tables (`_managed/`). | `asia-southeast3`, Bangkok ✅ | None configured | Public access prevention on; access only through the Unity Catalog storage credential. |
| **GitHub** | Read by the investigator: commit metadata and diffs of `praponb/OpenClaw-Line`. No pipeline data is sent to GitHub. | US (GitHub) | Repository history | Fine-grained token limited to this one repo with Contents read-only; write probes return 403 ✅. |
| **Cloudflare** | Dashboard pages (incident reports, run status), Airflow UI, LINE webhook traffic; Access login email. TLS terminates at Cloudflare's edge, so Cloudflare can see this traffic in clear text. | Edges `bkk08`, `sin12`, `sin15` ✅ | ❓ (per Cloudflare plan) | Access (one-time PIN, one allowed email) plus worker-side JWT check; Protect-with-Access on the Airflow route ✅. |

## Data at rest on the home server (Bangkok)

| Store | Content | Retention |
|---|---|---|
| `demo/.state/feedback/feedback.db` (SQLite) | Incidents (ids, timestamps, full report text, prompt version, resolution), feedback (hashed voter, rating, reason), model token usage. | Kept forever. Only `line_events` (webhook dedupe) is pruned after 7 days ✅. |
| `feedback.db.bak-*` | Copies made before schema changes on 2026-10-11. | Until deleted by hand. |
| Airflow volume `openclaw-line-demo_airflow-home` | Metadata DB, task logs (may contain data values printed by tasks). | Kept forever (no log cleanup configured). |
| `demo/.env`, `demo/.env.bak-*` | All secrets (Moonshot key, LINE token, Databricks OAuth secrets, GitHub token, Airflow passwords, Cloudflare tunnel token). | Files are mode 600 ✅; backups kept until deleted. |
| `demo/.state/incidents` | Dry-run incident messages (only written in `DRY_RUN` mode). | Until deleted. |

## Credentials

| Credential | Held by | Can do |
|---|---|---|
| Moonshot API key | Gateway | Call the model API (billing). |
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

`packages/shared/src/sanitize.ts` masks these before evidence reaches the model, the logs or LINE: private keys, connection-string passwords, bearer tokens, JWTs, AWS keys, Stripe keys, GitHub/GitLab/Slack tokens, Anthropic and OpenAI-style keys, Databricks PATs and OAuth secrets, `key=value` secrets, emails, and card numbers (Luhn-checked).

**Not covered:** Thai national ID numbers (13 digits with check digit), phone numbers (`+66…`, `0[689]…`), names, addresses, bank account numbers, and any free-text field values that tasks print or put into exceptions.

## Recommendations

In order. Items 1–4 should happen before real production data is connected.

1. **Decide where Databricks should process data.** Databricks doesn't offer a Bangkok workspace region, so options are:
   - (a) Accept Mumbai and record it as a cross-border transfer.
   - (b) Recreate the workspace in `asia-southeast1` (Singapore). That's closer to Bangkok and to the Cloudflare edges, with lower cross-region traffic cost; the bundle redeploys unchanged apart from the host.
   - Either way, keep the GCS bucket in Bangkok for data at rest.
2. **Keep data values out of pipeline errors.** Change `databricks/src/03_silver_transactions.py` to report **field names only** (e.g. "unexpected field `amount_cents`") and counts, not sample rows or event ids. Apply the same rule to any production task code: exceptions travel to the model, LINE and the dashboard.
3. **Extend the sanitizer for Thai personal data:** national ID (13 digits, validated check digit), Thai phone numbers, and bank account formats you use, with tests like the existing ones.
4. **Confirm Moonshot's terms for API data** ❓: processing location, retention, whether it's used for training, and whether a data processing agreement is available. If they don't fit, the model is a one-line config change (`OPENCLAW_MODEL`) to a provider with regional processing or zero-retention terms.
5. **Set retention periods:**
   - incident log report text, e.g. 180 days, via a small prune job in the worker
   - Airflow log cleanup, e.g. 30 days
   - a GCS lifecycle rule on `landing/`, e.g. delete after 90 days, once bronze is the system of record
   - Delta `VACUUM` on the managed tables
6. **Delete the old secret backups** on the server (`demo/.env.bak-*`) and the `feedback.db.bak-*` copies once you no longer need them.
7. **When moving to a team:** send reports to a LINE group with known members (the target is currently a personal chat), and add those people's emails to the Cloudflare Access policy and `DASHBOARD_ALLOWED_EMAILS`.

## To verify with providers ❓

- [ ] Moonshot: data-center location for `api.moonshot.ai`, retention of API inputs and outputs, training use, DPA.
- [ ] LINE (LY Corporation): storage location and retention for Messaging API messages in Thailand.
- [ ] Databricks: confirm the workspace region in the account console (this review saw the metastore `metastore_gcp_asia_south1`), and run-history retention.
- [ ] Cloudflare: logging and retention for your plan (Access logs, tunnel traffic).
- [ ] Kimi K3 prices used for cost estimates came from an AI-generated summary; check them on Moonshot's price page.
