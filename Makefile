SHELL := /bin/bash
COMPOSE := docker compose --env-file demo/.env -f demo/docker-compose.yml
STATE := demo/.state

.PHONY: airflow-db-clean install build typecheck test test-py validate-plugin stage-plugin seed demo-env demo-prepare demo-up demo-dry demo-trigger demo-logs demo-down demo-tunnel feedback-report demo-dashboard build-docker demo-prepare-docker state demo-clean lakehouse-validate lakehouse-deploy lakehouse-run

install:
	npm install

build:
	npm run build

typecheck:
	npm run typecheck

test:
	npm test

test-py:
	python3 -m pytest -q airflow/tests databricks/tests

# Regenerates/validates openclaw.plugin.json against the defineToolPlugin entry.
validate-plugin: build
	cd packages/plugin && npx openclaw plugins build --entry ./dist/index.js --check && npx openclaw plugins validate --entry ./dist/index.js

stage-plugin: build
	node scripts/stage-plugin.mjs $(STATE)/plugin/pipeline-investigator packages/plugin
	node scripts/stage-plugin.mjs $(STATE)/plugin/report-capture packages/report-capture

seed:
	python3 demo/seed_repo.py $(STATE)/repo

demo-env:
	@test -f demo/.env || (cp demo/.env.example demo/.env && echo "Created demo/.env - fill in MOONSHOT_API_KEY (and LINE_* for delivery), then re-run." && exit 1)

# Airflow simple-auth-manager passwords: admin, read-only investigator (viewer) and,
# when AIRFLOW_TRIGGER_PASSWORD is set, the dashboard's "Run pipeline" account (user).
demo-prepare: demo-env stage-plugin seed state

# Same as demo-prepare, but builds with Node inside Docker (servers without Node 24).
demo-prepare-docker: demo-env build-docker seed state

NODE_IMAGE ?= node:24
build-docker:
	docker run --rm -u "$$(id -u):$$(id -g)" -e HOME=/tmp -e npm_config_cache=/tmp/.npm \
	  -v "$(CURDIR)":/w -w /w $(NODE_IMAGE) sh -c 'npm ci --no-audit --no-fund && npm run build && \
	  node scripts/stage-plugin.mjs $(STATE)/plugin/pipeline-investigator packages/plugin && \
	  node scripts/stage-plugin.mjs $(STATE)/plugin/report-capture packages/report-capture'

state:
	@mkdir -p $(STATE)/incidents $(STATE)/airflow-auth $(STATE)/feedback
	@set -a; source demo/.env; set +a; umask 077; \
	  if [ -n "$$AIRFLOW_TRIGGER_PASSWORD" ]; then \
	    printf '{"admin": "%s", "investigator": "%s", "dashboard": "%s"}\n' "$$AIRFLOW_ADMIN_PASSWORD" "$$AIRFLOW_VIEWER_PASSWORD" "$$AIRFLOW_TRIGGER_PASSWORD"; \
	  else \
	    printf '{"admin": "%s", "investigator": "%s"}\n' "$$AIRFLOW_ADMIN_PASSWORD" "$$AIRFLOW_VIEWER_PASSWORD"; \
	  fi > $(STATE)/airflow-auth/passwords.json

demo-up: demo-prepare
	$(COMPOSE) up -d --build
	@echo "Airflow UI: http://localhost:8080 (admin / AIRFLOW_ADMIN_PASSWORD)"

# Dry run: worker writes the incident instead of calling /hooks/agent; the
# report is printed here instead of being sent to LINE.
demo-dry: demo-prepare
	DRY_RUN=1 $(COMPOSE) up -d --build
	$(MAKE) demo-trigger
	cd demo && COMPOSE_FILE=docker-compose.yml COMPOSE_ENV_FILES=.env ./run_dry.sh

demo-trigger:
	@touch $(STATE)/.trigger-marker
	@echo "Waiting for Airflow to be healthy..."
	@until [ "$$($(COMPOSE) ps --format '{{.Health}}' airflow)" = "healthy" ]; do sleep 5; done
	@until $(COMPOSE) exec -T airflow airflow dags list 2>/dev/null | grep -q daily_revenue_pipeline; do sleep 5; done
	$(COMPOSE) exec -T airflow airflow dags unpause daily_revenue_pipeline
	$(COMPOSE) exec -T airflow airflow dags trigger daily_revenue_pipeline
	@echo "Triggered. The worker dispatches after DEDUPE_WINDOW_S. Follow with: make demo-logs"

demo-logs:
	$(COMPOSE) logs -f ingestion-worker openclaw-gateway

# Phase 3: start a Cloudflare tunnel to the worker's LINE webhook listener and
# register https://<tunnel>/line/webhook as the LINE channel's webhook URL.
demo-tunnel:
	cd demo && ./tunnel.sh

# Live dashboard (internal worker port, localhost only).
demo-dashboard:
	@set -a; source demo/.env; set +a; url="http://localhost:$${WORKER_HOST_PORT:-8090}/dashboard"; \
	  echo "Dashboard: $$url"; command -v open >/dev/null && open "$$url" || true

# Feedback review for prompt tuning: make feedback-report [DAYS=30]
feedback-report:
	$(COMPOSE) exec -T ingestion-worker node dist/report.js --days $(or $(DAYS),30)

# Stops and removes containers; keeps Airflow history (airflow-home volume) and demo/.state.
demo-down:
	$(COMPOSE) --profile tunnel --profile tunnel-quick down

# Also deletes Airflow history (the airflow-home volume).
demo-clean:
	$(COMPOSE) --profile tunnel --profile tunnel-quick down -v

# Databricks on GCP pipeline (databricks/). Log in once with:
#   databricks auth login --host <workspace URL> --profile $(LAKEHOUSE_PROFILE)
LAKEHOUSE_PROFILE ?= openclaw
FAULT ?= none
lakehouse-validate:
	cd databricks && databricks bundle validate -p $(LAKEHOUSE_PROFILE)

lakehouse-deploy:
	cd databricks && databricks bundle deploy -p $(LAKEHOUSE_PROFILE)

# Manual run; FAULT=schema_drift|null_amounts|empty simulates upstream breakage.
lakehouse-run:
	cd databricks && databricks bundle run daily_revenue_lakehouse -p $(LAKEHOUSE_PROFILE) --params inject_fault=$(FAULT)

# Retention for Airflow's metadata DB (DAG runs, task instances, XComs). Runs outside
# the task sandbox: Airflow 3 tasks can't reach the metadata DB. Cron example (server):
#   15 3 * * * cd ~/OpenClaw-Line && make -s airflow-db-clean >/dev/null
AIRFLOW_METADATA_RETENTION_DAYS ?= 90
airflow-db-clean:
	$(COMPOSE) exec -T airflow airflow db clean \
	  --clean-before-timestamp "$$(date -u -d '-$(AIRFLOW_METADATA_RETENTION_DAYS) days' +%Y-%m-%dT%H:%M:%S+00:00)" --skip-archive --yes

