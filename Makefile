SHELL := /bin/bash
COMPOSE := docker compose --env-file demo/.env -f demo/docker-compose.yml
STATE := demo/.state

.PHONY: install build typecheck test test-py validate-plugin stage-plugin seed demo-env demo-prepare demo-up demo-dry demo-trigger demo-logs demo-down

install:
	npm install

build:
	npm run build

typecheck:
	npm run typecheck

test:
	npm test

test-py:
	python3 -m pytest -q airflow/tests

# Regenerates/validates openclaw.plugin.json against the defineToolPlugin entry.
validate-plugin: build
	cd packages/plugin && npx openclaw plugins build --entry ./dist/index.js --check && npx openclaw plugins validate --entry ./dist/index.js

stage-plugin: build
	node scripts/stage-plugin.mjs $(STATE)/plugin/pipeline-investigator

seed:
	python3 demo/seed_repo.py $(STATE)/repo

demo-env:
	@test -f demo/.env || (cp demo/.env.example demo/.env && echo "Created demo/.env - fill in MOONSHOT_API_KEY (and LINE_* for delivery), then re-run." && exit 1)

# Airflow simple-auth-manager passwords: admin + read-only investigator (Viewer).
demo-prepare: demo-env stage-plugin seed
	@mkdir -p $(STATE)/incidents $(STATE)/airflow-auth
	@set -a; source demo/.env; set +a; \
	  printf '{"admin": "%s", "investigator": "%s"}\n' "$$AIRFLOW_ADMIN_PASSWORD" "$$AIRFLOW_VIEWER_PASSWORD" > $(STATE)/airflow-auth/passwords.json

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

demo-down:
	$(COMPOSE) down -v
