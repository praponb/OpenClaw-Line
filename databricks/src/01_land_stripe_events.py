# Databricks notebook source
# MAGIC %md
# MAGIC # Land Stripe events
# MAGIC Stands in for the upstream export: writes one JSON-lines file for `run_date` into the
# MAGIC landing volume. Each run gets its own file name, so a rerun after a fix lands a fresh
# MAGIC delivery and downstream tasks only read the file this run produced.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("inject_fault", "none")

# COMMAND ----------

import os
from datetime import date, datetime

from stripe_events import BANGKOK, apply_fault, generate_events, to_json_lines

catalog = dbutils.widgets.get("catalog")
run_date_raw = dbutils.widgets.get("run_date").strip()
run_date = date.fromisoformat(run_date_raw) if run_date_raw else datetime.now(BANGKOK).date()
fault = dbutils.widgets.get("inject_fault").strip() or "none"

events = apply_fault(generate_events(run_date), fault)

folder = f"/Volumes/{catalog}/bronze/landing/stripe_events/date={run_date.isoformat()}"
filename = f"events-{run_date.isoformat()}-{datetime.now(BANGKOK):%Y%m%dT%H%M%S}.json"
os.makedirs(folder, exist_ok=True)
with open(f"{folder}/{filename}", "w") as fh:
    fh.write(to_json_lines(events))

dbutils.jobs.taskValues.set(key="run_date", value=run_date.isoformat())
dbutils.jobs.taskValues.set(key="landed_file", value=filename)
print(f"Landed {len(events)} events (fault={fault}) at {folder}/{filename}")
