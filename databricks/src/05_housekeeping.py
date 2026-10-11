# Databricks notebook source
# MAGIC %md
# MAGIC # Housekeeping (retention)
# MAGIC Runs after every job run, even a failed one: deletes landing files older than
# MAGIC `landing_retention_days` (bronze keeps the ingested rows) and VACUUMs the Delta tables
# MAGIC (7-day default), so old file versions don't pile up. See docs/data-residency.md.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse")
dbutils.widgets.text("landing_retention_days", "90")

# COMMAND ----------

import os
import re
import shutil
from datetime import date, timedelta

catalog = dbutils.widgets.get("catalog")
days = int(dbutils.widgets.get("landing_retention_days"))
cutoff = date.today() - timedelta(days=days)

root = f"/Volumes/{catalog}/bronze/landing/stripe_events"
removed = []
for name in sorted(os.listdir(root)) if os.path.isdir(root) else []:
    m = re.fullmatch(r"date=(\d{4}-\d{2}-\d{2})", name)
    if m and date.fromisoformat(m.group(1)) < cutoff:
        shutil.rmtree(f"{root}/{name}")
        removed.append(name)
print(f"Landing: removed {len(removed)} date folder(s) older than {cutoff} ({days} days)")

for table in ("bronze.stripe_events", "silver.stripe_transactions", "gold.daily_revenue"):
    if spark.catalog.tableExists(f"{catalog}.{table}"):
        spark.sql(f"VACUUM {catalog}.{table}")  # default retention: 7 days
        print(f"VACUUM {catalog}.{table}")
