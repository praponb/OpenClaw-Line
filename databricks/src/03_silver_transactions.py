# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: validated, deduplicated transactions
# MAGIC Reads only the bronze rows of the file this run landed, fails loudly on schema drift or
# MAGIC missing amounts, dedupes at-least-once deliveries and signs refunds negative. Rewrites
# MAGIC the `txn_date` partition, so reruns are idempotent.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse")
dbutils.widgets.text("run_date", "")

# COMMAND ----------

from pyspark.sql import functions as F

from quality import null_field_message, schema_drift_message, unexpected_fields

EXPECTED_FIELDS = ["id", "type", "created", "charge_id", "amount", "currency", "customer"]

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.jobs.taskValues.get(
    taskKey="land_stripe_events", key="run_date", debugValue=dbutils.widgets.get("run_date")
)
landed_file = dbutils.jobs.taskValues.get(taskKey="land_stripe_events", key="landed_file", debugValue="")

bronze = spark.table(f"{catalog}.bronze.stripe_events").where(F.col("_file_date") == run_date)
if landed_file:
    day = bronze.where(F.col("_source_file").endswith("/" + landed_file))
else:  # interactive run: use the latest delivery for the date
    latest = bronze.agg(F.max_by("_source_file", "_ingested_at")).first()[0]
    day = bronze.where(F.col("_source_file") == latest)

total = day.count()
if total == 0:
    raise RuntimeError(
        f"No bronze rows for run_date={run_date} from landed file {landed_file or '(latest)'}: "
        "the upstream delivery is empty or was not ingested."
    )

# Failure messages reach the model provider, LINE and the dashboard: field names and
# counts only, never data values (docs/data-residency.md).
rescued = day.where(F.col("_rescued_data").isNotNull())
n_rescued = rescued.count()
if n_rescued:
    fields = unexpected_fields(r[0] for r in rescued.select("_rescued_data").limit(1000).collect())
    raise ValueError(schema_drift_message(landed_file, n_rescued, total, fields, EXPECTED_FIELDS))

n_null_amount = day.where(F.col("amount").isNull()).count()
if n_null_amount:
    raise ValueError(null_field_message(landed_file, "amount", n_null_amount, total))

known = day.where(F.col("type").isin("charge.succeeded", "charge.refunded"))
skipped = total - known.count()
if skipped:
    print(f"Ignoring {skipped} events with other types")

transactions = known.dropDuplicates(["id"]).select(
    F.col("id").alias("event_id"),
    "type",
    "charge_id",
    "customer",
    "currency",
    F.when(F.col("type") == "charge.refunded", -F.col("amount")).otherwise(F.col("amount")).alias("amount"),
    F.timestamp_seconds("created").alias("created_at"),
    F.lit(run_date).cast("date").alias("txn_date"),
    F.lit(landed_file).alias("source_file"),
)

(
    transactions.write.mode("overwrite")
    .option("replaceWhere", f"txn_date = DATE'{run_date}'")
    .saveAsTable(f"{catalog}.silver.stripe_transactions")
)
print(f"Silver: {transactions.count()} transactions for {run_date} from {total} events ({skipped} skipped, duplicates removed)")
