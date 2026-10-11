# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: daily revenue
# MAGIC One row per currency per day: gross charges, refunds, net revenue and counts. The final
# MAGIC data-quality gate refuses to publish an empty or negative day.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse")
dbutils.widgets.text("run_date", "")

# COMMAND ----------

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.jobs.taskValues.get(
    taskKey="land_stripe_events", key="run_date", debugValue=dbutils.widgets.get("run_date")
)

is_refund = F.col("type") == "charge.refunded"
daily = (
    spark.table(f"{catalog}.silver.stripe_transactions")
    .where(F.col("txn_date") == F.lit(run_date).cast("date"))
    .groupBy(F.col("txn_date").alias("revenue_date"), "currency")
    .agg(
        F.sum(F.when(~is_refund, F.col("amount")).otherwise(0)).alias("gross_amount"),
        F.sum(F.when(is_refund, -F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.sum("amount").alias("net_amount"),
        F.count(F.when(~is_refund, 1)).alias("charge_count"),
        F.count(F.when(is_refund, 1)).alias("refund_count"),
    )
    .withColumn("computed_at", F.current_timestamp())
)

rows = daily.collect()
if not rows:
    raise RuntimeError(f"No silver transactions for {run_date}; refusing to publish an empty revenue day.")
negative = [r.currency for r in rows if r.net_amount < 0]
if negative:
    raise ValueError(f"Net revenue for {run_date} is negative for {', '.join(negative)}; refunds exceed charges.")

(
    daily.write.mode("overwrite")
    .option("replaceWhere", f"revenue_date = DATE'{run_date}'")
    .saveAsTable(f"{catalog}.gold.daily_revenue")
)
for r in rows:
    print(f"{r.revenue_date} {r.currency}: net {r.net_amount / 100:,.2f} ({r.charge_count} charges, {r.refund_count} refunds)")
