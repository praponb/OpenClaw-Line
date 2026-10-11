# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest landed files with Auto Loader
# MAGIC Incremental (`availableNow`) load of every new file under the landing volume into
# MAGIC `bronze.stripe_events`. The schema is fixed: fields that don't match it go to
# MAGIC `_rescued_data` instead of silently evolving the table, and silver fails on them.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

catalog = dbutils.widgets.get("catalog")

EVENT_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("type", T.StringType()),
    T.StructField("created", T.LongType()),
    T.StructField("charge_id", T.StringType()),
    T.StructField("amount", T.LongType()),
    T.StructField("currency", T.StringType()),
    T.StructField("customer", T.StringType()),
])

source = f"/Volumes/{catalog}/bronze/landing/stripe_events/"
checkpoint = f"/Volumes/{catalog}/bronze/checkpoints/stripe_events"

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(EVENT_SCHEMA)
    .load(source)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    .withColumn("_file_date", F.regexp_extract("_source_file", r"date=(\d{4}-\d{2}-\d{2})", 1))
)

query = (
    stream.writeStream.option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(f"{catalog}.bronze.stripe_events")
)
query.awaitTermination()
print(f"Bronze ingest finished: {query.lastProgress}")
