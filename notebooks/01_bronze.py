# Databricks notebook source
# BRONZE: load both CSV files exactly as they are (all columns STRING).
# Nothing is cleaned, typed or rejected here.  Re-running a batch replaces that
# batch only, so rows are never counted twice.

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_id", "batch_001", "Batch ID")
CATALOG = dbutils.widgets.get("catalog")
BATCH_ID = dbutils.widgets.get("batch_id")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

LOG_SCHEMA = ("batch_id string, table_name string, source_file string, source_bytes long, "
              "source_data_rows long, bronze_rows long, row_count_match boolean, loaded_at timestamp")


def load_bronze(file_path, cols, table):
    # Header must match the contract.
    header = spark.read.text(file_path).limit(1).first()[0]
    header = header.replace("\ufeff", "").replace('"', "").strip()
    if header.split(",") != cols:
        raise ValueError(f"Unexpected header in {file_path}: {header}")

    # All columns STRING, no schema inference.
    schema = StructType([StructField(c, StringType(), True) for c in cols])
    df = (spark.read.format("csv").schema(schema)
          .option("header", "true").option("mode", "PERMISSIVE").load(file_path))

    # Audit columns.
    row_hash = F.sha2(F.concat_ws("\u001f", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in cols]), 256)
    df = (df.select("*", F.col("_metadata.file_path").alias("_source_file"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_row_hash", row_hash)
            .withColumn("_row_seq", F.monotonically_increasing_id())
            .withColumn("_row_id", F.concat_ws("-", F.lit(BATCH_ID), F.col("_row_seq").cast("string"))))

    # Idempotent write: remove this batch first, then append it.
    full = f"{FQ}.{table}"
    if spark.catalog.tableExists(full):
        spark.sql(f"DELETE FROM {full} WHERE _batch_id = '{BATCH_ID}'")
        df.write.format("delta").mode("append").saveAsTable(full)
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(full)

    # Reconcile: data rows in file (lines minus header) vs rows in Bronze.
    bronze_rows = spark.table(full).where(F.col("_batch_id") == BATCH_ID).count()
    source_rows = spark.read.text(file_path).count() - 1
    size = os.path.getsize(file_path)
    ok = bronze_rows == source_rows

    log_df = spark.createDataFrame(
        [(BATCH_ID, table, file_path, size, source_rows, bronze_rows, ok, datetime.now())], LOG_SCHEMA)
    log_full = f"{FQ}.bronze_load_log"
    if spark.catalog.tableExists(log_full):
        spark.sql(f"DELETE FROM {log_full} WHERE batch_id = '{BATCH_ID}' AND table_name = '{table}'")
        log_df.write.format("delta").mode("append").saveAsTable(log_full)
    else:
        log_df.write.format("delta").mode("overwrite").saveAsTable(log_full)

    print(f"{table}: source rows={source_rows:,}  bronze rows={bronze_rows:,}  bytes={size:,}  match={ok}")
    return ok


results = [
    load_bronze(ORDERS_FILE, ORDER_COLS, "bronze_orders"),
    load_bronze(TXN_FILE, TXN_COLS, "bronze_transactions"),
]
if not all(results):
    raise AssertionError("Source row count does not equal Bronze row count. See bronze_load_log.")

# COMMAND ----------

# Evidence: load log, schema, sample rows
display(spark.table("bronze_load_log"))

# COMMAND ----------

display(spark.sql("DESCRIBE TABLE bronze_orders"))

# COMMAND ----------

display(spark.sql("DESCRIBE TABLE bronze_transactions"))

# COMMAND ----------

display(spark.sql("SELECT * FROM bronze_orders LIMIT 10"))

# COMMAND ----------

display(spark.sql("SELECT * FROM bronze_transactions LIMIT 10"))

# COMMAND ----------

# Idempotency proof: run this notebook again with the same batch_id.
# The counts below must stay the same (1,000,000 each).
display(spark.sql("""
  SELECT 'bronze_orders' AS tbl, _batch_id, COUNT(*) AS rows FROM bronze_orders GROUP BY _batch_id
  UNION ALL
  SELECT 'bronze_transactions', _batch_id, COUNT(*) FROM bronze_transactions GROUP BY _batch_id
"""))
