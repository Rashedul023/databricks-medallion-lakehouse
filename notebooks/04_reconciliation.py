# Databricks notebook source
# RECONCILIATION: Bronze = accepted Silver + quarantined + duplicate losers, no overlap.
# The notebook FAILS if any check fails, so the job stops before Gold.

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

DATASETS = [
    ("orders", "bronze_orders", "silver_orders", "quarantine_orders", "duplicate_losers_orders"),
    ("transactions", "bronze_transactions", "silver_transactions", "quarantine_transactions", "duplicate_losers_transactions"),
]

parts = []
for ds, b, s, q, d in DATASETS:
    parts.append(f"""
    SELECT '{ds}' AS dataset,
      (SELECT COUNT(*) FROM {b}) AS bronze_rows,
      (SELECT COUNT(*) FROM {s}) AS accepted_silver_rows,
      (SELECT COUNT(*) FROM {q}) AS quarantined_rows,
      (SELECT COUNT(*) FROM {d}) AS duplicate_loser_rows,
      -- an id that appears in more than one outcome (should be 0)
      (SELECT COUNT(*) - COUNT(DISTINCT _row_id) FROM
         (SELECT _row_id FROM {s} UNION ALL SELECT _row_id FROM {q} UNION ALL SELECT _row_id FROM {d})) AS overlap_rows,
      -- a Bronze row that has no outcome (should be 0)
      (SELECT COUNT(*) FROM {b} x WHERE NOT EXISTS (
         SELECT 1 FROM (SELECT _row_id FROM {s} UNION ALL SELECT _row_id FROM {q} UNION ALL SELECT _row_id FROM {d}) o
         WHERE o._row_id = x._row_id)) AS unaccounted_rows,
      (SELECT source_data_rows FROM bronze_load_log WHERE table_name = '{b}') AS source_file_rows""")

spark.sql(f"""
CREATE OR REPLACE TABLE recon_summary AS
SELECT *,
  bronze_rows - (accepted_silver_rows + quarantined_rows + duplicate_loser_rows) AS difference,
  (bronze_rows = accepted_silver_rows + quarantined_rows + duplicate_loser_rows
   AND overlap_rows = 0 AND unaccounted_rows = 0
   AND bronze_rows = source_file_rows) AS reconciles,
  current_timestamp() AS checked_at
FROM ({' UNION ALL '.join(parts)})
""")

display(spark.table("recon_summary"))

# COMMAND ----------

failed = spark.table("recon_summary").where("NOT reconciles").collect()
if failed:
    raise AssertionError(f"Reconciliation failed: {failed}")
print("Reconciliation passed for all datasets.")

# COMMAND ----------

# Duplicate loser breakdown
display(spark.sql("""
  SELECT 'orders' AS dataset, duplicate_type, COUNT(*) AS rows FROM duplicate_losers_orders GROUP BY duplicate_type
  UNION ALL
  SELECT 'transactions', duplicate_type, COUNT(*) FROM duplicate_losers_transactions GROUP BY duplicate_type"""))
