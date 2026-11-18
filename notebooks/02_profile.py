# Databricks notebook source
# PROFILE: look at the real values in Bronze, then edit 00_config.py to match.
# Also builds the dirty-data inventory (type, field, count, percent) from the data.

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# 1. Distinct raw values of the label columns.  Copy the valid ones into 00_config.py.
for tbl, col in [("bronze_orders", "order_status"), ("bronze_orders", "category"),
                 ("bronze_orders", "country"), ("bronze_transactions", "payment_method"),
                 ("bronze_transactions", "currency"), ("bronze_transactions", "txn_status")]:
    print(f"--- {tbl}.{col}")
    display(spark.sql(f"SELECT {col} AS raw_value, COUNT(*) AS rows FROM {tbl} "
                      f"GROUP BY {col} ORDER BY rows DESC LIMIT 60"))

# COMMAND ----------

# 2. Date formats: look at a few raw values.
display(spark.sql("SELECT order_date FROM bronze_orders LIMIT 30"))

# COMMAND ----------

display(spark.sql("SELECT txn_ts FROM bronze_transactions LIMIT 30"))

# COMMAND ----------

# 3. Dirty-data inventory, measured from Bronze (one scan per table).
orders_checks = []
for c in ORDER_COLS:
    orders_checks.append(("null_or_blank", c, f"{clean(c)} IS NULL"))
for c in ["quantity", "unit_price"]:
    cast = "INT" if c == "quantity" else "DECIMAL(18,2)"
    orders_checks.append(("non_numeric_text", c,
                          f"{clean(c)} IS NOT NULL AND try_cast({clean(c)} AS {cast}) IS NULL"))
orders_checks += [
    ("out_of_range", "quantity", f"try_cast({clean('quantity')} AS INT) < 1 OR try_cast({clean('quantity')} AS INT) > {MAX_QTY}"),
    ("out_of_range", "unit_price", f"try_cast({clean('unit_price')} AS DECIMAL(18,2)) <= 0 OR try_cast({clean('unit_price')} AS DECIMAL(18,2)) > {MAX_UNIT_PRICE}"),
    ("invalid_timestamp", "order_date", f"{clean('order_date')} IS NOT NULL AND try_cast({clean('order_date')} AS DATE) IS NULL"),
    ("future_timestamp", "order_date", f"try_cast({clean('order_date')} AS DATE) > current_date()"),
]
# Label columns: (column, valid list, function that gives the clean form)
for c, valid, fn in [("category", CATEGORIES, "INITCAP"), ("order_status", ORDER_STATUSES, "LOWER"),
                     ("country", COUNTRIES, "INITCAP")]:
    if valid:
        v = sql_list(valid)
        orders_checks.append(("case_or_space_variant", c, f"UPPER(TRIM({c})) IN ({v}) AND {c} <> {fn}(TRIM({c}))"))
        orders_checks.append(("invalid_label", c, f"{clean(c)} IS NOT NULL AND UPPER(TRIM({c})) NOT IN ({v})"))

txn_checks = []
for c in TXN_COLS:
    txn_checks.append(("null_or_blank", c, f"{clean(c)} IS NULL"))
txn_checks += [
    ("non_numeric_text", "amount", f"{clean('amount')} IS NOT NULL AND try_cast({clean('amount')} AS DECIMAL(18,2)) IS NULL"),
    ("out_of_range", "amount", f"try_cast({clean('amount')} AS DECIMAL(18,2)) <= 0 OR try_cast({clean('amount')} AS DECIMAL(18,2)) > {MAX_AMOUNT}"),
    ("invalid_timestamp", "txn_ts", f"{clean('txn_ts')} IS NOT NULL AND try_cast({clean('txn_ts')} AS TIMESTAMP) IS NULL"),
    ("future_timestamp", "txn_ts", f"try_cast({clean('txn_ts')} AS TIMESTAMP) > current_timestamp()"),
]
for c, valid, fn in [("payment_method", PAYMENT_METHODS, "LOWER"), ("currency", list(FX_TO_USD), "UPPER"),
                     ("txn_status", TXN_STATUSES, "LOWER")]:
    if valid:
        v = sql_list(valid)
        txn_checks.append(("case_or_space_variant", c, f"UPPER(TRIM({c})) IN ({v}) AND {c} <> {fn}(TRIM({c}))"))
        txn_checks.append(("invalid_label", c, f"{clean(c)} IS NOT NULL AND UPPER(TRIM({c})) NOT IN ({v})"))


def run_checks(dataset, table, checks):
    exprs = ", ".join(f"SUM(CASE WHEN {cond} THEN 1 ELSE 0 END) AS c{i}" for i, (_, _, cond) in enumerate(checks))
    row = spark.sql(f"SELECT COUNT(*) AS total, {exprs} FROM {table}").first()
    total = row["total"]
    return [(dataset, t, f, int(row[f"c{i}"] or 0), round(100.0 * (row[f"c{i}"] or 0) / total, 3))
            for i, (t, f, _) in enumerate(checks)], total


o_rows, o_total = run_checks("orders", "bronze_orders", orders_checks)
t_rows, t_total = run_checks("transactions", "bronze_transactions", txn_checks)

# Duplicate keys and orphan references
dup_o = spark.sql("SELECT COUNT(*) - COUNT(DISTINCT UPPER(TRIM(order_id))) FROM bronze_orders").first()[0]
dup_t = spark.sql("SELECT COUNT(*) - COUNT(DISTINCT UPPER(TRIM(txn_id))) FROM bronze_transactions").first()[0]
exact_o = spark.sql("SELECT COUNT(*) - COUNT(DISTINCT _row_hash) FROM bronze_orders").first()[0]
exact_t = spark.sql("SELECT COUNT(*) - COUNT(DISTINCT _row_hash) FROM bronze_transactions").first()[0]
orphans = spark.sql("""
  SELECT COUNT(*) FROM bronze_transactions t
  LEFT JOIN (SELECT DISTINCT UPPER(TRIM(order_id)) AS oid FROM bronze_orders) o
    ON UPPER(TRIM(t.order_id)) = o.oid
  WHERE t.order_id IS NOT NULL AND TRIM(t.order_id) <> '' AND o.oid IS NULL""").first()[0]

extra = [
    ("orders", "duplicate_business_key_extra_rows", "order_id", dup_o, round(100.0 * dup_o / o_total, 3)),
    ("orders", "exact_duplicate_rows_extra", "all columns", exact_o, round(100.0 * exact_o / o_total, 3)),
    ("transactions", "duplicate_business_key_extra_rows", "txn_id", dup_t, round(100.0 * dup_t / t_total, 3)),
    ("transactions", "exact_duplicate_rows_extra", "all columns", exact_t, round(100.0 * exact_t / t_total, 3)),
    ("transactions", "orphan_order_id", "order_id", orphans, round(100.0 * orphans / t_total, 3)),
]

inv = spark.createDataFrame(o_rows + t_rows + extra,
                            "dataset string, anomaly_type string, field string, rows long, pct_of_file double")
inv.write.format("delta").mode("overwrite").saveAsTable("bronze_dirty_inventory")
display(spark.table("bronze_dirty_inventory").where("rows > 0").orderBy("dataset", "anomaly_type", "field"))
