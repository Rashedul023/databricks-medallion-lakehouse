# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# SILVER: safe typing, rule validation, quarantine, deterministic de-duplication.
#
# Processing order (precedence) for every Bronze row:
#   1. Validate (typed values + business rules).  Any failure -> quarantine.
#   2. Among VALID rows only, de-duplicate on the business key.
#         rank 1 (first row in the source file) -> Silver (accepted)
#         rank > 1                              -> duplicate loser
# So each Bronze row ends in exactly one bucket:
#   Bronze rows = accepted + quarantined + duplicate losers

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

RULES_MATRIX = []   # (dataset, rule_code, condition) -> saved as silver_rules_matrix

# =============================================================================
# ORDERS
# =============================================================================
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_orders_parsed AS
SELECT
  _row_id, _row_seq, _row_hash, _batch_id, _source_file, _ingested_at,
  UPPER({clean('order_id')})                          AS order_id,
  UPPER({clean('customer_id')})                       AS customer_id,
  {clean('order_date')}                               AS order_date_raw,
  try_cast({clean('order_date')} AS DATE)             AS order_date,
  INITCAP({clean('category')})                        AS category,
  {clean('quantity')}                                 AS quantity_raw,
  try_cast({clean('quantity')} AS INT)                AS quantity,
  {clean('unit_price')}                               AS unit_price_raw,
  try_cast({clean('unit_price')} AS DECIMAL(18,2))    AS unit_price,
  UPPER({clean('order_status')})                      AS order_status,
  INITCAP({clean('country')})                         AS country
FROM bronze_orders
""")

ORDER_RULES = [
    ("ORDER_ID_MISSING",        "order_id IS NULL"),
    ("CUSTOMER_ID_MISSING",     "customer_id IS NULL"),
    ("ORDER_DATE_MISSING",      "order_date_raw IS NULL"),
    ("ORDER_DATE_UNPARSABLE",   "order_date_raw IS NOT NULL AND order_date IS NULL"),
    ("ORDER_DATE_FUTURE",       "order_date > current_date()"),
    ("ORDER_DATE_TOO_OLD",      f"order_date < DATE'{MIN_DATE}'"),
    ("CATEGORY_MISSING",        "category IS NULL"),
    ("QUANTITY_MISSING",        "quantity_raw IS NULL"),
    ("QUANTITY_NOT_NUMERIC",    "quantity_raw IS NOT NULL AND quantity IS NULL"),
    ("QUANTITY_OUT_OF_RANGE",   f"quantity < 1 OR quantity > {MAX_QTY}"),
    ("UNIT_PRICE_MISSING",      "unit_price_raw IS NULL"),
    ("UNIT_PRICE_NOT_NUMERIC",  "unit_price_raw IS NOT NULL AND unit_price IS NULL"),
    ("UNIT_PRICE_OUT_OF_RANGE", f"unit_price <= 0 OR unit_price > {MAX_UNIT_PRICE}"),
    ("ORDER_STATUS_MISSING",    "order_status IS NULL"),
    ("ORDER_STATUS_INVALID",    f"order_status IS NOT NULL AND order_status NOT IN ({sql_list(ORDER_STATUSES)})"),
    ("COUNTRY_MISSING",         "country IS NULL"),
]
if CATEGORIES:
    ORDER_RULES.append(("CATEGORY_INVALID", f"category IS NOT NULL AND UPPER(category) NOT IN ({sql_list(CATEGORIES)})"))
if COUNTRIES:
    ORDER_RULES.append(("COUNTRY_INVALID", f"country IS NOT NULL AND UPPER(country) NOT IN ({sql_list(COUNTRIES)})"))
RULES_MATRIX += [("orders", c, cond) for c, cond in ORDER_RULES]

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_orders_checked AS
SELECT *, {reasons_sql(ORDER_RULES)} AS rejection_reasons
FROM v_orders_parsed
""")

# Quarantine: original raw (Bronze) values + reasons.
spark.sql("""
CREATE OR REPLACE TABLE quarantine_orders AS
SELECT b.*, c.rejection_reasons,
       concat_ws('; ', c.rejection_reasons) AS rejection_reason,
       current_timestamp() AS _quarantined_at
FROM v_orders_checked c
JOIN bronze_orders b ON b._row_id = c._row_id
WHERE size(c.rejection_reasons) > 0
""")

# Deterministic de-duplication among valid rows: keep lowest _row_seq (first in file).
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_orders_ranked AS
SELECT *,
  ROW_NUMBER()  OVER (PARTITION BY order_id ORDER BY _row_seq) AS rn,
  FIRST_VALUE(_row_id)   OVER (PARTITION BY order_id ORDER BY _row_seq) AS winner_row_id,
  FIRST_VALUE(_row_hash) OVER (PARTITION BY order_id ORDER BY _row_seq) AS winner_row_hash
FROM v_orders_checked
WHERE size(rejection_reasons) = 0
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_orders AS
SELECT order_id, customer_id, order_date, category, quantity, unit_price,
       CAST(quantity * unit_price AS DECIMAL(18,2)) AS order_value,
       order_status, country,
       _row_id, _row_hash, _batch_id, _source_file, _ingested_at,
       current_timestamp() AS _silver_at
FROM v_orders_ranked
WHERE rn = 1
""")

spark.sql("""
CREATE OR REPLACE TABLE duplicate_losers_orders AS
SELECT b.*, r.order_id AS business_key, r.winner_row_id,
       CASE WHEN r.winner_row_hash = r._row_hash
            THEN 'EXACT_DUPLICATE_ROW' ELSE 'DUPLICATE_KEY_DIFFERENT_CONTENT' END AS duplicate_type,
       current_timestamp() AS _flagged_at
FROM v_orders_ranked r
JOIN bronze_orders b ON b._row_id = r._row_id
WHERE r.rn > 1
""")

# =============================================================================
# TRANSACTIONS  (needs silver_orders for the orphan check)
# =============================================================================
fx_rows = [(k, float(v)) for k, v in FX_TO_USD.items()]
spark.createDataFrame(fx_rows, "currency string, rate_to_usd double") \
     .write.format("delta").mode("overwrite").saveAsTable("ref_fx_rates")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_txn_base AS
SELECT
  _row_id, _row_seq, _row_hash, _batch_id, _source_file, _ingested_at,
  UPPER({clean('txn_id')})                          AS txn_id,
  UPPER({clean('order_id')})                        AS order_id,
  UPPER({clean('payment_method')})                  AS payment_method,
  {clean('amount')}                                 AS amount_raw,
  try_cast({clean('amount')} AS DECIMAL(18,2))      AS amount,
  UPPER({clean('currency')})                        AS currency,
  UPPER({clean('txn_status')})                      AS txn_status,
  {clean('txn_ts')}                                 AS txn_ts_raw,
  try_cast({clean('txn_ts')} AS TIMESTAMP)          AS txn_ts
FROM bronze_transactions
""")

# All three joins are many-to-one (each right side is unique on its join key),
# so they cannot multiply rows.
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_txn_parsed AS
SELECT p.*, (o.order_id IS NOT NULL) AS order_exists, (b.oid IS NOT NULL) AS order_in_bronze,
       fx.rate_to_usd
FROM v_txn_base p
LEFT JOIN (SELECT order_id FROM silver_orders) o ON p.order_id = o.order_id
LEFT JOIN (SELECT DISTINCT UPPER(TRIM(order_id)) AS oid FROM bronze_orders) b ON p.order_id = b.oid
LEFT JOIN ref_fx_rates fx ON p.currency = fx.currency
""")

TXN_RULES = [
    ("TXN_ID_MISSING",          "txn_id IS NULL"),
    ("ORDER_ID_MISSING",        "order_id IS NULL"),
    # True orphan: the order_id never appears in the source orders file.
    ("ORDER_ID_ORPHAN",         "order_id IS NOT NULL AND NOT order_in_bronze"),
    # The order exists in Bronze but was quarantined, so it is not in trusted Silver.
    ("ORDER_REJECTED_IN_SILVER", "order_in_bronze AND NOT order_exists"),
    ("PAYMENT_METHOD_MISSING",  "payment_method IS NULL"),
    ("AMOUNT_MISSING",          "amount_raw IS NULL"),
    ("AMOUNT_NOT_NUMERIC",      "amount_raw IS NOT NULL AND amount IS NULL"),
    ("AMOUNT_OUT_OF_RANGE",     f"amount <= 0 OR amount > {MAX_AMOUNT}"),
    ("CURRENCY_MISSING",        "currency IS NULL"),
    ("CURRENCY_UNSUPPORTED",    "currency IS NOT NULL AND rate_to_usd IS NULL"),
    ("TXN_STATUS_MISSING",      "txn_status IS NULL"),
    ("TXN_STATUS_INVALID",      f"txn_status IS NOT NULL AND txn_status NOT IN ({sql_list(TXN_STATUSES)})"),
    ("TXN_TS_MISSING",          "txn_ts_raw IS NULL"),
    ("TXN_TS_UNPARSABLE",       "txn_ts_raw IS NOT NULL AND txn_ts IS NULL"),
    ("TXN_TS_FUTURE",           "txn_ts > current_timestamp()"),
    ("TXN_TS_TOO_OLD",          f"txn_ts < TIMESTAMP'{MIN_DATE} 00:00:00'"),
]
if PAYMENT_METHODS:
    TXN_RULES.append(("PAYMENT_METHOD_INVALID", f"payment_method IS NOT NULL AND payment_method NOT IN ({sql_list(PAYMENT_METHODS)})"))
RULES_MATRIX += [("transactions", c, cond) for c, cond in TXN_RULES]

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_txn_checked AS
SELECT *, {reasons_sql(TXN_RULES)} AS rejection_reasons
FROM v_txn_parsed
""")

spark.sql("""
CREATE OR REPLACE TABLE quarantine_transactions AS
SELECT b.*, c.rejection_reasons,
       concat_ws('; ', c.rejection_reasons) AS rejection_reason,
       current_timestamp() AS _quarantined_at
FROM v_txn_checked c
JOIN bronze_transactions b ON b._row_id = c._row_id
WHERE size(c.rejection_reasons) > 0
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW v_txn_ranked AS
SELECT *,
  ROW_NUMBER()  OVER (PARTITION BY txn_id ORDER BY _row_seq) AS rn,
  FIRST_VALUE(_row_id)   OVER (PARTITION BY txn_id ORDER BY _row_seq) AS winner_row_id,
  FIRST_VALUE(_row_hash) OVER (PARTITION BY txn_id ORDER BY _row_seq) AS winner_row_hash
FROM v_txn_checked
WHERE size(rejection_reasons) = 0
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_transactions AS
SELECT txn_id, order_id, payment_method, amount, currency,
       CAST(amount * rate_to_usd AS DECIMAL(18,2)) AS amount_usd,
       txn_status, txn_ts, CAST(txn_ts AS DATE) AS txn_date,
       _row_id, _row_hash, _batch_id, _source_file, _ingested_at,
       current_timestamp() AS _silver_at
FROM v_txn_ranked
WHERE rn = 1
""")

spark.sql("""
CREATE OR REPLACE TABLE duplicate_losers_transactions AS
SELECT b.*, r.txn_id AS business_key, r.winner_row_id,
       CASE WHEN r.winner_row_hash = r._row_hash
            THEN 'EXACT_DUPLICATE_ROW' ELSE 'DUPLICATE_KEY_DIFFERENT_CONTENT' END AS duplicate_type,
       current_timestamp() AS _flagged_at
FROM v_txn_ranked r
JOIN bronze_transactions b ON b._row_id = r._row_id
WHERE r.rn > 1
""")

# =============================================================================
# Evidence tables
# =============================================================================
spark.createDataFrame(RULES_MATRIX, "dataset string, rule_code string, condition string") \
     .write.format("delta").mode("overwrite").saveAsTable("silver_rules_matrix")

spark.sql("""
CREATE OR REPLACE TABLE silver_rule_counts AS
SELECT 'orders' AS dataset, reason AS rule_code, COUNT(*) AS rows_failing
FROM (SELECT explode(rejection_reasons) AS reason FROM quarantine_orders) GROUP BY reason
UNION ALL
SELECT 'transactions', reason, COUNT(*)
FROM (SELECT explode(rejection_reasons) AS reason FROM quarantine_transactions) GROUP BY reason
""")

# COMMAND ----------

# Rows per rule (one row can fail several rules, so these do not add up to the quarantine total).
display(spark.sql("""
  SELECT m.dataset, m.rule_code, COALESCE(c.rows_failing, 0) AS rows_failing, m.condition
  FROM silver_rules_matrix m LEFT JOIN silver_rule_counts c
    ON m.dataset = c.dataset AND m.rule_code = c.rule_code
  ORDER BY m.dataset, rows_failing DESC"""))

# COMMAND ----------

# Sanity check: if one rule rejects a huge share, your lists in 00_config.py are probably wrong.
display(spark.sql("""
  SELECT 'orders' AS dataset, (SELECT COUNT(*) FROM bronze_orders) AS bronze_rows,
         (SELECT COUNT(*) FROM silver_orders) AS accepted,
         (SELECT COUNT(*) FROM quarantine_orders) AS quarantined,
         (SELECT COUNT(*) FROM duplicate_losers_orders) AS duplicate_losers
  UNION ALL
  SELECT 'transactions', (SELECT COUNT(*) FROM bronze_transactions),
         (SELECT COUNT(*) FROM silver_transactions),
         (SELECT COUNT(*) FROM quarantine_transactions),
         (SELECT COUNT(*) FROM duplicate_losers_transactions)"""))

# COMMAND ----------

# Before / after conversion examples (raw string -> typed value)
display(spark.sql("""
  SELECT b.order_id AS raw_order_id, b.order_date AS raw_order_date, b.quantity AS raw_quantity,
         b.unit_price AS raw_unit_price, s.order_id, s.order_date, s.quantity, s.unit_price, s.order_value
  FROM silver_orders s JOIN bronze_orders b ON b._row_id = s._row_id LIMIT 10"""))

# COMMAND ----------

# Samples: quarantined and duplicate rows
display(spark.sql("SELECT * FROM quarantine_orders LIMIT 10"))

# COMMAND ----------

display(spark.sql("SELECT * FROM quarantine_transactions LIMIT 10"))

# COMMAND ----------

# Duplicate winners and losers side by side (orders)
display(spark.sql("""
  SELECT l.business_key, l.duplicate_type, l._row_id AS loser_row_id, l.winner_row_id,
         w.order_date AS winner_order_date, l.order_date AS loser_order_date_raw
  FROM duplicate_losers_orders l JOIN silver_orders w ON w._row_id = l.winner_row_id LIMIT 10"""))