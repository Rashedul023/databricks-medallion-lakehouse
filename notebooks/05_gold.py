# Databricks notebook source
# GOLD: business-ready tables.  Source of truth: silver_transactions (payments) joined to
# silver_orders (order attributes).  Payments are summed per order FIRST, then joined
# one-to-one to the order, so payment amounts are never multiplied.

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

NON_REV = sql_list(NON_REVENUE_ORDER_STATUSES)
CANCELLED = sql_list(CANCELLED_ORDER_STATUSES)
SUCCESS = sql_list(SUCCESS_TXN_STATUSES)
FAILED = sql_list(FAILED_TXN_STATUSES)

# 1) Helper fact: one row per order (grain = order_id)
spark.sql(f"""
CREATE OR REPLACE TABLE gold_order_revenue AS
SELECT o.order_id, o.customer_id, o.order_date, o.category, o.country, o.order_status,
       o.order_value,
       CAST(COALESCE(p.paid_usd, 0) AS DECIMAL(18,2))          AS paid_usd,
       COALESCE(p.successful_payments, 0)                      AS successful_payments,
       CAST(CASE WHEN o.order_status IN ({NON_REV}) THEN 0
                 ELSE COALESCE(p.paid_usd, 0) END AS DECIMAL(18,2)) AS revenue_usd,
       CASE WHEN o.order_status IN ({CANCELLED}) THEN 1 ELSE 0 END AS is_cancelled
FROM silver_orders o
LEFT JOIN (
  SELECT order_id, SUM(amount_usd) AS paid_usd, COUNT(*) AS successful_payments
  FROM silver_transactions
  WHERE txn_status IN ({SUCCESS})
  GROUP BY order_id
) p ON o.order_id = p.order_id
""")

# 2) Daily sales and cancellation rate (grain = order_date)
spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT order_date,
       COUNT(*)                                          AS orders_count,
       SUM(is_cancelled)                                 AS cancelled_orders,
       CAST(SUM(is_cancelled) AS DOUBLE) / COUNT(*)      AS cancellation_rate,
       SUM(CASE WHEN revenue_usd > 0 THEN 1 ELSE 0 END)  AS paying_orders,
       CAST(SUM(revenue_usd) AS DECIMAL(18,2))           AS revenue_usd
FROM gold_order_revenue
GROUP BY order_date
""")

# 3) Revenue by category and country (grain = order_date, category, country)
spark.sql("""
CREATE OR REPLACE TABLE gold_revenue_breakdown AS
SELECT order_date, category, country,
       COUNT(*)                                AS orders_count,
       CAST(SUM(revenue_usd) AS DECIMAL(18,2)) AS revenue_usd
FROM gold_order_revenue
GROUP BY order_date, category, country
""")

# 4) Payment success by method (grain = txn_date, payment_method)
spark.sql(f"""
CREATE OR REPLACE TABLE gold_payment_success AS
SELECT txn_date, payment_method,
       COUNT(*)                                                         AS total_txns,
       SUM(CASE WHEN txn_status IN ({SUCCESS}) THEN 1 ELSE 0 END)       AS successful_txns,
       SUM(CASE WHEN txn_status IN ({FAILED})  THEN 1 ELSE 0 END)       AS failed_txns,
       CAST(SUM(CASE WHEN txn_status IN ({SUCCESS}) THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS success_rate,
       CAST(SUM(CASE WHEN txn_status IN ({SUCCESS}) THEN amount_usd ELSE 0 END) AS DECIMAL(18,2)) AS successful_amount_usd
FROM silver_transactions
GROUP BY txn_date, payment_method
""")

# 5) Top customers by spend (grain = customer_id, top 1000)
spark.sql("""
CREATE OR REPLACE TABLE gold_top_customers AS
SELECT * FROM (
  SELECT customer_id,
         COUNT(*)                                AS orders_count,
         CAST(SUM(revenue_usd) AS DECIMAL(18,2)) AS revenue_usd,
         ROW_NUMBER() OVER (ORDER BY SUM(revenue_usd) DESC, customer_id) AS spend_rank
  FROM gold_order_revenue
  GROUP BY customer_id
) WHERE spend_rank <= 1000
""")

# COMMAND ----------

# Documentation: table and column comments (visible in Catalog Explorer)
TABLES = {
    "gold_order_revenue": ("One row per accepted order with its paid and recognised revenue in USD. Helper table for the other Gold tables. Key: order_id.", {
        "order_id": "Unique order key (trimmed, upper case).",
        "customer_id": "Customer key from the order.",
        "order_date": "Order date. All revenue reporting uses this date.",
        "category": "Product category (trimmed, initcap).",
        "country": "Order country (trimmed, initcap).",
        "order_status": "Order status (upper case).",
        "order_value": "quantity x unit_price. Informational, not revenue.",
        "paid_usd": "Sum of successful payments in USD, before order status rules.",
        "successful_payments": "Number of successful payments on the order (repeat or partial payments are counted).",
        "revenue_usd": "Recognised revenue in USD: paid_usd, or 0 if the order status is in the non-revenue list.",
        "is_cancelled": "1 if the order status is cancelled, else 0."}),
    "gold_daily_sales": ("Daily orders, revenue and cancellation rate. Grain: one row per order_date. Key: order_date.", {
        "order_date": "Order date (unique key).",
        "orders_count": "Accepted orders on the date.",
        "cancelled_orders": "Orders with a cancelled status.",
        "cancellation_rate": "cancelled_orders / orders_count. For a period, divide the sums, do not average this column.",
        "paying_orders": "Orders with revenue above 0.",
        "revenue_usd": "Recognised revenue in USD."}),
    "gold_revenue_breakdown": ("Revenue by category and country per day. Grain: order_date x category x country. Key: those three columns.", {
        "order_date": "Order date.",
        "category": "Product category.",
        "country": "Order country.",
        "orders_count": "Accepted orders in the group.",
        "revenue_usd": "Recognised revenue in USD."}),
    "gold_payment_success": ("Payment success by method per day. Grain: txn_date x payment_method. Key: those two columns.", {
        "txn_date": "Date of the payment transaction.",
        "payment_method": "Payment method (upper case).",
        "total_txns": "All valid transactions in the group, any status.",
        "successful_txns": "Transactions with a success status.",
        "failed_txns": "Transactions with a failed status.",
        "success_rate": "successful_txns / total_txns. For a period, divide the sums.",
        "successful_amount_usd": "Amount of successful transactions in USD (not filtered by order status)."}),
    "gold_top_customers": ("Top 1000 customers by recognised revenue. Grain: one row per customer_id. Key: customer_id.", {
        "customer_id": "Customer key (unique).",
        "orders_count": "Accepted orders of the customer.",
        "revenue_usd": "Recognised revenue in USD over all dates.",
        "spend_rank": "1 = highest spend. Ties broken by customer_id."}),
}
for t, (tcomment, cols) in TABLES.items():
    spark.sql(f"COMMENT ON TABLE {t} IS '{tcomment}'")
    for c, text in cols.items():
        spark.sql(f"ALTER TABLE {t} ALTER COLUMN {c} COMMENT '{text}'")

# COMMAND ----------

# Gold checks: grain is unique, and revenue agrees across Gold tables and the Silver recomputation.
GRAINS = {
    "gold_order_revenue": ["order_id"],
    "gold_daily_sales": ["order_date"],
    "gold_revenue_breakdown": ["order_date", "category", "country"],
    "gold_payment_success": ["txn_date", "payment_method"],
    "gold_top_customers": ["customer_id"],
}
for t, keys in GRAINS.items():
    k = ", ".join(keys)
    dups = spark.sql(f"SELECT COUNT(*) FROM (SELECT {k} FROM {t} GROUP BY {k} HAVING COUNT(*) > 1)").first()[0]
    assert dups == 0, f"{t}: grain ({k}) is not unique"

rev = spark.sql(f"""
SELECT
  (SELECT SUM(revenue_usd) FROM gold_daily_sales)       AS daily_sales,
  (SELECT SUM(revenue_usd) FROM gold_revenue_breakdown) AS breakdown,
  (SELECT SUM(revenue_usd) FROM gold_order_revenue)     AS order_level,
  (SELECT SUM(t.amount_usd) FROM silver_transactions t
     JOIN silver_orders o ON t.order_id = o.order_id
    WHERE t.txn_status IN ({SUCCESS}) AND o.order_status NOT IN ({NON_REV})) AS independent_from_silver
""").first()
print(rev)
assert rev["daily_sales"] == rev["breakdown"] == rev["order_level"] == rev["independent_from_silver"], "Revenue mismatch"

# COMMAND ----------

# Evidence: row counts and sample output
display(spark.sql("""
  SELECT 'gold_order_revenue' AS tbl, COUNT(*) AS rows FROM gold_order_revenue UNION ALL
  SELECT 'gold_daily_sales', COUNT(*) FROM gold_daily_sales UNION ALL
  SELECT 'gold_revenue_breakdown', COUNT(*) FROM gold_revenue_breakdown UNION ALL
  SELECT 'gold_payment_success', COUNT(*) FROM gold_payment_success UNION ALL
  SELECT 'gold_top_customers', COUNT(*) FROM gold_top_customers"""))

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_daily_sales ORDER BY order_date DESC LIMIT 10"))

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_revenue_breakdown ORDER BY revenue_usd DESC LIMIT 10"))

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_payment_success ORDER BY txn_date DESC, payment_method LIMIT 10"))

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_top_customers ORDER BY spend_rank LIMIT 10"))
