-- Each query should return 0 rows (or TRUE where stated). Replace workspace.
USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- A1. Source rows = Bronze rows (expect 0 rows)
SELECT * FROM bronze_load_log WHERE NOT row_count_match;

-- A2. Bronze = accepted + quarantined + duplicate losers, no overlap (expect 0 rows)
SELECT * FROM recon_summary WHERE NOT reconciles;

-- A3. Silver business keys are unique (expect 0 rows each)
SELECT order_id, COUNT(*) FROM silver_orders GROUP BY order_id HAVING COUNT(*) > 1;
SELECT txn_id,   COUNT(*) FROM silver_transactions GROUP BY txn_id HAVING COUNT(*) > 1;

-- A4. No orphan transactions in Silver (expect 0 rows)
SELECT t.txn_id FROM silver_transactions t
LEFT JOIN silver_orders o ON t.order_id = o.order_id
WHERE o.order_id IS NULL;

-- A5. No future or invalid values in Silver (expect 0 rows)
SELECT order_id FROM silver_orders
WHERE order_date > current_date() OR quantity < 1 OR unit_price <= 0;
SELECT txn_id FROM silver_transactions
WHERE txn_ts > current_timestamp() OR amount <= 0;

-- A6. Gold grain is unique (expect 0 rows each)
SELECT order_date, COUNT(*) FROM gold_daily_sales GROUP BY order_date HAVING COUNT(*) > 1;
SELECT order_date, category, country, COUNT(*) FROM gold_revenue_breakdown GROUP BY 1,2,3 HAVING COUNT(*) > 1;
SELECT txn_date, payment_method, COUNT(*) FROM gold_payment_success GROUP BY 1,2 HAVING COUNT(*) > 1;
SELECT customer_id, COUNT(*) FROM gold_top_customers GROUP BY customer_id HAVING COUNT(*) > 1;

-- A7. Total revenue is the same in every Gold table (expect one row, all equal)
SELECT (SELECT SUM(revenue_usd) FROM gold_daily_sales)       AS daily_sales,
       (SELECT SUM(revenue_usd) FROM gold_revenue_breakdown) AS breakdown,
       (SELECT SUM(revenue_usd) FROM gold_order_revenue)     AS order_level;
