-- Same total revenue in: (1) this SQL, (2) the AI/BI dashboard, (3) Power BI.
-- Pick ONE date range and use it in all three. Replace workspace and dates.
USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- (1a) From the Gold table the dashboards use
SELECT SUM(revenue_usd) AS revenue_usd_gold
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-12-31';

-- (1b) Independent recomputation from Silver (does not use any Gold table)
SELECT SUM(t.amount_usd) AS revenue_usd_from_silver
FROM silver_transactions t
JOIN silver_orders o ON t.order_id = o.order_id      -- many-to-one, cannot multiply amounts
WHERE t.txn_status IN ('SUCCESS')                    -- keep in sync with 00_config.py
  AND o.order_status NOT IN ('CANCELLED', 'RETURNED')
  AND o.order_date BETWEEN DATE'2026-01-01' AND DATE'2026-12-31';

-- Write the three results in the comparison table in README.md.
-- Common causes of mismatch: different date filter, filter applied to the wrong date column
-- (txn_date vs order_date), averaging a ratio instead of dividing sums, Power BI Import not refreshed.
