-- Metric definitions used by BOTH the AI/BI dashboard and Power BI.
-- Replace workspace.
USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- Total revenue (USD)         = SUM(gold_daily_sales.revenue_usd)
-- Total orders                = SUM(gold_daily_sales.orders_count)
-- Cancellation rate           = SUM(cancelled_orders) / SUM(orders_count)      (ratio of sums)
-- Average revenue per paying order = SUM(revenue_usd) / SUM(paying_orders)
-- Payment success rate        = SUM(successful_txns) / SUM(total_txns)         (ratio of sums)

-- Example: all KPIs for a date range
SELECT SUM(revenue_usd)                                   AS total_revenue_usd,
       SUM(orders_count)                                  AS total_orders,
       SUM(cancelled_orders) / SUM(orders_count)          AS cancellation_rate,
       SUM(revenue_usd) / NULLIF(SUM(paying_orders), 0)   AS avg_revenue_per_paying_order
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-12-31';
