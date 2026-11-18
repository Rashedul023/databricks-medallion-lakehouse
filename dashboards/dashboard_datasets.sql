-- Datasets for the AI/BI dashboard (Dashboards > Create > Data tab > Add dataset).
-- Replace workspace.

-- ds_daily  (KPI counters + trend + date filter)
SELECT order_date, orders_count, cancelled_orders, paying_orders, revenue_usd
FROM workspace.ecom_poc.gold_daily_sales;

-- ds_category_country  (breakdown visuals)
SELECT order_date, category, country, orders_count, revenue_usd
FROM workspace.ecom_poc.gold_revenue_breakdown;

-- ds_payments  (payment success)
SELECT txn_date, payment_method, total_txns, successful_txns, failed_txns
FROM workspace.ecom_poc.gold_payment_success;

-- ds_top_customers
SELECT customer_id, orders_count, revenue_usd, spend_rank
FROM workspace.ecom_poc.gold_top_customers
ORDER BY spend_rank LIMIT 20;

-- KPI counter expressions (add as measures/aggregations on ds_daily):
--   Total revenue       : SUM(revenue_usd)
--   Total orders        : SUM(orders_count)
--   Cancellation rate   : SUM(cancelled_orders) / SUM(orders_count)    (format as %)
--   Avg revenue / paying order : SUM(revenue_usd) / SUM(paying_orders)
-- Payment success rate on ds_payments: SUM(successful_txns) / SUM(total_txns)
