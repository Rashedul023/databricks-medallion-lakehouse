-- Three Genie questions and the SQL that verifies each answer. Replace workspace.
USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- Q1. "What was total revenue and the cancellation rate in 2026?"
SELECT SUM(revenue_usd) AS revenue_usd,
       SUM(cancelled_orders) / SUM(orders_count) AS cancellation_rate
FROM gold_daily_sales
WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-12-31';

-- Q2. "Which 5 categories have the highest revenue?"
SELECT category, SUM(revenue_usd) AS revenue_usd
FROM gold_revenue_breakdown GROUP BY category ORDER BY revenue_usd DESC LIMIT 5;

-- Q3. "Which payment method has the lowest success rate?"
SELECT payment_method, SUM(successful_txns) / SUM(total_txns) AS success_rate
FROM gold_payment_success GROUP BY payment_method ORDER BY success_rate ASC LIMIT 1;
