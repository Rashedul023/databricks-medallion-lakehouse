-- Least privilege: reporting users can read Gold tables only. Replace workspace.
-- `reporting_users` must be an existing group or user. If you cannot create one in the trial,
-- keep this file as the grant plan and explain the restriction in the README.

GRANT USE CATALOG ON CATALOG workspace TO `reporting_users`;
GRANT USE SCHEMA  ON SCHEMA  workspace.ecom_poc TO `reporting_users`;

-- Table-level grants. Do NOT grant SELECT on the whole schema: Bronze and Silver share it.
GRANT SELECT ON TABLE workspace.ecom_poc.gold_daily_sales        TO `reporting_users`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_revenue_breakdown  TO `reporting_users`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_payment_success    TO `reporting_users`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_top_customers      TO `reporting_users`;
-- gold_order_revenue is a helper table (order level). Leave it out unless needed.

-- Also grant CAN USE on the SQL warehouse (workspace UI: SQL Warehouses > Permissions).

-- Evidence
SHOW GRANTS ON TABLE workspace.ecom_poc.gold_daily_sales;

-- How to test: sign in (or use a second account) as a member of reporting_users, then run:
--   SELECT * FROM workspace.ecom_poc.gold_daily_sales LIMIT 5;   -- must work
--   SELECT * FROM workspace.ecom_poc.silver_orders LIMIT 5;      -- must fail (permission denied)
--   SELECT * FROM workspace.ecom_poc.bronze_orders LIMIT 5;      -- must fail
