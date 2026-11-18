# Power BI Desktop steps

1. Get Data > **Databricks** (native connector). Server hostname and HTTP path: SQL Warehouse >
   Connection details. Choose **Import**. Sign in with a personal access token or Microsoft Entra ID.
   Mask the token in any screenshot.
2. Select the four Gold tables (`gold_daily_sales`, `gold_revenue_breakdown`, `gold_payment_success`,
   `gold_top_customers`). Load.
3. Add the measures from `measures.dax`.
4. One page: 4 KPI cards, line chart (`order_date` x Total Revenue USD), bar chart (category revenue),
   slicers for date range, category and country. Note: a slicer on `gold_daily_sales[order_date]` does not
   filter `gold_revenue_breakdown` unless you relate the tables. Create a Date table related to both
   `order_date` columns, or use the same date range in both when you test.
5. Save `.pbix` in this folder, plus a screenshot.
6. If the connector is blocked: run `SELECT * FROM <gold table>` in Databricks, download the result as
   CSV into `gold_exports/` (small files only, synthetic data), load the CSVs, and say so in the README.

## Import vs DirectQuery
- Import: data copied into the report. Fast, works offline, uses warehouse only at refresh. Data is a snapshot.
- DirectQuery: each visual sends SQL to the warehouse. Always current, but slower and uses credits on every click.
- Choice here: Import, because Gold tables are small and trial credits are limited.
