-- Run in a SQL editor / notebook. Replace workspace first.
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS ecom_poc COMMENT 'E-commerce medallion assignment (synthetic data only)';
USE SCHEMA ecom_poc;
CREATE VOLUME IF NOT EXISTS raw_files COMMENT 'Raw, unchanged source CSV files';

-- Upload orders.csv and transactions.csv to:
--   /Volumes/workspace/ecom_poc/raw_files/

-- Evidence: file names and byte sizes
LIST '/Volumes/workspace/ecom_poc/raw_files/';

-- Evidence: data rows per file (lines minus the header)
SELECT 'orders.csv' AS file, COUNT(*) - 1 AS data_rows
FROM read_files('/Volumes/workspace/ecom_poc/raw_files/orders.csv', format => 'text')
UNION ALL
SELECT 'transactions.csv', COUNT(*) - 1
FROM read_files('/Volumes/workspace/ecom_poc/raw_files/transactions.csv', format => 'text');
