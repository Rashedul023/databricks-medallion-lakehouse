#!/usr/bin/env bash
# Run this on your own computer, inside the project folder.
# 1) Create an EMPTY repo on GitHub first (no README): databricks-medallion-lakehouse
set -e
git init
git branch -M main
git add .
git commit -m "Stage 0: project skeleton, config and notebooks"
git remote add origin https://github.com/Rashedul023/databricks-medallion-lakehouse.git
git push -u origin main

# Safety check: no CSV files should be tracked
git ls-files | grep -i '\.csv$' && echo "WARNING: CSV tracked!" || echo "OK: no CSV files tracked"

# Commit after each stage, for example:
#   git add notebooks/01_bronze.py && git commit -m "Stage 2: Bronze ingestion" && git push
#   git add notebooks/03_silver.py notebooks/04_reconciliation.py && git commit -m "Stage 3: Silver" && git push
#   git add notebooks/05_gold.py sql/ && git commit -m "Stage 4: Gold" && git push
