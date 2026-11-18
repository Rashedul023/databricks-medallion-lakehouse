# Databricks notebook source
# Shared settings for all notebooks. Use it with:   %run ./00_config
# The calling notebook must define CATALOG before the %run cell.

SCHEMA = "ecom_poc"
VOLUME = "raw_files"
FQ = f"{CATALOG}.{SCHEMA}"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
ORDERS_FILE = f"{RAW_PATH}/orders.csv"
TXN_FILE = f"{RAW_PATH}/transactions.csv"

ORDER_COLS = ["order_id", "customer_id", "order_date", "category",
              "quantity", "unit_price", "order_status", "country"]
TXN_COLS = ["txn_id", "order_id", "payment_method", "amount",
            "currency", "txn_status", "txn_ts"]

# Text values treated as "missing" after trimming (case-insensitive).
# Only a blank field and the literal text NULL are missing values in these files.
# Other text such as N/A or abc is NOT missing: it is bad data and gets its own reason.
MISSING_TOKENS = ["", "NULL"]

# ---------------------------------------------------------------------------
# BUSINESS RULES (set from the data description of orders.csv and transactions.csv).
# Lists are compared in UPPER CASE after trimming. Use None to skip a list check.
# ---------------------------------------------------------------------------
ORDER_STATUSES = ["DELIVERED", "SHIPPED", "CONFIRMED", "PENDING", "CANCELLED", "RETURNED"]
CANCELLED_ORDER_STATUSES = ["CANCELLED"]                 # used for cancellation rate
NON_REVENUE_ORDER_STATUSES = ["CANCELLED", "RETURNED"]   # these orders carry no revenue

TXN_STATUSES = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]
SUCCESS_TXN_STATUSES = ["SUCCESS"]                       # counted as revenue
FAILED_TXN_STATUSES = ["FAILED"]

PAYMENT_METHODS = ["CARD", "MOBILE_WALLET", "BANK_TRANSFER", "PAYPAL", "CASH_ON_DELIVERY"]
CATEGORIES = ["Electronics", "Fashion", "Home & Kitchen", "Beauty", "Sports", "Books", "Toys", "Grocery"]
COUNTRIES = ["Bangladesh", "India", "United States", "United Kingdom", "Germany",
             "Canada", "Australia", "Singapore", "United Arab Emirates"]

# Supported currencies. The source made EUR = USD x 0.92 and GBP = USD x 0.79,
# so the rate back to USD is the inverse. Document this in the README.
FX_TO_USD = {"USD": 1.0, "EUR": 1 / 0.92, "GBP": 1 / 0.79}

MAX_QTY = 5            # clean quantity is 1 to 5
MAX_UNIT_PRICE = 1200  # clean unit_price is 1.00 to 1200.00
MAX_AMOUNT = 6000      # largest clean amount is 5 x 1200 (USD)
MIN_DATE = "2025-01-01"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sql_list(values):
    """['a','b'] -> 'A','B'  (upper case, quoted, ready for IN (...))."""
    return ", ".join("'" + str(v).upper().replace("'", "''") + "'" for v in values)


def clean(col):
    """SQL expression: trim text, and turn missing tokens into NULL."""
    toks = sql_list(MISSING_TOKENS)
    return f"CASE WHEN UPPER(TRIM({col})) IN ({toks}) THEN NULL ELSE TRIM({col}) END"


def reasons_sql(rules):
    """Build an ARRAY of rejection reasons from [(code, condition), ...]."""
    parts = ",\n    ".join(f"CASE WHEN {cond} THEN '{code}' END" for code, cond in rules)
    return f"filter(array(\n    {parts}\n  ), x -> x IS NOT NULL)"
