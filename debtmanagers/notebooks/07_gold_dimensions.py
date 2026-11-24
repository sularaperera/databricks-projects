# Databricks notebook source
# MAGIC %md
# MAGIC # 07_gold_dimensions: the "who, what, when" of the star schema
# MAGIC | Dimension | One row = | Notes |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | one calendar day | NZ financial year (April to March), weekends |
# MAGIC | `dim_client` | one client | from silver (SCD1) |
# MAGIC | `dim_debtor` | one **version** of a debtor | SCD2 from silver. Phone and email are **masked**: gold is for reporting, not for contacting people. |
# MAGIC | `dim_account` | one debt | links to client and debtor |
# MAGIC
# MAGIC The surrogate keys (`*_sk`) are an `xxhash64` of the business key (plus `valid_from` for SCD2). The same input
# MAGIC always gives the same key, so a rebuild gives the same keys and facts never point at the wrong row.
# MAGIC Dimensions are small, so we simply rebuild them every run (`CREATE OR REPLACE TABLE`). That's safe to rerun.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD}.dim_date USING DELTA AS
    SELECT
        CAST(date_format(date, 'yyyyMMdd') AS INT)                         AS date_key,
        date,
        year(date)                                                          AS year,
        month(date)                                                         AS month,
        date_format(date, 'yyyy-MM')                                        AS year_month,
        date_format(date, 'MMMM')                                           AS month_name,
        quarter(date)                                                       AS quarter,
        CASE WHEN month(date) >= 4 THEN year(date) + 1 ELSE year(date) END  AS nz_financial_year,
        dayofweek(date) IN (1, 7)                                           AS is_weekend
    FROM (SELECT explode(sequence(DATE'2025-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS date)
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD}.dim_client USING DELTA AS
    SELECT
        xxhash64(client_code) AS client_sk,
        client_code,
        client_name,
        industry,
        commission_rate
    FROM {SILVER}.clients
""")

# COMMAND ----------

# Masking: '+64215550101' -> '+64 *** *101' and 'aroha@example.com' -> 'a***@example.com'
# birth_year instead of age: age changes every day, so a rerun would give different results
spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD}.dim_debtor USING DELTA AS
    SELECT
        xxhash64(debtor_key, valid_from)                     AS debtor_sk,
        debtor_key,
        concat_ws(' ', first_name, last_name)                AS debtor_name,
        year(date_of_birth)                                  AS birth_year,
        concat('+64 *** *', substring(phone_e164, -3, 3))    AS phone_masked,
        regexp_replace(email, '^(.).*(@.*)$', '$1***$2')     AS email_masked,
        valid_from,
        valid_to,
        is_current
    FROM {SILVER}.debtors
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD}.dim_account USING DELTA AS
    SELECT
        xxhash64(client_code, client_account_ref) AS account_sk,
        client_account_ref,
        client_code,
        xxhash64(client_code)                     AS client_sk,
        debtor_key,
        placed_date,
        placed_balance,
        first_seen_date
    FROM {SILVER}.accounts
""")

# COMMAND ----------

n_clients = spark.table(f"{GOLD}.dim_client").count()
n_debtors = spark.table(f"{GOLD}.dim_debtor").count()
n_accounts = spark.table(f"{GOLD}.dim_account").count()
log_step("07_gold_dimensions", f"dim_client={n_clients}, dim_debtor={n_debtors}, dim_account={n_accounts}")

display(spark.table(f"{GOLD}.dim_debtor").orderBy("debtor_key", "valid_from").limit(5))
