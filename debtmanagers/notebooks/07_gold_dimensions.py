# Databricks notebook source
# MAGIC %md
# MAGIC # 07_gold_dimensions: the "who, what, when" of the star schema
# MAGIC | Dimension | Grain | Notes |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | one calendar day | NZ financial year (April to March), weekends |
# MAGIC | `dim_client` | one client | from silver SCD1 |
# MAGIC | `dim_debtor` | one **version** of a debtor | SCD2 from silver. Phone and email are **masked**: gold is for reporting, not contacting people. |
# MAGIC | `dim_account` | one debt | links to client and debtor |
# MAGIC
# MAGIC Surrogate keys are `xxhash64` of the business key (plus `valid_from` for SCD2), so they're **deterministic**:
# MAGIC a rebuild gives the same keys, and facts never point at the wrong row. Dimensions are small, so a full overwrite
# MAGIC each run is the simplest idempotent option.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dim_date = (spark.sql("SELECT explode(sequence(DATE'2025-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS date")
    .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
            F.year("date").alias("year"), F.month("date").alias("month"),
            F.date_format("date", "yyyy-MM").alias("year_month"), F.date_format("date", "MMMM").alias("month_name"),
            F.quarter("date").alias("quarter"),
            F.when(F.month("date") >= 4, F.year("date") + 1).otherwise(F.year("date")).alias("nz_financial_year"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend")))
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(GOLD, "dim_date"))

# COMMAND ----------

dim_client = (spark.table(tbl(SILVER, "clients"))
    .select(F.xxhash64("client_code").alias("client_sk"), "client_code", "client_name", "industry", "commission_rate"))
dim_client.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(GOLD, "dim_client"))

# COMMAND ----------

def mask_phone(c):
    return F.concat(F.lit("+64 *** *"), F.substring(F.col(c), -3, 3))


def mask_email(c):
    return F.regexp_replace(F.col(c), r"^(.).*(@.*)$", "$1***$2")


dim_debtor = (spark.table(tbl(SILVER, "debtors"))
    .select(F.xxhash64("debtor_key", "valid_from").alias("debtor_sk"), "debtor_key",
            F.concat_ws(" ", "first_name", "last_name").alias("debtor_name"),
            F.year("date_of_birth").alias("birth_year"),   # not age: age changes daily, so reruns would differ
            mask_phone("phone_e164").alias("phone_masked"),
            mask_email("email").alias("email_masked"),
            "valid_from", "valid_to", "is_current"))
dim_debtor.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(GOLD, "dim_debtor"))

# COMMAND ----------

dim_account = (spark.table(tbl(SILVER, "accounts"))
    .select(F.xxhash64("client_code", "client_account_ref").alias("account_sk"),
            "client_account_ref", "client_code", F.xxhash64("client_code").alias("client_sk"),
            "debtor_key", "placed_date", "placed_balance", "first_seen_date"))
dim_account.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(GOLD, "dim_account"))

log_step("07_gold_dimensions", dim_client=dim_client.count(), dim_debtor=dim_debtor.count(), dim_account=dim_account.count())
show(spark.table(tbl(GOLD, "dim_debtor")).orderBy("debtor_key", "valid_from"), 5)
