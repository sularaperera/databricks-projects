# Databricks notebook source
# MAGIC %md
# MAGIC # 05_silver_payments: clean the bank file, match payments to accounts, never double-count money
# MAGIC * **Key = `bank_txn_id`.** The bank sometimes re-sends a transaction (in the same file or the next day).
# MAGIC   Dedupe inside the batch, then MERGE on the key, so each transaction is counted once.
# MAGIC * **Late payments** (for example, an October payment arriving on 15 Nov) keep their real `value_date`,
# MAGIC   so they land in the right month.
# MAGIC * **Unknown account ref** goes to quarantine. Money we can't match must be investigated, not dropped.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

bronze_t = tbl(BRONZE, "bank_payments")
batch = spark.table(bronze_t).filter(F.col("_file_date") == RUN_DATE) if table_exists(bronze_t) else None
if batch is None or batch.limit(1).count() == 0:
    log_step("05_silver_payments", status="no_data")
    dbutils.notebook.exit("no bank file for this date")

file_name = batch.select("_source_file").first()[0]
run_date = F.lit(RUN_DATE).cast("date")

# COMMAND ----------

cleaned = batch.select(
    clean_text("bank_txn_id", "upper").alias("bank_txn_id"),
    clean_date("value_date").alias("value_date"),
    clean_text("client_account_ref", "upper").alias("client_account_ref"),
    clean_text("payer_name", "upper").alias("payer_name"),
    clean_money("amount").alias("amount"),
    clean_text("transaction_type", "upper").alias("transaction_type"),
    F.to_json(F.struct(*[c for c in batch.columns if not c.startswith("_")])).alias("raw_record"),
    "_source_file", "_file_date", "_row_id",
)

accounts = spark.table(tbl(SILVER, "accounts")).select("client_account_ref", "client_code")
checked = (cleaned.join(accounts, "client_account_ref", "left")
    .withColumn("reason", reasons(
        (F.col("bank_txn_id").isNull(), "missing bank txn id"),
        (F.col("value_date").isNull(), "invalid value date"),
        (F.col("value_date") > run_date, "value date in the future"),
        (F.col("amount").isNull() | (F.col("amount") == 0), "invalid amount"),
        (F.col("client_code").isNull(), "unknown account ref"),
    )))

bad = checked.filter("reason != ''")
replace_slice(bad.select("bank_txn_id", "client_account_ref", "amount", "reason", "raw_record", "_source_file", "_file_date"),
              tbl(SILVER, "payments_quarantine"), f"_source_file = '{file_name}'")

# COMMAND ----------

good = checked.filter("reason = ''")
in_file_dupes = good.count() - good.select("bank_txn_id").distinct().count()

payments = (good
    .withColumn("rn", F.row_number().over(Window.partitionBy("bank_txn_id").orderBy(F.col("_row_id").desc())))
    .filter("rn = 1")
    .select("bank_txn_id", "value_date", "client_code", "client_account_ref", "payer_name", "amount", "transaction_type",
            (F.col("amount") < 0).alias("is_reversal"),
            F.col("_source_file").alias("first_seen_file"))
    .withColumn("row_hash", row_hash("value_date", "client_account_ref", "amount", "transaction_type"))
    .withColumn("loaded_date", run_date))

m = upsert(payments, tbl(SILVER, "payments"), ["bank_txn_id"], update_exclude=("first_seen_file", "loaded_date"))
log_step("05_silver_payments", rows_in=batch.count(), quarantined=bad.count(), in_file_duplicates=in_file_dupes,
         inserted=m["inserted"], updated=m["updated"], already_loaded=payments.count() - m["inserted"] - m["updated"])

show(bad.select("bank_txn_id", "client_account_ref", "amount", "reason"))
