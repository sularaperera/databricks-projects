# Databricks notebook source
# MAGIC %md
# MAGIC # 05_silver_payments: clean the bank file, match payments to accounts, never count money twice
# MAGIC * **Key = `bank_txn_id`.** The bank sometimes re-sends a transaction (in the same file or the next day).
# MAGIC   We remove duplicates inside the file, then MERGE on the key, so each transaction is counted once.
# MAGIC * **Late payments** (for example an October payment arriving on 15 Nov) keep their real `value_date`,
# MAGIC   so they land in the right month.
# MAGIC * **Unknown account ref** goes to quarantine. Money we can't match must be investigated, not dropped.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

batch = read_bronze_batch("bank_payments")
if batch is None or batch.isEmpty():
    log_step("05_silver_payments", status="no_data")
    dbutils.notebook.exit("no bank file for this date")

file_name = batch.select("_source_file").first()[0]
run_date = F.lit(RUN_DATE).cast("date")

# COMMAND ----------

raw_columns = [c for c in batch.columns if not c.startswith("_")]

cleaned = batch.select(
    F.upper(clean_text("bank_txn_id")).alias("bank_txn_id"),
    clean_date("value_date").alias("value_date"),
    F.upper(clean_text("client_account_ref")).alias("client_account_ref"),
    F.upper(clean_text("payer_name")).alias("payer_name"),
    clean_money("amount").alias("amount"),
    F.upper(clean_text("transaction_type")).alias("transaction_type"),
    F.to_json(F.struct(*raw_columns)).alias("raw_record"),
    "_source_file", "_file_date", "_row_id",
)

# Look up the account. If client_code is still NULL after the join, we don't know this account.
accounts = spark.table(f"{SILVER}.accounts").select("client_account_ref", "client_code")
cleaned = cleaned.join(accounts, "client_account_ref", "left")

checked = cleaned.withColumn("reason", F.concat_ws("; ",
    F.when(F.col("bank_txn_id").isNull(), "missing bank txn id"),
    F.when(F.col("value_date").isNull(), "invalid value date"),
    F.when(F.col("value_date") > run_date, "value date in the future"),
    F.when(F.col("amount").isNull() | (F.col("amount") == 0), "invalid amount"),
    F.when(F.col("client_code").isNull(), "unknown account ref"),
))

bad = checked.filter("reason != ''")
good = checked.filter("reason = ''")

quarantine = bad.select("bank_txn_id", "client_account_ref", "amount", "reason", "raw_record", "_source_file", "_file_date")
overwrite_slice(quarantine, f"{SILVER}.payments_quarantine", f"_source_file = '{file_name}'")

# COMMAND ----------

# How many transactions appear more than once in this file (just for the log)
in_file_duplicates = good.count() - good.select("bank_txn_id").distinct().count()

# Keep one row per transaction (the last one in the file)
last_row = Window.partitionBy("bank_txn_id").orderBy(F.col("_row_id").desc())
payments = (good
    .withColumn("rn", F.row_number().over(last_row))
    .filter("rn = 1")
    .select("bank_txn_id", "value_date", "client_code", "client_account_ref", "payer_name", "amount",
            "transaction_type",
            (F.col("amount") < 0).alias("is_reversal"),
            F.col("_source_file").alias("first_seen_file"))
    .withColumn("row_hash", make_hash("value_date", "client_account_ref", "amount", "transaction_type"))
    .withColumn("loaded_date", run_date))

# first_seen_file and loaded_date are never updated: they record when we FIRST received the payment
inserted, updated = run_merge(payments, f"{SILVER}.payments", f"""
    MERGE INTO {SILVER}.payments t
    USING updates s
    ON t.bank_txn_id = s.bank_txn_id
    WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
        t.value_date         = s.value_date,
        t.client_code        = s.client_code,
        t.client_account_ref = s.client_account_ref,
        t.payer_name         = s.payer_name,
        t.amount             = s.amount,
        t.transaction_type   = s.transaction_type,
        t.is_reversal        = s.is_reversal,
        t.row_hash           = s.row_hash
    WHEN NOT MATCHED THEN INSERT *
""")

already_loaded = payments.count() - inserted - updated
log_step("05_silver_payments",
         f"rows_in={batch.count()}, quarantined={bad.count()}, in_file_duplicates={in_file_duplicates}, "
         f"inserted={inserted}, updated={updated}, already_loaded={already_loaded}")

display(bad.select("bank_txn_id", "client_account_ref", "amount", "reason"))
