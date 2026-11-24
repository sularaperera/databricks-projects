# Databricks notebook source
# MAGIC %md
# MAGIC # 06_silver_contacts: calls, SMS, emails and letters from the dialler
# MAGIC Same steps as payments: clean, check against the allowed values, quarantine, remove duplicates, MERGE on `attempt_id`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

batch = read_bronze_batch("contact_attempts")
if batch is None or batch.isEmpty():
    log_step("06_silver_contacts", status="no_data")
    dbutils.notebook.exit("no contact file for this date")

file_name = batch.select("_source_file").first()[0]

ALLOWED_CHANNELS = ["call", "sms", "email", "letter"]
ALLOWED_OUTCOMES = ["no_answer", "right_party_contact", "promise_to_pay", "wrong_number", "refused", "sent"]

# COMMAND ----------

raw_columns = [c for c in batch.columns if not c.startswith("_")]

cleaned = batch.select(
    F.upper(clean_text("attempt_id")).alias("attempt_id"),
    F.upper(clean_text("client_account_ref")).alias("client_account_ref"),
    F.expr("try_to_timestamp(trim(attempt_ts), 'yyyy-MM-dd HH:mm')").alias("attempt_ts"),
    F.lower(clean_text("channel")).alias("channel"),
    F.lower(clean_text("outcome")).alias("outcome"),
    F.upper(clean_text("agent_id")).alias("agent_id"),
    F.to_json(F.struct(*raw_columns)).alias("raw_record"),
    "_source_file", "_file_date", "_row_id",
)

accounts = spark.table(f"{SILVER}.accounts").select("client_account_ref", "client_code")
cleaned = cleaned.join(accounts, "client_account_ref", "left")

checked = cleaned.withColumn("reason", F.concat_ws("; ",
    F.when(F.col("attempt_id").isNull(), "missing attempt id"),
    F.when(F.col("attempt_ts").isNull(), "invalid timestamp"),
    F.when(F.col("channel").isNull() | ~F.col("channel").isin(ALLOWED_CHANNELS), "unknown channel"),
    F.when(F.col("outcome").isNull() | ~F.col("outcome").isin(ALLOWED_OUTCOMES), "unknown outcome"),
    F.when(F.col("client_code").isNull(), "unknown account ref"),
))

bad = checked.filter("reason != ''")
good = checked.filter("reason = ''")

quarantine = bad.select("attempt_id", "client_account_ref", "reason", "raw_record", "_source_file", "_file_date")
overwrite_slice(quarantine, f"{SILVER}.contacts_quarantine", f"_source_file = '{file_name}'")

# COMMAND ----------

# Keep one row per attempt (the last one in the file)
last_row = Window.partitionBy("attempt_id").orderBy(F.col("_row_id").desc())
contacts = (good
    .withColumn("rn", F.row_number().over(last_row))
    .filter("rn = 1")
    .select("attempt_id", "client_code", "client_account_ref", "attempt_ts", "channel", "outcome", "agent_id")
    .withColumn("row_hash", make_hash("client_account_ref", "attempt_ts", "channel", "outcome", "agent_id"))
    .withColumn("loaded_date", F.lit(RUN_DATE).cast("date")))

# loaded_date is never updated: it records when we first received the attempt
inserted, updated = run_merge(contacts, f"{SILVER}.contact_attempts", f"""
    MERGE INTO {SILVER}.contact_attempts t
    USING updates s
    ON t.attempt_id = s.attempt_id
    WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
        t.client_code        = s.client_code,
        t.client_account_ref = s.client_account_ref,
        t.attempt_ts         = s.attempt_ts,
        t.channel            = s.channel,
        t.outcome            = s.outcome,
        t.agent_id           = s.agent_id,
        t.row_hash           = s.row_hash
    WHEN NOT MATCHED THEN INSERT *
""")

log_step("06_silver_contacts", f"rows_in={batch.count()}, quarantined={bad.count()}, inserted={inserted}, updated={updated}")
