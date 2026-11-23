# Databricks notebook source
# MAGIC %md
# MAGIC # 06_silver_contacts: calls, SMS, emails and letters from the dialler
# MAGIC Same pattern as payments: clean, validate against allowed values, quarantine, dedupe, MERGE on `attempt_id`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

bronze_t = tbl(BRONZE, "contact_attempts")
batch = spark.table(bronze_t).filter(F.col("_file_date") == RUN_DATE) if table_exists(bronze_t) else None
if batch is None or batch.limit(1).count() == 0:
    log_step("06_silver_contacts", status="no_data")
    dbutils.notebook.exit("no contact file for this date")

file_name = batch.select("_source_file").first()[0]
CHANNELS = ["call", "sms", "email", "letter"]
OUTCOMES = ["no_answer", "right_party_contact", "promise_to_pay", "wrong_number", "refused", "sent"]

cleaned = batch.select(
    clean_text("attempt_id", "upper").alias("attempt_id"),
    clean_text("client_account_ref", "upper").alias("client_account_ref"),
    F.expr("try_to_timestamp(trim(attempt_ts), 'yyyy-MM-dd HH:mm')").alias("attempt_ts"),
    clean_text("channel", "lower").alias("channel"),
    clean_text("outcome", "lower").alias("outcome"),
    clean_text("agent_id", "upper").alias("agent_id"),
    F.to_json(F.struct(*[c for c in batch.columns if not c.startswith("_")])).alias("raw_record"),
    "_source_file", "_file_date", "_row_id",
)

accounts = spark.table(tbl(SILVER, "accounts")).select("client_account_ref", "client_code")
checked = (cleaned.join(accounts, "client_account_ref", "left")
    .withColumn("reason", reasons(
        (F.col("attempt_id").isNull(), "missing attempt id"),
        (F.col("attempt_ts").isNull(), "invalid timestamp"),
        (~F.col("channel").isin(CHANNELS) | F.col("channel").isNull(), "unknown channel"),
        (~F.col("outcome").isin(OUTCOMES) | F.col("outcome").isNull(), "unknown outcome"),
        (F.col("client_code").isNull(), "unknown account ref"),
    )))

bad = checked.filter("reason != ''")
replace_slice(bad.select("attempt_id", "client_account_ref", "reason", "raw_record", "_source_file", "_file_date"),
              tbl(SILVER, "contacts_quarantine"), f"_source_file = '{file_name}'")

contacts = (checked.filter("reason = ''")
    .withColumn("rn", F.row_number().over(Window.partitionBy("attempt_id").orderBy(F.col("_row_id").desc())))
    .filter("rn = 1")
    .select("attempt_id", "client_code", "client_account_ref", "attempt_ts", "channel", "outcome", "agent_id")
    .withColumn("row_hash", row_hash("client_account_ref", "attempt_ts", "channel", "outcome", "agent_id"))
    .withColumn("loaded_date", F.lit(RUN_DATE).cast("date")))

m = upsert(contacts, tbl(SILVER, "contact_attempts"), ["attempt_id"], update_exclude=("loaded_date",))
log_step("06_silver_contacts", rows_in=batch.count(), quarantined=bad.count(), inserted=m["inserted"], updated=m["updated"])
