# Databricks notebook source
# MAGIC %md
# MAGIC # 04_silver_placements: clean, validate, quarantine, then load accounts (SCD1) and debtors (SCD2)
# MAGIC
# MAGIC ```
# MAGIC bronze.placements (run_date) ─ clean ─ validate ─┬─ bad rows ──> silver.placements_quarantine (with reasons)
# MAGIC                                                 └─ good rows ─ dedupe ─┬─> silver.accounts  (MERGE, SCD1)
# MAGIC                                                                        └─> silver.debtors   (MERGE, SCD2)
# MAGIC ```
# MAGIC * **One debtor, many debts:** the same person can owe Kiwi Power and Harbour Telecom. A `debtor_key` built from
# MAGIC   the cleaned name + date of birth links them, and also merges duplicate records that only differ in formatting.
# MAGIC * **SCD2 on debtors:** when a phone or email changes we keep the old version (`valid_to`, `is_current = false`).
# MAGIC   That matters for disputes: "which number did we call on 14 Nov?"

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

batch = read_bronze_batch("placements")
if batch is None or batch.isEmpty():
    log_step("04_silver_placements", status="no_data")
    dbutils.notebook.exit("no placement file for this date")

file_name = batch.select("_source_file").first()[0]
run_date = F.lit(RUN_DATE).cast("date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clean

# COMMAND ----------

# Keep the original values as JSON too, so the quarantine report shows exactly what the client sent
raw_columns = [c for c in batch.columns if not c.startswith("_")]

cleaned = batch.select(
    F.upper(clean_text("client_code")).alias("client_code"),
    F.upper(clean_text("client_account_ref")).alias("client_account_ref"),
    F.initcap(clean_text("first_name")).alias("first_name"),
    F.initcap(clean_text("last_name")).alias("last_name"),
    clean_date("date_of_birth").alias("date_of_birth"),
    clean_nz_phone("phone").alias("phone_e164"),
    F.lower(clean_text("email")).alias("email"),
    clean_money("balance_owing").alias("balance_owing"),
    clean_date("debt_date").alias("debt_date"),
    F.to_json(F.struct(*raw_columns)).alias("raw_record"),
    "_source_file", "_file_date", "_row_id",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate
# MAGIC We collect **every** problem with a row, not just the first one. The client gets one complete list,
# MAGIC fixes the row once, and it loads with the next file.

# COMMAND ----------

known_clients = spark.table(f"{SILVER}.clients").select("client_code", F.lit(True).alias("is_known_client"))
cleaned = cleaned.join(known_clients, "client_code", "left")

valid_email = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# Each F.when gives the reason text when the check fails, otherwise NULL.
# concat_ws skips the NULLs, so a good row ends up with reason = ''.
checked = cleaned.withColumn("reason", F.concat_ws("; ",
    F.when(F.col("is_known_client").isNull(), "unknown client"),
    F.when(F.col("client_account_ref").isNull(), "missing account ref"),
    F.when(F.col("first_name").isNull() | F.col("last_name").isNull(), "missing name"),
    F.when(F.col("date_of_birth").isNull(), "invalid date of birth"),
    F.when(F.months_between(run_date, F.col("date_of_birth")) < 18 * 12, "debtor under 18"),
    F.when(F.col("phone_e164").isNull(), "invalid phone"),
    F.when(F.col("email").isNotNull() & ~F.col("email").rlike(valid_email), "invalid email"),
    F.when(F.col("balance_owing").isNull(), "missing balance"),
    F.when(F.col("balance_owing") <= 0, "balance not positive"),
    F.when(F.col("debt_date").isNull(), "invalid debt date"),
    F.when(F.col("debt_date") > run_date, "debt date in the future"),
)).drop("is_known_client")

bad = checked.filter(F.col("reason") != "")
good = checked.filter(F.col("reason") == "")

# Quarantine: replace this file's rows, so a rerun never adds them twice
quarantine = bad.select("client_code", "client_account_ref", "reason", "raw_record", "_source_file", "_file_date")
overwrite_slice(quarantine, f"{SILVER}.placements_quarantine", f"_source_file = '{file_name}'")
display(bad.select("client_account_ref", "reason"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Remove duplicates in the file, then MERGE accounts (SCD1)
# MAGIC Removing duplicates first matters: a Delta MERGE fails if two source rows match the same target row.

# COMMAND ----------

# If an account is in the file twice, keep the last row
last_row = Window.partitionBy("client_code", "client_account_ref").orderBy(F.col("_row_id").desc())
good = good.withColumn("rn", F.row_number().over(last_row)).filter("rn = 1").drop("rn")

# debtor_key: the same person (same name + date of birth) gets the same key in every file
good = good.withColumn("debtor_key", F.substring(
    F.sha2(F.concat_ws("|", F.lower("first_name"), F.lower("last_name"), F.col("date_of_birth").cast("string")), 256),
    1, 16))

accounts = (good
    .select("client_code", "client_account_ref", "debtor_key",
            F.col("balance_owing").alias("placed_balance"),
            F.col("debt_date").alias("placed_date"),
            "_source_file")
    .withColumn("row_hash", make_hash("debtor_key", "placed_balance", "placed_date"))
    .withColumn("first_seen_date", run_date)
    .withColumn("last_updated_date", run_date))

# Update everything except first_seen_date, and only if the row changed and isn't older than what we have
accounts_inserted, accounts_updated = run_merge(accounts, f"{SILVER}.accounts", f"""
    MERGE INTO {SILVER}.accounts t
    USING updates s
    ON t.client_code = s.client_code AND t.client_account_ref = s.client_account_ref
    WHEN MATCHED AND t.row_hash <> s.row_hash AND s.last_updated_date >= t.last_updated_date THEN UPDATE SET
        t.debtor_key        = s.debtor_key,
        t.placed_balance    = s.placed_balance,
        t.placed_date       = s.placed_date,
        t._source_file      = s._source_file,
        t.row_hash          = s.row_hash,
        t.last_updated_date = s.last_updated_date
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE debtors as SCD Type 2
# MAGIC This is the standard Delta SCD2 pattern, done in **one** MERGE so it's all or nothing:
# MAGIC * A debtor whose details **changed** goes into the source twice:
# MAGIC   * once with `merge_key = debtor_key`: it matches the current version, which we close (`is_current = false`)
# MAGIC   * once with `merge_key = NULL`: it can't match anything, so it gets INSERTED as the new current version
# MAGIC * A **new** debtor doesn't match anything, so it's inserted.
# MAGIC * An **unchanged** debtor matches, but `row_hash` is the same, so nothing happens. That's why reruns are safe.
# MAGIC * Backfill guard: when rerunning an OLD date, we never replace a version that started after that date
# MAGIC   (`valid_from <= run_date`).

# COMMAND ----------

# One row per debtor (the last one in the file)
last_row_per_debtor = Window.partitionBy("debtor_key").orderBy(F.col("_row_id").desc())
debtors = (good
    .withColumn("rn", F.row_number().over(last_row_per_debtor))
    .filter("rn = 1")
    .select("debtor_key", "first_name", "last_name", "date_of_birth", "phone_e164", "email")
    .withColumn("row_hash", make_hash("first_name", "last_name", "date_of_birth", "phone_e164", "email")))

debtors_table = f"{SILVER}.debtors"

if not spark.catalog.tableExists(debtors_table):
    # First run: every debtor gets their first version
    (debtors
        .withColumn("valid_from", run_date)
        .withColumn("valid_to", F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
        .write.format("delta").saveAsTable(debtors_table))
    versions_added, versions_closed = debtors.count(), 0
else:
    debtors.createOrReplaceTempView("todays_debtors")
    result = spark.sql(f"""
        MERGE INTO {debtors_table} t
        USING (
            -- every debtor in today's file, with their real key
            SELECT debtor_key AS merge_key, d.*
            FROM todays_debtors d

            UNION ALL

            -- the changed debtors again, with a NULL key, so they get inserted as the new version
            SELECT CAST(NULL AS STRING) AS merge_key, d.*
            FROM todays_debtors d
            JOIN {debtors_table} cur ON cur.debtor_key = d.debtor_key AND cur.is_current = true
            WHERE d.row_hash <> cur.row_hash
              AND cur.valid_from <= DATE'{RUN_DATE}'
        ) s
        ON t.debtor_key = s.merge_key AND t.is_current = true

        -- close the old version
        WHEN MATCHED AND t.row_hash <> s.row_hash AND t.valid_from <= DATE'{RUN_DATE}' THEN UPDATE SET
            t.is_current = false,
            t.valid_to   = DATE'{RUN_DATE}'

        -- add the new version (or a brand new debtor)
        WHEN NOT MATCHED THEN INSERT
            (debtor_key, first_name, last_name, date_of_birth, phone_e164, email, row_hash,
             valid_from, valid_to, is_current)
        VALUES
            (s.debtor_key, s.first_name, s.last_name, s.date_of_birth, s.phone_e164, s.email, s.row_hash,
             DATE'{RUN_DATE}', DATE'9999-12-31', true)
    """).first()
    versions_added, versions_closed = result["num_inserted_rows"], result["num_updated_rows"]

log_step("04_silver_placements",
         f"rows_in={batch.count()}, quarantined={bad.count()}, "
         f"accounts_inserted={accounts_inserted}, accounts_updated={accounts_updated}, "
         f"debtor_versions_added={versions_added}, debtor_versions_closed={versions_closed}")

# COMMAND ----------

# Debtors that have more than one version (their details changed at some point)
debtors_with_history = (spark.table(debtors_table)
    .groupBy("debtor_key").count()
    .filter("count > 1")
    .select("debtor_key"))

display(spark.table(debtors_table).join(debtors_with_history, "debtor_key").orderBy("debtor_key", "valid_from"))
