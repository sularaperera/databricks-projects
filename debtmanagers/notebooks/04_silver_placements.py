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
# MAGIC   cleaned name + date of birth links them, and also merges duplicate records that only differ in formatting.
# MAGIC * **SCD2 on debtors:** when a phone or email changes we keep the old version (`valid_to`, `is_current = false`).
# MAGIC   That matters for disputes: "which number did we call on 14 Nov?"

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

bronze_t = tbl(BRONZE, "placements")
batch = spark.table(bronze_t).filter(F.col("_file_date") == RUN_DATE) if table_exists(bronze_t) else None
if batch is None or batch.limit(1).count() == 0:
    log_step("04_silver_placements", status="no_data")
    dbutils.notebook.exit("no placement file for this date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clean

# COMMAND ----------

run_date = F.lit(RUN_DATE).cast("date")

cleaned = batch.select(
    clean_text("client_code", "upper").alias("client_code"),
    clean_text("client_account_ref", "upper").alias("client_account_ref"),
    clean_text("first_name").alias("first_name"),
    clean_text("last_name").alias("last_name"),
    clean_date("date_of_birth").alias("date_of_birth"),
    clean_nz_phone("phone").alias("phone_e164"),
    clean_text("email", "lower").alias("email"),
    clean_money("balance_owing").alias("balance_owing"),
    clean_date("debt_date").alias("debt_date"),
    # keep the raw values next to the clean ones, for the quarantine report
    F.to_json(F.struct(*[c for c in batch.columns if not c.startswith("_")])).alias("raw_record"),
    "_source_file", "_file_date", "_row_id",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate: collect every reason, don't stop at the first
# MAGIC The client gets one complete list, fixes the row once, and it loads on the next file.

# COMMAND ----------

known_clients = spark.table(tbl(SILVER, "clients")).select("client_code", F.lit(True).alias("_known"))
checked = (cleaned.join(known_clients, "client_code", "left")
    .withColumn("reason", reasons(
        (F.col("_known").isNull(), "unknown client"),
        (F.col("client_account_ref").isNull(), "missing account ref"),
        (F.col("first_name").isNull() | F.col("last_name").isNull(), "missing name"),
        (F.col("date_of_birth").isNull(), "invalid date of birth"),
        (F.months_between(run_date, F.col("date_of_birth")) < 18 * 12, "debtor under 18"),
        (F.col("phone_e164").isNull(), "invalid phone"),
        (F.col("email").isNotNull() & ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), "invalid email"),
        (F.col("balance_owing").isNull(), "missing balance"),
        (F.col("balance_owing") <= 0, "balance not positive"),
        (F.col("debt_date").isNull(), "invalid debt date"),
        (F.col("debt_date") > run_date, "debt date in the future"),
    ))
    .drop("_known"))

bad = checked.filter(F.col("reason") != "")
good = checked.filter(F.col("reason") == "")

# Quarantine: rewrite this file's slice so a rerun never duplicates it
file_name = batch.select("_source_file").first()[0]
replace_slice(bad.select("client_code", "client_account_ref", "reason", "raw_record", "_source_file", "_file_date"),
              tbl(SILVER, "placements_quarantine"), f"_source_file = '{file_name}'")
show(bad.select("client_account_ref", "reason"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dedupe inside the file, then MERGE accounts (SCD1)
# MAGIC Deduping first matters: a Delta MERGE fails if two source rows match the same target row.

# COMMAND ----------

latest = Window.partitionBy("client_code", "client_account_ref").orderBy(F.col("_row_id").desc())
good = (good.withColumn("rn", F.row_number().over(latest)).filter("rn = 1").drop("rn")
            .withColumn("debtor_key", F.substring(F.sha2(F.concat_ws("|", F.lower("first_name"), F.lower("last_name"),
                                                                     F.col("date_of_birth").cast("string")), 256), 1, 16)))

accounts = (good.select("client_code", "client_account_ref", "debtor_key", "balance_owing", "debt_date", "_source_file")
    .withColumnRenamed("balance_owing", "placed_balance")
    .withColumnRenamed("debt_date", "placed_date")
    .withColumn("row_hash", row_hash("debtor_key", "placed_balance", "placed_date"))
    .withColumn("first_seen_date", run_date)
    .withColumn("last_updated_date", run_date))

acc_metrics = upsert(accounts, tbl(SILVER, "accounts"), ["client_code", "client_account_ref"],
                     update_exclude=("first_seen_date",), only_if_newer="last_updated_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE debtors as SCD Type 2
# MAGIC The standard Delta pattern in one MERGE:
# MAGIC * Rows whose attributes **changed** go in twice: once with a NULL merge key (forces an INSERT of the new version)
# MAGIC   and once with the real key (matches the old current row so it can be closed).
# MAGIC * Unchanged rows match, but the `row_hash <>` condition is false, so nothing happens. That's why reruns are safe.

# COMMAND ----------

latest_debtor = Window.partitionBy("debtor_key").orderBy(F.col("_row_id").desc())
debtors = (good.withColumn("rn", F.row_number().over(latest_debtor)).filter("rn = 1")
    .select("debtor_key", "first_name", "last_name", "date_of_birth", "phone_e164", "email")
    .withColumn("row_hash", row_hash("first_name", "last_name", "date_of_birth", "phone_e164", "email")))

debtors_t = tbl(SILVER, "debtors")
if not table_exists(debtors_t):
    (debtors.withColumn("valid_from", run_date)
            .withColumn("valid_to", F.lit("9999-12-31").cast("date"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta").saveAsTable(debtors_t))
    deb_metrics = {"new_versions": debtors.count(), "closed": 0}
else:
    current = (spark.table(debtors_t).filter("is_current")
                    .select("debtor_key", F.col("row_hash").alias("cur_hash"), F.col("valid_from").alias("cur_from")))
    # Backfill guard: a rerun of an OLDER date must not replace a newer version
    changed = (debtors.join(current, "debtor_key")
                      .filter((F.col("row_hash") != F.col("cur_hash")) & (F.col("cur_from") <= run_date))
                      .drop("cur_hash", "cur_from"))
    staged = (changed.withColumn("merge_key", F.lit(None).cast("string"))
              .unionByName(debtors.withColumn("merge_key", F.col("debtor_key"))))
    (DeltaTable.forName(spark, debtors_t).alias("t")
        .merge(staged.alias("s"), "t.debtor_key = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(condition=f"t.row_hash <> s.row_hash AND t.valid_from <= DATE'{RUN_DATE}'",
                           set={"is_current": "false", "valid_to": f"DATE'{RUN_DATE}'"})
        .whenNotMatchedInsert(values={
            "debtor_key": "s.debtor_key", "first_name": "s.first_name", "last_name": "s.last_name",
            "date_of_birth": "s.date_of_birth", "phone_e164": "s.phone_e164", "email": "s.email",
            "row_hash": "s.row_hash", "valid_from": f"DATE'{RUN_DATE}'", "valid_to": "DATE'9999-12-31'",
            "is_current": "true"})
        .execute())
    m = last_merge_metrics(debtors_t)
    deb_metrics = {"new_versions": m.get("numTargetRowsInserted", 0), "closed": m.get("numTargetRowsUpdated", 0)}

log_step("04_silver_placements", rows_in=batch.count(), quarantined=bad.count(),
         accounts_inserted=acc_metrics["inserted"], accounts_updated=acc_metrics["updated"],
         debtor_versions_added=deb_metrics["new_versions"], debtor_versions_closed=deb_metrics["closed"])

# COMMAND ----------

show(spark.table(debtors_t).filter(F.col("debtor_key").isin(
    [r[0] for r in spark.table(debtors_t).groupBy("debtor_key").count().filter("count > 1").collect()]))
    .orderBy("debtor_key", "valid_from"))
