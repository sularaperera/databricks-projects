# Databricks notebook source
# MAGIC %md
# MAGIC # 02_bronze_ingest: load every source file for `run_date` as raw rows
# MAGIC **Bronze = exactly what arrived.** Every column stays a string and nothing is cleaned or dropped.
# MAGIC We only add `_source_file`, `_file_date`, `_row_id` and `_ingested_at`. If a cleaning rule turns out
# MAGIC to be wrong later, we can rebuild silver from bronze without asking the client to resend anything.
# MAGIC
# MAGIC **Safe to rerun, in two ways:**
# MAGIC 1. We store a SHA-256 checksum of each file in `debt_meta.load_log`. The same file is skipped next time,
# MAGIC    even if it was renamed or re-sent.
# MAGIC 2. If you force a reload (`force_reload = yes`), the write uses `replaceWhere _source_file = ...`,
# MAGIC    so that file's rows are replaced instead of being added a second time.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

force_reload = get_param("force_reload", "no").lower() == "yes"


def file_checksum(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def already_loaded(checksum):
    loaded = (spark.table(f"{META}.load_log")
                   .filter((F.col("checksum") == checksum) & (F.col("status") == "loaded")))
    return not loaded.isEmpty()


def write_load_log(source, file_name, checksum, rows, status):
    row = [(source, file_name, RUN_DATE, checksum, rows, status)]
    schema = "source string, file_name string, file_date string, checksum string, rows_loaded long, status string"
    (spark.createDataFrame(row, schema)
          .withColumn("loaded_at", F.current_timestamp())
          .write.format("delta").mode("append").saveAsTable(f"{META}.load_log"))

# COMMAND ----------

results = []

for source in ALL_SOURCES:
    file_name = f"{source}_{RUN_DATE}.csv"
    path = f"{LANDING}/{source}/{file_name}"

    # 1. File not there? Log it as missing (only for daily files; clients doesn't come every day)
    if not os.path.exists(path):
        if source in DAILY_SOURCES:
            write_load_log(source, file_name, None, 0, "missing")
        results.append(f"{source}: no file")
        continue

    # 2. Already loaded this exact file before? Skip it.
    checksum = file_checksum(path)
    if already_loaded(checksum) and not force_reload:
        write_load_log(source, file_name, checksum, 0, "skipped_duplicate")
        results.append(f"{source}: skipped (already loaded)")
        continue

    # 3. Load it as text, add the audit columns, and replace this file's rows in bronze
    df = (spark.read.format("csv")
               .option("header", True)
               .option("inferSchema", False)     # bronze keeps everything as text
               .load(path)
               .withColumn("_source_file", F.lit(file_name))
               .withColumn("_file_date", F.lit(RUN_DATE))
               .withColumn("_row_id", F.monotonically_increasing_id())
               .withColumn("_ingested_at", F.current_timestamp()))
    rows = df.count()
    overwrite_slice(df, f"{BRONZE}.{source}", f"_source_file = '{file_name}'", partition_col="_file_date")
    write_load_log(source, file_name, checksum, rows, "loaded")
    results.append(f"{source}: {rows} rows loaded")

log_step("02_bronze_ingest", " | ".join(results))

# COMMAND ----------

display(spark.table(f"{META}.load_log").filter(F.col("file_date") == RUN_DATE).orderBy("source"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Auto Loader alternative (for lots of files or near-real-time)
# MAGIC Auto Loader keeps its own checkpoint of the files it has processed, so it replaces the checksum table:
# MAGIC ```python
# MAGIC (spark.readStream.format("cloudFiles")
# MAGIC      .option("cloudFiles.format", "csv")
# MAGIC      .option("cloudFiles.schemaLocation", f"{LANDING}/_schemas/placements")
# MAGIC      .option("header", True)
# MAGIC      .load(f"{LANDING}/placements")
# MAGIC      .withColumn("_source_file", F.col("_metadata.file_name"))
# MAGIC      .withColumn("_ingested_at", F.current_timestamp())
# MAGIC  .writeStream
# MAGIC      .option("checkpointLocation", f"{LANDING}/_checkpoints/placements")
# MAGIC      .trigger(availableNow=True)          # process what's new, then stop: batch-style cost
# MAGIC      .toTable(f"{BRONZE}.placements"))
# MAGIC ```
# MAGIC This project uses the `run_date` version because it is easier to explain, test and backfill.
