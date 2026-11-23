# Databricks notebook source
# MAGIC %md
# MAGIC # 02_bronze_ingest: land every source file for `run_date` as raw rows
# MAGIC **Bronze = exactly what arrived.** Every column stays a string, nothing is cleaned or dropped, and we add
# MAGIC `_source_file`, `_file_date`, `_row_id` and `_ingested_at`. If a cleaning rule is wrong later, silver can be
# MAGIC rebuilt from bronze without asking the client to resend anything.
# MAGIC
# MAGIC **Rerun safety, two layers:**
# MAGIC 1. A SHA-256 checksum of the file is stored in `debt_meta.load_log`. The same file is skipped next time,
# MAGIC    even if it's renamed or re-sent.
# MAGIC 2. If you force a reload (`force_reload = yes`), the write uses `replaceWhere _source_file = ...`,
# MAGIC    so that file's rows are swapped out rather than added a second time.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

FORCE = get_param("force_reload", "no").lower() == "yes"


def file_checksum(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def already_loaded(checksum):
    return (spark.table(tbl(META, "load_log"))
                 .filter((F.col("checksum") == checksum) & (F.col("status") == "loaded"))
                 .limit(1).count() > 0)


def write_load_log(source, file_name, checksum, rows, status):
    (spark.createDataFrame([(source, file_name, RUN_DATE, checksum, rows, status)],
                           "source string, file_name string, file_date string, checksum string, rows_loaded long, status string")
          .withColumn("loaded_at", F.current_timestamp())
          .write.format("delta").mode("append").saveAsTable(tbl(META, "load_log")))


def ingest(source, cfg):
    path = landing_file(source)
    file_name = os.path.basename(path)
    if not os.path.exists(path):
        if cfg["frequency"] == "daily":
            write_load_log(source, file_name, None, 0, "missing")
        return f"{source}: no file"

    checksum = file_checksum(path)
    if already_loaded(checksum) and not FORCE:
        write_load_log(source, file_name, checksum, 0, "skipped_duplicate")
        return f"{source}: skipped (already loaded)"

    raw = (spark.read.format("csv")
                .option("header", True)
                .option("inferSchema", False)       # bronze keeps everything as text
                .load(path)
                .withColumn("_source_file", F.lit(file_name))
                .withColumn("_file_date", F.lit(RUN_DATE))
                .withColumn("_row_id", F.monotonically_increasing_id())
                .withColumn("_ingested_at", F.current_timestamp()))
    rows = raw.count()
    replace_slice(raw, tbl(BRONZE, cfg["bronze"]), f"_source_file = '{file_name}'", partition_by="_file_date")
    write_load_log(source, file_name, checksum, rows, "loaded")
    return f"{source}: {rows} rows loaded"


results = [ingest(s, cfg) for s, cfg in SOURCES.items()]
log_step("02_bronze_ingest", details=" | ".join(results))

# COMMAND ----------

show(spark.table(tbl(META, "load_log")).filter(F.col("file_date") == RUN_DATE).orderBy("source"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Auto Loader alternative (what you'd use for many files or near-real-time)
# MAGIC Auto Loader keeps its own checkpoint of processed files, so it replaces the checksum table:
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
# MAGIC      .toTable(tbl(BRONZE, "placements")))
# MAGIC ```
# MAGIC This project uses the explicit `run_date` version because it is easier to explain, test and backfill.
