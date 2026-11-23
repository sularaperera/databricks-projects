# Databricks notebook source
# MAGIC %md
# MAGIC # 01_setup: create schemas, the landing volume and metadata tables
# MAGIC Run this **once** (it's safe to run again).
# MAGIC
# MAGIC * Creates `debt_bronze`, `debt_silver`, `debt_gold`, `debt_meta` in your catalog
# MAGIC * Creates the volume `debt_meta.landing` (the landing zone, like an ADLS container)
# MAGIC * Copies the sample CSVs from this project's `data/landing` folder into the volume
# MAGIC * Set `reset = yes` to drop everything and start again

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import shutil

RESET = get_param("reset", "no").lower() == "yes"

if RESET:
    for s in (BRONZE, SILVER, GOLD, META):
        spark.sql(f"DROP SCHEMA IF EXISTS {schema_name(s)} CASCADE")
    print("Dropped all debt_* schemas")

for s in (BRONZE, SILVER, GOLD, META):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name(s)}")

if IS_DATABRICKS:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{META}.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata tables
# MAGIC * `load_log`: one row per file ingested into bronze (file name, checksum, rows). Stops double loads.
# MAGIC * `pipeline_runs`: one row per notebook run, with counts.
# MAGIC * `dq_results`: one row per data quality check per run date.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(META, 'load_log')} (
    source STRING, file_name STRING, file_date STRING, checksum STRING,
    rows_loaded BIGINT, status STRING, loaded_at TIMESTAMP) USING DELTA""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(META, 'pipeline_runs')} (
    run_date STRING, step STRING, status STRING, details STRING, logged_at TIMESTAMP) USING DELTA""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(META, 'dq_results')} (
    run_date STRING, check_name STRING, target STRING, status STRING,
    observed STRING, expected STRING, checked_at TIMESTAMP) USING DELTA""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy the sample data into the landing volume
# MAGIC Works when the project is in a **Git folder** or workspace folder (the notebook can see `../data/landing`).
# MAGIC If it can't find the files, upload them yourself: **Catalog → your catalog → debt_meta → landing → Upload to this volume**, keeping the sub-folders (`placements/`, `bank_payments/`, ...).

# COMMAND ----------

def find_sample_data():
    candidates = [os.path.join(os.getcwd(), "..", "data", "landing"), os.environ.get("SAMPLE_DATA", "")]
    return next((os.path.abspath(p) for p in candidates if p and os.path.isdir(p)), None)


sample = find_sample_data()
if sample and os.path.abspath(sample) != os.path.abspath(LANDING):
    copied = 0
    for source in sorted(os.listdir(sample)):
        os.makedirs(f"{LANDING}/{source}", exist_ok=True)
        for f in sorted(os.listdir(os.path.join(sample, source))):
            target = f"{LANDING}/{source}/{f}"
            if not os.path.exists(target):
                shutil.copyfile(os.path.join(sample, source, f), target)
                copied += 1
    print(f"Copied {copied} new file(s) from {sample} to {LANDING}")
else:
    print("Sample data folder not found next to the notebooks. Upload data/landing/* to the volume manually.")

for source in SOURCES:
    folder = f"{LANDING}/{source}"
    files = sorted(os.listdir(folder)) if os.path.isdir(folder) else []
    print(f"  {source:<17} {len(files)} file(s): {', '.join(files)}")
