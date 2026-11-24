# Databricks notebook source
# MAGIC %md
# MAGIC # 01_setup: create the schemas, the landing volume and the meta tables
# MAGIC Run this **once** (running it again is safe).
# MAGIC
# MAGIC * Creates `debt_bronze`, `debt_silver`, `debt_gold` and `debt_meta` in your catalog
# MAGIC * Creates the volume `debt_meta.landing` (the landing zone, like an ADLS container)
# MAGIC * Copies the sample CSVs from this project's `data/landing` folder into the volume
# MAGIC * Set `reset = yes` to drop everything and start again

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import shutil

reset = get_param("reset", "no").lower() == "yes"

if reset:
    for schema in [BRONZE, SILVER, GOLD, META]:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    print("Dropped all debt_* schemas")

for schema in [BRONZE, SILVER, GOLD, META]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

if ON_DATABRICKS:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {META}.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Meta tables
# MAGIC * `load_log`: one row per file we tried to load into bronze (file name, checksum, rows). Stops double loads.
# MAGIC * `pipeline_runs`: one row per notebook run, with counts.
# MAGIC * `dq_results`: one row per data quality check per run date.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {META}.load_log (
        source STRING,
        file_name STRING,
        file_date STRING,
        checksum STRING,
        rows_loaded BIGINT,
        status STRING,
        loaded_at TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {META}.pipeline_runs (
        run_date STRING,
        step STRING,
        status STRING,
        details STRING,
        logged_at TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {META}.dq_results (
        run_date STRING,
        check_name STRING,
        target STRING,
        status STRING,
        observed STRING,
        expected STRING,
        checked_at TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy the sample data into the landing volume
# MAGIC This works when the project is in a **Git folder** or workspace folder (the notebook can see `../data/landing`).
# MAGIC If it can't find the files, upload them yourself: **Catalog → your catalog → debt_meta → landing → Upload to this volume**,
# MAGIC keeping the sub-folders (`placements/`, `bank_payments/`, ...).

# COMMAND ----------

# Look for the sample data next to the notebooks folder (or in the SAMPLE_DATA folder when testing locally)
sample_folder = None
for folder in [os.path.join(os.getcwd(), "..", "data", "landing"), os.environ.get("SAMPLE_DATA", "")]:
    if folder and os.path.isdir(folder):
        sample_folder = os.path.abspath(folder)
        break

if sample_folder and sample_folder != os.path.abspath(LANDING):
    copied = 0
    for source in sorted(os.listdir(sample_folder)):
        os.makedirs(f"{LANDING}/{source}", exist_ok=True)
        for file_name in sorted(os.listdir(os.path.join(sample_folder, source))):
            target = f"{LANDING}/{source}/{file_name}"
            if not os.path.exists(target):
                shutil.copyfile(os.path.join(sample_folder, source, file_name), target)
                copied += 1
    print(f"Copied {copied} new file(s) from {sample_folder} to {LANDING}")
else:
    print("Sample data folder not found next to the notebooks. Upload data/landing/* to the volume manually.")

# Show what is in the landing folder now
for source in ALL_SOURCES:
    folder = f"{LANDING}/{source}"
    files = sorted(os.listdir(folder)) if os.path.isdir(folder) else []
    print(f"  {source:<17} {len(files)} file(s): {', '.join(files)}")
