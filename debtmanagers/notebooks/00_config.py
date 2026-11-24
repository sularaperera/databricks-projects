# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config: shared settings and helper functions
# MAGIC Every other notebook runs this first with `%run ./00_config`.
# MAGIC
# MAGIC Parameters (widgets / job parameters):
# MAGIC * `catalog`: the Unity Catalog catalog to use. Default `workspace` (exists in Databricks Free Edition).
# MAGIC * `run_date`: the **business date** we are loading, as `YYYY-MM-DD`. We never use "today",
# MAGIC   so running the same date again always gives the same result.

# COMMAND ----------

import datetime as dt
import hashlib
import os
import re

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

def get_param(name, default):
    """Read a widget / job parameter. The widget is created with the default value the first time."""
    dbutils.widgets.text(name, default)
    value = dbutils.widgets.get(name).strip()
    if value == "":
        return default
    return value


CATALOG = get_param("catalog", "workspace")
RUN_DATE = get_param("run_date", "2026-11-13")

if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", RUN_DATE):
    raise ValueError(f"run_date must be YYYY-MM-DD, got {RUN_DATE}")

# Schemas. They start with debt_ so they don't clash with other projects in the same catalog.
BRONZE = f"{CATALOG}.debt_bronze"
SILVER = f"{CATALOG}.debt_silver"
GOLD = f"{CATALOG}.debt_gold"
META = f"{CATALOG}.debt_meta"

# Landing folder: a Unity Catalog volume on Databricks, a normal folder when testing on a laptop.
ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
if ON_DATABRICKS:
    LANDING = f"/Volumes/{CATALOG}/debt_meta/landing"
else:
    LANDING = os.environ.get("LOCAL_LANDING", "/tmp/landing")

# Source files. Each file lands as <LANDING>/<source>/<source>_<YYYY-MM-DD>.csv
# and is loaded into a bronze table with the same name as the source.
DAILY_SOURCES = ["placements", "bank_payments", "bank_control", "contact_attempts"]
ALL_SOURCES = ["clients"] + DAILY_SOURCES   # clients is reference data, it doesn't arrive every day

print(f"Config loaded | catalog={CATALOG} | run_date={RUN_DATE} | landing={LANDING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning functions
# MAGIC They return Spark column expressions, so the work runs on the cluster.
# MAGIC `try_to_timestamp` and `try_cast` return NULL for bad values instead of failing the job
# MAGIC (Databricks runs in ANSI mode, where a plain `cast` throws an error on bad input).

# COMMAND ----------

def clean_text(col_name):
    """Trim, replace repeated spaces with one space, and turn empty strings into NULL."""
    c = F.regexp_replace(F.trim(F.col(col_name)), r"\s+", " ")
    return F.when(c != "", c)


def clean_date(col_name):
    """The files use several date formats. Try each one and keep the first that works.
    NZ format: 02/11/2026 means 2 November."""
    return F.expr(f"""
        coalesce(
            to_date(try_to_timestamp(trim({col_name}), 'yyyy-MM-dd')),
            to_date(try_to_timestamp(trim({col_name}), 'dd/MM/yyyy')),
            to_date(try_to_timestamp(trim({col_name}), 'd MMM yyyy')),
            to_date(try_to_timestamp(trim({col_name}), 'yyyyMMdd'))
        )""")


def clean_money(col_name):
    """'$1,234.50' -> 1234.50. Bad values become NULL."""
    return F.expr(rf"try_cast(regexp_replace({col_name}, '[$,\\s]', '') AS DECIMAL(12,2))")


def clean_nz_phone(col_name):
    """'021 555 0101', '+64 21 555 0101', '(09) 555 0103', '64-22-555-0105' -> '+64215550101'.
    Anything that isn't a valid NZ number becomes NULL."""
    digits = F.regexp_replace(F.col(col_name), r"\D", "")    # keep only the digits
    without_prefix = (F.when(digits.startswith("64"), F.substring(digits, 3, 20))
                       .when(digits.startswith("0"), F.substring(digits, 2, 20)))
    return F.when(F.length(without_prefix).between(8, 10), F.concat(F.lit("+64"), without_prefix))


def make_hash(*cols):
    """SHA-256 of the given columns. If the hash didn't change, the row didn't change."""
    values = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]
    return F.sha2(F.concat_ws("||", *values), 256)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and write functions
# MAGIC All writes are safe to rerun: running the same date twice gives the same tables.

# COMMAND ----------

def read_bronze_batch(source):
    """The bronze rows loaded for RUN_DATE, or None if the bronze table doesn't exist yet."""
    table = f"{BRONZE}.{source}"
    if not spark.catalog.tableExists(table):
        return None
    return spark.table(table).filter(F.col("_file_date") == RUN_DATE)


def overwrite_slice(df, table, condition, partition_col=None):
    """Replace only the rows that match `condition` (for example one file or one date).
    The first run creates the table. After that, Delta's replaceWhere swaps the slice in one commit,
    so a rerun replaces the rows instead of adding them again."""
    if not spark.catalog.tableExists(table):
        writer = df.write.format("delta")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.saveAsTable(table)
    else:
        (df.write.format("delta")
           .mode("overwrite")
           .option("replaceWhere", condition)
           .option("mergeSchema", "true")
           .saveAsTable(table))


def run_merge(df, table, merge_sql):
    """First run: create the table from df. After that: run merge_sql, which reads from a view called `updates`.
    Returns (rows_inserted, rows_updated)."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return df.count(), 0
    df.createOrReplaceTempView("updates")
    result = spark.sql(merge_sql).first()      # Delta's MERGE returns the row counts
    return result["num_inserted_rows"], result["num_updated_rows"]


def log_step(step, details="", status="success"):
    """Add one row to debt_meta.pipeline_runs. It's the first place to look when something breaks."""
    row = [(RUN_DATE, step, status, details)]
    (spark.createDataFrame(row, "run_date string, step string, status string, details string")
          .withColumn("logged_at", F.current_timestamp())
          .write.format("delta").mode("append").saveAsTable(f"{META}.pipeline_runs"))
    print(f"[{RUN_DATE}] {step}: {status} {details}")
