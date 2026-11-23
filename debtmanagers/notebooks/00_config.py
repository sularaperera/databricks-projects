# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config: shared settings and helpers
# MAGIC Every other notebook starts with `%run ./00_config`, so names, paths and helpers live in **one place**.
# MAGIC
# MAGIC | Setting | Default | Meaning |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `workspace` | Unity Catalog catalog. `workspace` exists in Databricks Free Edition and most new workspaces. |
# MAGIC | `run_date` | `2026-11-13` | The **business date** being processed. Never "today": the same date always gives the same result, so reruns and backfills are safe. |
# MAGIC
# MAGIC Schemas are prefixed with `debt_` so they don't clash with other projects in the same catalog.

# COMMAND ----------

import datetime as dt
import hashlib
import os
import re

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_param(name, default):
    """Read a notebook widget / job parameter, creating it with a default if needed."""
    try:
        dbutils.widgets.text(name, default)
        value = dbutils.widgets.get(name).strip()
        return value or default
    except Exception:
        return default


CATALOG = get_param("catalog", "workspace") if IS_DATABRICKS else None
RUN_DATE = get_param("run_date", "2026-11-13")
if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", RUN_DATE):
    raise ValueError(f"run_date must be YYYY-MM-DD, got {RUN_DATE!r}")

BRONZE, SILVER, GOLD, META = "debt_bronze", "debt_silver", "debt_gold", "debt_meta"


def tbl(schema, name):
    """Full table name: catalog.schema.table on Databricks, schema.table when testing locally."""
    return f"{CATALOG}.{schema}.{name}" if CATALOG else f"{schema}.{name}"


def schema_name(schema):
    return f"{CATALOG}.{schema}" if CATALOG else schema


# Landing zone: a Unity Catalog volume on Databricks (ADLS Gen2 behind the scenes), a local folder in tests.
LANDING = f"/Volumes/{CATALOG}/{META}/landing" if IS_DATABRICKS else os.environ.get("LOCAL_LANDING", "/tmp/landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata: one entry per source
# MAGIC This is the "metadata-driven" part. Adding a new feed means adding an entry here, not writing a new notebook.

# COMMAND ----------

SOURCES = {
    #  source name          bronze table           frequency    business key in silver
    "clients":          {"bronze": "clients",          "frequency": "reference", "key": ["client_code"]},
    "placements":       {"bronze": "placements",       "frequency": "daily",     "key": ["client_code", "client_account_ref"]},
    "bank_payments":    {"bronze": "bank_payments",    "frequency": "daily",     "key": ["bank_txn_id"]},
    "bank_control":     {"bronze": "bank_control",     "frequency": "daily",     "key": ["file_name"]},
    "contact_attempts": {"bronze": "contact_attempts", "frequency": "daily",     "key": ["attempt_id"]},
}


def landing_file(source, run_date=None):
    """Files follow <landing>/<source>/<source>_<YYYY-MM-DD>.csv"""
    return f"{LANDING}/{source}/{source}_{run_date or RUN_DATE}.csv"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning helpers (Spark column expressions, so they run in parallel on the cluster)
# MAGIC `try_to_timestamp` / `try_cast` return NULL instead of failing the job. Databricks runs in ANSI mode, where plain `to_date` or `cast` would throw on bad input.

# COMMAND ----------

DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "d MMM yyyy", "yyyyMMdd"]  # NZ: 02/11/2026 = 2 Nov


def clean_date(col_name):
    return F.coalesce(*[F.to_date(F.expr(f"try_to_timestamp(trim(`{col_name}`), '{fmt}')")) for fmt in DATE_FORMATS])


def clean_nz_phone(col_name):
    """'021 555 0101', '+64 21 555 0101', '(09) 555 0103', '64-22-555-0105' -> +64215550101. Invalid -> NULL."""
    digits = F.regexp_replace(F.col(col_name), r"\D", "")
    national = F.when(digits.startswith("64"), F.substring(digits, 3, 20)) \
                .when(digits.startswith("0"), F.substring(digits, 2, 20))
    return F.when(F.length(national).between(8, 10), F.concat(F.lit("+64"), national))
# Here you can use google's libphonenumber for more robust validation, but it's not available in Spark SQL.
# so to use it, you would need to write a UDF, which is not as efficient as using Spark SQL functions.
# UDFs can be slower because they require data to be moved between the JVM and Python, and they don't benefit from Spark's Catalyst optimizer.


def clean_money(col_name):
    return F.expr(f"try_cast(regexp_replace(`{col_name}`, '[$,\\\\s]', '') AS DECIMAL(12,2))")


def clean_text(col_name, case="title"):
    c = F.regexp_replace(F.trim(F.col(col_name)), r"\s+", " ")
    c = F.when(c == "", None).otherwise(c)
    return {"title": F.initcap(c), "upper": F.upper(c), "lower": F.lower(c)}[case]


def row_hash(*cols):
    """Fingerprint of the business columns. If the hash is unchanged, the row is unchanged."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def reasons(*checks):
    """checks = (condition, 'reason') pairs. Returns '' for a good row, else 'reason 1; reason 2'."""
    return F.concat_ws("; ", *[F.when(cond, F.lit(text)) for cond, text in checks])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write helpers (all idempotent)

# COMMAND ----------

def table_exists(full_name):
    return spark.catalog.tableExists(full_name)


def replace_slice(df: DataFrame, full_name, predicate, partition_by=None):
    """Delete-and-rewrite only the slice that matches `predicate` (e.g. one file or one date).
    Running it twice gives the same table. On Delta this is one atomic commit."""
    if not table_exists(full_name):
        writer = df.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.saveAsTable(full_name)
        return
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", predicate)
       .option("mergeSchema", "true")
       .saveAsTable(full_name))


def upsert(df: DataFrame, full_name, keys, hash_col="row_hash", update_exclude=(), only_if_newer=None):
    """SCD Type 1 MERGE: insert new keys, update a key only when its hash changed, otherwise do nothing.
    only_if_newer: a date column. A backfill of an OLD date must never overwrite newer data, so the update
    only happens when the incoming row is at least as new as the stored one."""
    if not table_exists(full_name):
        df.write.format("delta").saveAsTable(full_name)
        return {"inserted": df.count(), "updated": 0}
    cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    update_cols = {c: f"s.`{c}`" for c in df.columns if c not in update_exclude}
    (DeltaTable.forName(spark, full_name).alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdate(condition=f"t.`{hash_col}` <> s.`{hash_col}`"
                           + (f" AND s.`{only_if_newer}` >= t.`{only_if_newer}`" if only_if_newer else ""),
                           set=update_cols)
        .whenNotMatchedInsertAll()
        .execute())
    m = last_merge_metrics(full_name)
    return {"inserted": m.get("numTargetRowsInserted", 0), "updated": m.get("numTargetRowsUpdated", 0)}


def last_merge_metrics(full_name):
    row = DeltaTable.forName(spark, full_name).history(1).select("operationMetrics").collect()[0][0] or {}
    return {k: int(v) for k, v in row.items() if v is not None and str(v).isdigit()}


def log_step(step, status="success", **metrics):
    """One row per notebook run in debt_meta.pipeline_runs: the first place to look when something breaks."""
    details = ", ".join(f"{k}={v}" for k, v in metrics.items())
    (spark.createDataFrame([(RUN_DATE, step, status, details)],
                           "run_date string, step string, status string, details string")
          .withColumn("logged_at", F.current_timestamp())
          .write.format("delta").mode("append").saveAsTable(tbl(META, "pipeline_runs")))
    print(f"[{RUN_DATE}] {step}: {status} {details}")


def show(df, n=20):
    """display() on Databricks, a text table elsewhere."""
    try:
        display(df.limit(n))
    except NameError:
        df.show(n, truncate=False)


print(f"Config loaded | catalog={CATALOG} | run_date={RUN_DATE} | landing={LANDING}")
