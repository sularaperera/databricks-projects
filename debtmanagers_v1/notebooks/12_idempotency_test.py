# Databricks notebook source
# MAGIC %md
# MAGIC # 12_idempotency_test: prove that reruns are safe
# MAGIC Run this **after** `11_run_pipeline` has loaded 13 to 15 Nov.
# MAGIC
# MAGIC | Test | What it simulates | Expected |
# MAGIC |---|---|---|
# MAGIC | 1. Rerun the last day | The scheduler retries a job that already succeeded | every table identical |
# MAGIC | 2. Force-reload the last day | Someone bypasses the file checksum and reloads bronze | every table identical |
# MAGIC | 3. Replay all days | A backfill of the whole range | every silver and gold table identical* |
# MAGIC
# MAGIC \* Except account snapshots for **earlier** days. `silver.accounts` is SCD1 (latest balance only), so a restated
# MAGIC old snapshot uses today's balance. Making accounts SCD2 as well would fix that, and it's a good thing to mention in an interview.
# MAGIC
# MAGIC A table's "fingerprint" is its row count plus a hash of every row, ignoring the audit columns.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

LAST_DAY = get_param("last_day", "2026-11-15")

# _row_id is a technical id that can change on a forced reload, _ingested_at is a timestamp
IGNORE_COLUMNS = ["_ingested_at", "_row_id"]

TABLES = (
    [f"{BRONZE}.{source}" for source in ALL_SOURCES]
    + [f"{SILVER}.{t}" for t in ["clients", "accounts", "debtors", "payments", "contact_attempts",
                                 "placements_quarantine", "payments_quarantine", "contacts_quarantine"]]
    + [f"{GOLD}.{t}" for t in ["dim_client", "dim_debtor", "dim_account", "fact_payments", "fact_contact_attempts"]]
)
SNAPSHOT_TABLE = f"{GOLD}.fact_account_snapshot"


def fingerprint(table, where=None):
    """(row count, sum of a hash of every row). Same fingerprint = same data."""
    df = spark.table(table)
    if where:
        df = df.filter(where)
    columns = sorted(c for c in df.columns if c not in IGNORE_COLUMNS)
    row_hashes = df.select(F.xxhash64(*[F.col(c).cast("string") for c in columns]).alias("h"))
    result = row_hashes.agg(F.count("*").alias("n"), F.sum(F.col("h").cast("decimal(38,0)")).alias("s")).first()
    return (result["n"], str(result["s"]))


def fingerprint_all(snapshot_filter):
    fingerprints = {}
    for table in TABLES:
        fingerprints[table] = fingerprint(table)
    fingerprints[f"{SNAPSHOT_TABLE} [{snapshot_filter}]"] = fingerprint(SNAPSHOT_TABLE, snapshot_filter)
    return fingerprints


def compare(label, before, after):
    changed = [t for t in before if before[t] != after[t]]
    result = "PASS" if not changed else "FAIL"
    print(f"{label}: {result}  ({len(before) - len(changed)}/{len(before)} tables identical)")
    for t in changed:
        print(f"   changed: {t}  {before[t]} -> {after[t]}")
    return not changed


def run_pipeline(start_date, end_date):
    dbutils.notebook.run("./11_run_pipeline", 3600, {"start_date": start_date, "end_date": end_date, "catalog": CATALOG})

# COMMAND ----------

ALL_SNAPSHOTS = "snapshot_date IS NOT NULL"
LAST_SNAPSHOT = f"snapshot_date = DATE'{LAST_DAY}'"

# Test 1: run the last day again
baseline = fingerprint_all(ALL_SNAPSHOTS)
run_pipeline(LAST_DAY, LAST_DAY)
test1_ok = compare("Test 1, rerun last day", baseline, fingerprint_all(ALL_SNAPSHOTS))

# COMMAND ----------

# Test 2: run the last day again, but force bronze to reload the files
steps = ["02_bronze_ingest", "03_silver_clients", "04_silver_placements", "05_silver_payments", "06_silver_contacts",
         "07_gold_dimensions", "08_gold_facts", "09_gold_kpi_views", "10_data_quality"]

for step in steps:
    params = {"run_date": LAST_DAY, "catalog": CATALOG}
    if step == "02_bronze_ingest":
        params["force_reload"] = "yes"
    dbutils.notebook.run(f"./{step}", 1800, params)

test2_ok = compare("Test 2, force-reload last day", baseline, fingerprint_all(ALL_SNAPSHOTS))

# COMMAND ----------

# Test 3: replay every day from the start
baseline_last = fingerprint_all(LAST_SNAPSHOT)
run_pipeline("2026-11-13", LAST_DAY)
test3_ok = compare("Test 3, replay all days", baseline_last, fingerprint_all(LAST_SNAPSHOT))

# COMMAND ----------

summary = (f"rerun={'PASS' if test1_ok else 'FAIL'}, "
           f"force_reload={'PASS' if test2_ok else 'FAIL'}, "
           f"replay={'PASS' if test3_ok else 'FAIL'}")

all_ok = test1_ok and test2_ok and test3_ok
log_step("12_idempotency_test", f"result={summary}", status="success" if all_ok else "failed")
assert all_ok, summary
