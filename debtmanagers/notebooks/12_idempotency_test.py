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
# MAGIC A table's "fingerprint" is its row count plus a hash of every row, ignoring audit timestamps.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

LAST_DAY = get_param("last_day", "2026-11-15")
AUDIT_COLS = {"_ingested_at", "_row_id"}  # _row_id is a technical id that can change on a forced reload


def fingerprint(full_name, where=None):
    df = spark.table(full_name)
    if where:
        df = df.filter(where)
    cols = sorted(c for c in df.columns if c not in AUDIT_COLS)
    row = df.select(F.xxhash64(*[F.col(c).cast("string") for c in cols]).alias("h")) \
            .agg(F.count("*").alias("n"), F.sum(F.col("h").cast("decimal(38,0)")).alias("s")).first()
    return (row.n, str(row.s))


TABLES = ([tbl(BRONZE, s["bronze"]) for s in SOURCES.values()]
          + [tbl(SILVER, t) for t in ["clients", "accounts", "debtors", "payments", "contact_attempts",
                                      "placements_quarantine", "payments_quarantine", "contacts_quarantine"]]
          + [tbl(GOLD, t) for t in ["dim_client", "dim_debtor", "dim_account", "fact_payments",
                                    "fact_contact_attempts"]])
SNAPSHOT = tbl(GOLD, "fact_account_snapshot")


def take(snapshot_where):
    fp = {t: fingerprint(t) for t in TABLES}
    fp[SNAPSHOT + f" [{snapshot_where}]"] = fingerprint(SNAPSHOT, snapshot_where)
    return fp


def run_days(start, end, force="no"):
    dbutils.notebook.run("./11_run_pipeline", 3600, {"start_date": start, "end_date": end, "catalog": CATALOG or ""})


def compare(label, before, after):
    diffs = [t for t in before if before[t] != after[t]]
    print(f"{label}: {'PASS' if not diffs else 'FAIL'}  ({len(before) - len(diffs)}/{len(before)} tables identical)")
    for t in diffs:
        print(f"   changed: {t}  {before[t]} -> {after[t]}")
    return not diffs

# COMMAND ----------

all_snapshots = "snapshot_date IS NOT NULL"
last_snapshot = f"snapshot_date = DATE'{LAST_DAY}'"

baseline = take(all_snapshots)
run_days(LAST_DAY, LAST_DAY)
ok1 = compare("Test 1, rerun last day", baseline, take(all_snapshots))

# COMMAND ----------

steps = ["02_bronze_ingest", "03_silver_clients", "04_silver_placements", "05_silver_payments", "06_silver_contacts",
         "07_gold_dimensions", "08_gold_facts", "09_gold_kpi_views", "10_data_quality"]
for step in steps:
    params = {"run_date": LAST_DAY, "catalog": CATALOG or ""}
    if step == "02_bronze_ingest":
        params["force_reload"] = "yes"
    dbutils.notebook.run(f"./{step}", 1800, params)
ok2 = compare("Test 2, force-reload last day", baseline, take(all_snapshots))

# COMMAND ----------

baseline_last = take(last_snapshot)
run_days("2026-11-13", LAST_DAY)
ok3 = compare("Test 3, replay all days", baseline_last, take(last_snapshot))

# COMMAND ----------

summary = f"rerun={'PASS' if ok1 else 'FAIL'}, force_reload={'PASS' if ok2 else 'FAIL'}, replay={'PASS' if ok3 else 'FAIL'}"
log_step("12_idempotency_test", status="success" if ok1 and ok2 and ok3 else "failed", result=summary)
assert ok1 and ok2 and ok3, summary
