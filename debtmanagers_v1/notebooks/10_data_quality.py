# Databricks notebook source
# MAGIC %md
# MAGIC # 10_data_quality: find out it broke before the business tells you
# MAGIC Results go to `debt_meta.dq_results` (one row per check per run date). Any **FAIL** makes this notebook fail,
# MAGIC so the job fails and Databricks sends the failure email or Teams alert.
# MAGIC
# MAGIC | Check | FAIL when | WARN when |
# MAGIC |---|---|---|
# MAGIC | Freshness | a daily file didn't arrive | |
# MAGIC | Bank reconciliation | our total ≠ the bank's control total (to the cent) | |
# MAGIC | Quarantine rate | > 50% of a file rejected | > 10% rejected |
# MAGIC | Unique keys | duplicate keys in silver or gold | |
# MAGIC | Orphans | a fact row with no account or debtor | |
# MAGIC | Unmatched money | | payments stuck in quarantine |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

results = []


def check(name, target, failed, observed, expected, warning=False):
    if failed:
        status = "FAIL"
    elif warning:
        status = "WARN"
    else:
        status = "PASS"
    results.append((RUN_DATE, name, target, status, str(observed), str(expected)))

# COMMAND ----------

# 1. Freshness: did every daily file arrive?
load_log = spark.table(f"{META}.load_log").filter(F.col("file_date") == RUN_DATE)

for source in DAILY_SOURCES:
    arrived = not (load_log
        .filter((F.col("source") == source) & F.col("status").isin("loaded", "skipped_duplicate"))
        .isEmpty())
    check("freshness", source, not arrived, "arrived" if arrived else "missing", "file for run_date")

# COMMAND ----------

# 2. Bank reconciliation: the unique transactions in the bank file must match the bank's own control file
payments_raw = read_bronze_batch("bank_payments")
control_raw = read_bronze_batch("bank_control")

if payments_raw is not None and control_raw is not None and not control_raw.isEmpty():
    ours = (payments_raw
        .dropDuplicates(["bank_txn_id"])
        .select(clean_money("amount").alias("amount"))
        .agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
        .first())
    bank = control_raw.select(clean_money("control_total").alias("total"),
                              F.col("txn_count").cast("int").alias("n")).first()

    check("bank_reconciliation_total", "bank_payments", ours["total"] != bank["total"], ours["total"], bank["total"])
    check("bank_reconciliation_count", "bank_payments", ours["n"] != bank["n"], ours["n"], bank["n"])

# COMMAND ----------

# 3. Quarantine rate: what % of today's file was rejected?
quarantine_tables = {
    "placements": "placements_quarantine",
    "bank_payments": "payments_quarantine",
    "contact_attempts": "contacts_quarantine",
}

for source, quarantine_table in quarantine_tables.items():
    bronze_rows = read_bronze_batch(source)
    if bronze_rows is None or not spark.catalog.tableExists(f"{SILVER}.{quarantine_table}"):
        continue
    rows_in = bronze_rows.count()
    if rows_in == 0:
        continue
    rows_bad = spark.table(f"{SILVER}.{quarantine_table}").filter(F.col("_file_date") == RUN_DATE).count()
    rate = round(100 * rows_bad / rows_in, 1)
    check("quarantine_rate_pct", source, rate > 50, rate, "<= 10", warning=rate > 10)

# COMMAND ----------

# 4. Unique keys: no duplicates in the main tables
unique_keys = [
    (f"{SILVER}.accounts", ["client_code", "client_account_ref"]),
    (f"{SILVER}.payments", ["bank_txn_id"]),
    (f"{SILVER}.contact_attempts", ["attempt_id"]),
    (f"{GOLD}.fact_payments", ["bank_txn_id"]),
    (f"{GOLD}.fact_account_snapshot", ["snapshot_date", "account_sk"]),
]

for table, keys in unique_keys:
    duplicates = spark.table(table).groupBy(*keys).count().filter("count > 1").count()
    table_short_name = table.split(".")[-1]
    check("unique_key", table_short_name, duplicates > 0, duplicates, 0)

# Each debtor must have exactly one current version
current_duplicates = (spark.table(f"{SILVER}.debtors")
    .filter("is_current")
    .groupBy("debtor_key").count()
    .filter("count > 1")
    .count())
check("one_current_version", "debtors", current_duplicates > 0, current_duplicates, 0)

# COMMAND ----------

# 5. Orphans: every payment fact must point at an account and a debtor
orphans = spark.table(f"{GOLD}.fact_payments").filter("account_sk IS NULL OR debtor_sk IS NULL").count()
check("orphan_facts", "fact_payments", orphans > 0, orphans, 0)

# 6. Money we couldn't match to an account and that still isn't resolved
if spark.catalog.tableExists(f"{SILVER}.payments_quarantine"):
    stuck = (spark.table(f"{SILVER}.payments_quarantine")
        .join(spark.table(f"{SILVER}.payments"), "bank_txn_id", "left_anti")
        .agg(F.count("*").alias("n"), F.sum("amount").alias("amount"))
        .first())
    stuck_amount = stuck["amount"] or 0
    check("unmatched_payments", "payments_quarantine", False, f"{stuck['n']} txn, ${stuck_amount}", "0",
          warning=stuck["n"] > 0)

# COMMAND ----------

# Save the results for this date (replacing any earlier run of the same date)
dq = (spark.createDataFrame(results, "run_date string, check_name string, target string, status string, observed string, expected string")
           .withColumn("checked_at", F.current_timestamp()))
overwrite_slice(dq, f"{META}.dq_results", f"run_date = '{RUN_DATE}'")
display(dq.drop("checked_at").orderBy(F.desc("status"), "check_name"))

failures = [r for r in results if r[3] == "FAIL"]
warnings = [r for r in results if r[3] == "WARN"]
passed = len(results) - len(failures) - len(warnings)

log_step("10_data_quality",
         f"passed={passed}, warnings={len(warnings)}, failures={len(failures)}",
         status="failed" if failures else "success")

if failures:
    failed_checks = [(r[1], r[2]) for r in failures]
    raise Exception(f"{len(failures)} data quality check(s) failed for {RUN_DATE}: {failed_checks}")
