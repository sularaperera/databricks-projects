# Databricks notebook source
# MAGIC %md
# MAGIC # 10_data_quality: know it broke before the business tells you
# MAGIC Results go to `debt_meta.dq_results` (one row per check per run date). Any **FAIL** fails this task,
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


def check(name, target, fail_if, observed, expected, warn_if=False):
    status = "FAIL" if fail_if else ("WARN" if warn_if else "PASS")
    results.append((RUN_DATE, name, target, status, str(observed), str(expected)))


def bronze_for_today(source):
    t = tbl(BRONZE, SOURCES[source]["bronze"])
    return spark.table(t).filter(F.col("_file_date") == RUN_DATE) if table_exists(t) else None

# COMMAND ----------

# 1. Freshness: every daily file arrived
log = spark.table(tbl(META, "load_log")).filter(F.col("file_date") == RUN_DATE)
for source, cfg in SOURCES.items():
    if cfg["frequency"] != "daily":
        continue
    arrived = log.filter((F.col("source") == source) & F.col("status").isin("loaded", "skipped_duplicate")).count() > 0
    check("freshness", source, not arrived, "arrived" if arrived else "missing", "file for run_date")

# 2. Bank reconciliation: unique transactions in the bank file must match the bank's own control total
pay, ctl = bronze_for_today("bank_payments"), bronze_for_today("bank_control")
if pay is not None and ctl is not None and ctl.limit(1).count():
    uniq = pay.dropDuplicates(["bank_txn_id"]).select(clean_money("amount").alias("amount"))
    ours = uniq.agg(F.sum("amount").alias("t"), F.count("*").alias("n")).first()
    bank = ctl.select(clean_money("control_total").alias("t"), F.col("txn_count").cast("int").alias("n")).first()
    check("bank_reconciliation_total", "bank_payments", ours.t != bank.t, ours.t, bank.t)
    check("bank_reconciliation_count", "bank_payments", ours.n != bank.n, ours.n, bank.n)

# 3. Quarantine rate per source file
for source, q_table in [("placements", "placements_quarantine"), ("bank_payments", "payments_quarantine"),
                        ("contact_attempts", "contacts_quarantine")]:
    b = bronze_for_today(source)
    if b is None or not table_exists(tbl(SILVER, q_table)):
        continue
    rows_in = b.count()
    if rows_in == 0:
        continue
    bad = spark.table(tbl(SILVER, q_table)).filter(F.col("_file_date") == RUN_DATE).count()
    rate = round(100 * bad / rows_in, 1)
    check("quarantine_rate_pct", source, rate > 50, rate, "<= 10", warn_if=rate > 10)

# 4. Unique keys
for t, keys in [(tbl(SILVER, "accounts"), ["client_code", "client_account_ref"]),
                (tbl(SILVER, "payments"), ["bank_txn_id"]),
                (tbl(SILVER, "contact_attempts"), ["attempt_id"]),
                (tbl(GOLD, "fact_payments"), ["bank_txn_id"]),
                (tbl(GOLD, "fact_account_snapshot"), ["snapshot_date", "account_sk"])]:
    dupes = spark.table(t).groupBy(*keys).count().filter("count > 1").count()
    check("unique_key", t.split(".")[-1], dupes > 0, dupes, 0)

current_dupes = (spark.table(tbl(SILVER, "debtors")).filter("is_current")
                 .groupBy("debtor_key").count().filter("count > 1").count())
check("one_current_version", "debtors", current_dupes > 0, current_dupes, 0)

# 5. Orphans in facts
orphans = spark.table(tbl(GOLD, "fact_payments")).filter("account_sk IS NULL OR debtor_sk IS NULL").count()
check("orphan_facts", "fact_payments", orphans > 0, orphans, 0)

# 6. Money we couldn't match to an account (still unresolved)
if table_exists(tbl(SILVER, "payments_quarantine")):
    stuck = (spark.table(tbl(SILVER, "payments_quarantine")).alias("q")
             .join(spark.table(tbl(SILVER, "payments")).alias("p"), "bank_txn_id", "left_anti")
             .agg(F.count("*").alias("n"), F.sum("amount").alias("amt")).first())
    check("unmatched_payments", "payments_quarantine", False, f"{stuck.n} txn, ${stuck.amt or 0}", "0", warn_if=stuck.n > 0)

# COMMAND ----------

dq = (spark.createDataFrame(results, "run_date string, check_name string, target string, status string, observed string, expected string")
           .withColumn("checked_at", F.current_timestamp()))
replace_slice(dq, tbl(META, "dq_results"), f"run_date = '{RUN_DATE}'")
show(dq.drop("checked_at").orderBy(F.desc("status"), "check_name"), 50)

fails = [r for r in results if r[3] == "FAIL"]
warns = [r for r in results if r[3] == "WARN"]
log_step("10_data_quality", status="failed" if fails else "success", passed=len(results) - len(fails) - len(warns),
         warnings=len(warns), failures=len(fails))
if fails:
    raise Exception(f"{len(fails)} data quality check(s) failed for {RUN_DATE}: {[(r[1], r[2]) for r in fails]}")
