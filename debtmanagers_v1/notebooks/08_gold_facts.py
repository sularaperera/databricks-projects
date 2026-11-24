# Databricks notebook source
# MAGIC %md
# MAGIC # 08_gold_facts: payments, contact attempts and a daily account snapshot
# MAGIC | Fact | One row = | How it's loaded |
# MAGIC |---|---|---|
# MAGIC | `fact_payments` | one bank transaction | MERGE on `bank_txn_id` |
# MAGIC | `fact_contact_attempts` | one call / SMS / email / letter | MERGE on `attempt_id` |
# MAGIC | `fact_account_snapshot` | one account on one day | `replaceWhere snapshot_date = run_date` |
# MAGIC
# MAGIC **Point-in-time debtor key:** each fact gets the debtor *version* that was valid on the event date, so history
# MAGIC stays correct after a phone or address change. Events from before the debtor was first loaded get the earliest version.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dim_account = spark.table(f"{GOLD}.dim_account").select("account_sk", "client_sk", "client_account_ref", "debtor_key")
dim_debtor = spark.table(f"{GOLD}.dim_debtor").select("debtor_key", "debtor_sk", "valid_from", "valid_to")


def add_debtor_sk(df, event_date_col, event_key):
    """Join each event to the debtor version that was valid on the event date.
    If none was valid (the event is older than the first version), use the earliest version."""
    joined = df.join(dim_debtor, "debtor_key", "left")
    valid_on_event_date = (F.col("valid_from") <= F.col(event_date_col)) & (F.col(event_date_col) < F.col("valid_to"))
    best_version_first = Window.partitionBy(event_key).orderBy(valid_on_event_date.desc(), F.col("valid_from").asc())
    return (joined
        .withColumn("rn", F.row_number().over(best_version_first))
        .filter("rn = 1")
        .drop("rn", "valid_from", "valid_to", "debtor_key"))


def merge_fact(df, table, key):
    """Insert new rows, update a row only if something in it changed."""
    other_columns = [c for c in df.columns if c != key]
    df = df.withColumn("row_hash", make_hash(*other_columns))
    return run_merge(df, table, f"""
        MERGE INTO {table} t
        USING updates s
        ON t.{key} = s.{key}
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

payments = (spark.table(f"{SILVER}.payments")
    .join(dim_account, "client_account_ref")
    .withColumn("date_key", F.date_format("value_date", "yyyyMMdd").cast("int")))

fact_payments = add_debtor_sk(payments, "value_date", "bank_txn_id").select(
    "bank_txn_id", "date_key", "value_date", "account_sk", "client_sk", "debtor_sk",
    "amount", "transaction_type", "is_reversal", "loaded_date")

payments_inserted, payments_updated = merge_fact(fact_payments, f"{GOLD}.fact_payments", "bank_txn_id")

# COMMAND ----------

contacts = (spark.table(f"{SILVER}.contact_attempts")
    .join(dim_account, "client_account_ref")
    .withColumn("attempt_date", F.to_date("attempt_ts"))
    .withColumn("date_key", F.date_format("attempt_ts", "yyyyMMdd").cast("int")))

fact_contacts = add_debtor_sk(contacts, "attempt_date", "attempt_id").select(
    "attempt_id", "date_key", "attempt_ts", "account_sk", "client_sk", "debtor_sk", "channel", "outcome", "agent_id",
    F.col("outcome").isin("right_party_contact", "promise_to_pay").alias("is_right_party_contact"),
    (F.col("outcome") == "promise_to_pay").alias("is_promise_to_pay"))

contacts_inserted, contacts_updated = merge_fact(fact_contacts, f"{GOLD}.fact_contact_attempts", "attempt_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily account snapshot
# MAGIC Answers "what did the book look like on day X?" without recalculating history in every report.
# MAGIC * Ageing = days since the last positive payment, or since placement if they never paid.
# MAGIC * A payment only counts once it happened **and** we had received it (`loaded_date`). So a late payment shows up
# MAGIC   from the day it arrives, and an old snapshot never changes when you rerun it.
# MAGIC * To restate older days, rerun those dates (backfill).

# COMMAND ----------

as_of = F.lit(RUN_DATE).cast("date")

paid_so_far = (spark.table(f"{GOLD}.fact_payments")
    .filter((F.col("value_date") <= as_of) & (F.col("loaded_date") <= as_of))
    .groupBy("account_sk")
    .agg(F.sum("amount").alias("paid_to_date"),
         F.max(F.when(F.col("amount") > 0, F.col("value_date"))).alias("last_payment_date")))

snapshot = (spark.table(f"{GOLD}.dim_account")
    .filter(F.col("first_seen_date") <= as_of)
    .join(paid_so_far, "account_sk", "left")
    .withColumn("paid_to_date", F.coalesce("paid_to_date", F.lit(0)).cast("decimal(12,2)"))
    .withColumn("outstanding_balance", (F.col("placed_balance") - F.col("paid_to_date")).cast("decimal(12,2)"))
    .withColumn("days_since_payment", F.datediff(as_of, F.coalesce("last_payment_date", "placed_date"))))

snapshot = (snapshot
    .withColumn("ageing_bucket",
        F.when(F.col("days_since_payment") <= 30, "1) 0-30")
         .when(F.col("days_since_payment") <= 60, "2) 31-60")
         .when(F.col("days_since_payment") <= 90, "3) 61-90")
         .otherwise("4) 90+"))
    .withColumn("status",
        F.when(F.col("outstanding_balance") <= 0, "paid")
         .when(F.col("days_since_payment") <= 35, "paying")
         .otherwise("not_paying")))

snapshot = snapshot.select(
    as_of.alias("snapshot_date"),
    F.date_format(as_of, "yyyyMMdd").cast("int").alias("date_key"),
    "account_sk", "client_sk", "placed_balance", "paid_to_date", "outstanding_balance",
    "last_payment_date", "days_since_payment", "ageing_bucket", "status")

overwrite_slice(snapshot, f"{GOLD}.fact_account_snapshot", f"snapshot_date = DATE'{RUN_DATE}'", partition_col="snapshot_date")

log_step("08_gold_facts",
         f"payments_inserted={payments_inserted}, payments_updated={payments_updated}, "
         f"contacts_inserted={contacts_inserted}, snapshot_rows={snapshot.count()}")
