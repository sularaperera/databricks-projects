# Databricks notebook source
# MAGIC %md
# MAGIC # 09_gold_kpi_views: the numbers management asked for
# MAGIC These are the practice-pack SQL answers, now running on the star schema. Power BI or Databricks SQL dashboards
# MAGIC read these views. Because they are views (not tables), the logic lives in one place and is always up to date.
# MAGIC
# MAGIC | View | Question it answers |
# MAGIC |---|---|
# MAGIC | `vw_collections_monthly` | How much did we collect per client per month (net of reversals)? |
# MAGIC | `vw_collection_rate` | What % of the placed balance have we recovered per client? |
# MAGIC | `vw_ageing_latest` | How old is the unpaid book? |
# MAGIC | `vw_call_queue_90plus` | Who should the team call first? |
# MAGIC | `vw_promise_kept_rate` | Do promises to pay turn into payments within 7 days? |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_collections_monthly AS
SELECT
    c.client_name,
    d.year_month,
    ROUND(SUM(f.amount), 2)                         AS net_collected,
    SUM(CASE WHEN f.is_reversal THEN 1 ELSE 0 END)  AS reversals,
    COUNT(*)                                        AS n_payments
FROM {GOLD}.fact_payments f
JOIN {GOLD}.dim_client c ON c.client_sk = f.client_sk
JOIN {GOLD}.dim_date   d ON d.date_key  = f.date_key
GROUP BY c.client_name, d.year_month
""")

# COMMAND ----------

# Sum the payments per account FIRST, then join. If we joined first, each account's balance
# would be counted once per payment (the fan-out trap).
spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_collection_rate AS
WITH paid AS (
    SELECT account_sk, SUM(amount) AS paid
    FROM {GOLD}.fact_payments
    GROUP BY account_sk
)
SELECT
    c.client_name,
    COUNT(*)                                AS n_accounts,
    ROUND(SUM(a.placed_balance), 2)         AS placed,
    ROUND(SUM(COALESCE(p.paid, 0)), 2)      AS collected,
    ROUND(100 * SUM(COALESCE(p.paid, 0)) / NULLIF(SUM(a.placed_balance), 0), 1) AS collection_rate_pct
FROM {GOLD}.dim_account a
JOIN {GOLD}.dim_client c ON c.client_sk = a.client_sk
LEFT JOIN paid p         ON p.account_sk = a.account_sk
GROUP BY c.client_name
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_ageing_latest AS
SELECT
    s.snapshot_date,
    s.ageing_bucket,
    COUNT(*)                              AS n_accounts,
    ROUND(SUM(s.outstanding_balance), 2)  AS outstanding_balance
FROM {GOLD}.fact_account_snapshot s
WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM {GOLD}.fact_account_snapshot)
  AND s.status <> 'paid'
GROUP BY s.snapshot_date, s.ageing_bucket
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_call_queue_90plus AS
SELECT
    a.client_account_ref,
    c.client_name,
    d.debtor_name,
    d.phone_masked,
    s.outstanding_balance,
    s.last_payment_date,
    s.days_since_payment
FROM {GOLD}.fact_account_snapshot s
JOIN {GOLD}.dim_account a ON a.account_sk = s.account_sk
JOIN {GOLD}.dim_client  c ON c.client_sk  = s.client_sk
JOIN {GOLD}.dim_debtor  d ON d.debtor_key = a.debtor_key AND d.is_current
WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM {GOLD}.fact_account_snapshot)
  AND s.ageing_bucket = '4) 90+'
  AND s.status <> 'paid'
""")

# COMMAND ----------

# A promise counts as kept if a positive payment arrives within 7 days of the promise
spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_promise_kept_rate AS
WITH promises AS (
    SELECT attempt_id, account_sk, client_sk, CAST(attempt_ts AS DATE) AS promise_date
    FROM {GOLD}.fact_contact_attempts
    WHERE is_promise_to_pay
),
kept AS (
    SELECT
        pr.*,
        EXISTS (
            SELECT 1 FROM {GOLD}.fact_payments p
            WHERE p.account_sk = pr.account_sk
              AND p.amount > 0
              AND p.value_date BETWEEN pr.promise_date AND date_add(pr.promise_date, 7)
        ) AS was_kept
    FROM promises pr
)
SELECT
    c.client_name,
    COUNT(*)                                                 AS promises,
    SUM(CAST(was_kept AS INT))                               AS kept,
    ROUND(100.0 * SUM(CAST(was_kept AS INT)) / COUNT(*), 1)  AS kept_rate_pct
FROM kept
JOIN {GOLD}.dim_client c ON c.client_sk = kept.client_sk
GROUP BY c.client_name
""")

log_step("09_gold_kpi_views", "views=5")

# COMMAND ----------

display(spark.table(f"{GOLD}.vw_collection_rate").orderBy(F.desc("collection_rate_pct")))

# COMMAND ----------

display(spark.table(f"{GOLD}.vw_ageing_latest").orderBy("ageing_bucket"))

# COMMAND ----------

display(spark.table(f"{GOLD}.vw_call_queue_90plus").orderBy(F.desc("outstanding_balance")).limit(10))

# COMMAND ----------

display(spark.table(f"{GOLD}.vw_promise_kept_rate"))
