# Databricks notebook source
# MAGIC %md
# MAGIC # 09_gold_kpi_views: the numbers management asked for
# MAGIC These are the practice-pack SQL answers, now running on the star schema. Power BI or Databricks SQL dashboards
# MAGIC read these views. Views (not tables) mean the logic lives in one place and is always up to date.
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

G = lambda name: tbl(GOLD, name)

views = {
"vw_collections_monthly": f"""
    SELECT c.client_name, d.year_month,
           ROUND(SUM(f.amount), 2)                         AS net_collected,
           SUM(CASE WHEN f.is_reversal THEN 1 ELSE 0 END)  AS reversals,
           COUNT(*)                                        AS n_payments
    FROM {G('fact_payments')} f
    JOIN {G('dim_client')} c ON c.client_sk = f.client_sk
    JOIN {G('dim_date')}   d ON d.date_key  = f.date_key
    GROUP BY c.client_name, d.year_month""",

# Aggregate payments to account level FIRST, then join: avoids the fan-out trap (balance counted once per payment)
"vw_collection_rate": f"""
    WITH paid AS (SELECT account_sk, SUM(amount) AS paid FROM {G('fact_payments')} GROUP BY account_sk)
    SELECT c.client_name,
           COUNT(*)                                                           AS n_accounts,
           ROUND(SUM(a.placed_balance), 2)                                    AS placed,
           ROUND(SUM(COALESCE(p.paid, 0)), 2)                                 AS collected,
           ROUND(100 * SUM(COALESCE(p.paid, 0)) / NULLIF(SUM(a.placed_balance), 0), 1) AS collection_rate_pct
    FROM {G('dim_account')} a
    JOIN {G('dim_client')} c ON c.client_sk = a.client_sk
    LEFT JOIN paid p         ON p.account_sk = a.account_sk
    GROUP BY c.client_name""",

"vw_ageing_latest": f"""
    SELECT s.snapshot_date, s.ageing_bucket,
           COUNT(*)                            AS n_accounts,
           ROUND(SUM(s.outstanding_balance), 2) AS outstanding_balance
    FROM {G('fact_account_snapshot')} s
    WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM {G('fact_account_snapshot')})
      AND s.status <> 'paid'
    GROUP BY s.snapshot_date, s.ageing_bucket""",

"vw_call_queue_90plus": f"""
    SELECT a.client_account_ref, c.client_name, d.debtor_name, d.phone_masked,
           s.outstanding_balance, s.last_payment_date, s.days_since_payment
    FROM {G('fact_account_snapshot')} s
    JOIN {G('dim_account')} a ON a.account_sk = s.account_sk
    JOIN {G('dim_client')}  c ON c.client_sk  = s.client_sk
    JOIN {G('dim_debtor')}  d ON d.debtor_key = a.debtor_key AND d.is_current
    WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM {G('fact_account_snapshot')})
      AND s.ageing_bucket = '4) 90+' AND s.status <> 'paid'""",

"vw_promise_kept_rate": f"""
    WITH ptp AS (
        SELECT attempt_id, account_sk, client_sk, CAST(attempt_ts AS DATE) AS ptp_date
        FROM {G('fact_contact_attempts')} WHERE is_promise_to_pay),
    kept AS (
        SELECT ptp.*, EXISTS (SELECT 1 FROM {G('fact_payments')} p
                              WHERE p.account_sk = ptp.account_sk AND p.amount > 0
                                AND p.value_date BETWEEN ptp.ptp_date AND date_add(ptp.ptp_date, 7)) AS was_kept
        FROM ptp)
    SELECT c.client_name, COUNT(*) AS promises, SUM(CAST(was_kept AS INT)) AS kept,
           ROUND(100.0 * SUM(CAST(was_kept AS INT)) / COUNT(*), 1) AS kept_rate_pct
    FROM kept JOIN {G('dim_client')} c ON c.client_sk = kept.client_sk
    GROUP BY c.client_name""",
}

for name, sql in views.items():
    spark.sql(f"CREATE OR REPLACE VIEW {G(name)} AS {sql}")
log_step("09_gold_kpi_views", views=len(views))

# COMMAND ----------

show(spark.table(G("vw_collection_rate")).orderBy(F.desc("collection_rate_pct")))

# COMMAND ----------

show(spark.table(G("vw_ageing_latest")).orderBy("ageing_bucket"))

# COMMAND ----------

show(spark.table(G("vw_call_queue_90plus")).orderBy(F.desc("outstanding_balance")), 10)

# COMMAND ----------

show(spark.table(G("vw_promise_kept_rate")))
