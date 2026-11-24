# Databricks notebook source
# MAGIC %md
# MAGIC # 11_run_pipeline: run every step for a range of business dates
# MAGIC Use this to run the demo end to end, or to **backfill**: set `start_date` and `end_date` and each day runs in order.
# MAGIC
# MAGIC In production you'd use the Databricks **Job** in `resources/debt_pipeline_job.yml` instead. It runs the same
# MAGIC notebooks as separate tasks with dependencies, retries and failure alerts, with `run_date` passed in as a parameter.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

start_date = get_param("start_date", "2026-11-13")
end_date = get_param("end_date", "2026-11-15")

STEPS = [
    "02_bronze_ingest",
    "03_silver_clients",
    "04_silver_placements",
    "05_silver_payments",
    "06_silver_contacts",
    "07_gold_dimensions",
    "08_gold_facts",
    "09_gold_kpi_views",
    "10_data_quality",
]

day = dt.date.fromisoformat(start_date)
last_day = dt.date.fromisoformat(end_date)

while day <= last_day:
    print(f"===== {day} =====")
    for step in STEPS:
        result = dbutils.notebook.run(f"./{step}", 1800, {"run_date": day.isoformat(), "catalog": CATALOG})
        if result:
            print(f"  {step}: {result}")
    day += dt.timedelta(days=1)

# COMMAND ----------

display(spark.table(f"{META}.pipeline_runs")
             .filter(F.col("run_date").between(start_date, end_date))
             .orderBy("logged_at")
             .select("run_date", "step", "status", "details"))

# COMMAND ----------

display(spark.table(f"{META}.dq_results").filter("status <> 'PASS'").orderBy("run_date", "check_name"))
