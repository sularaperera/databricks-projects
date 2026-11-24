# Databricks notebook source
# MAGIC %md
# MAGIC # 03_silver_clients: the companies that place debts with us
# MAGIC A small table that rarely changes, so it uses **SCD Type 1**: a MERGE on `client_code` that overwrites changed values.
# MAGIC (If we needed commission history for invoicing, we'd switch this to SCD2 like the debtors.)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

batch = read_bronze_batch("clients")
if batch is None or batch.isEmpty():
    log_step("03_silver_clients", status="no_data")
    dbutils.notebook.exit("no client file for this date")

# COMMAND ----------

clients = (batch
    .select(
        F.upper(clean_text("client_code")).alias("client_code"),
        F.initcap(clean_text("client_name")).alias("client_name"),
        F.initcap(clean_text("industry")).alias("industry"),
        F.expr("try_cast(commission_rate AS DECIMAL(5,4))").alias("commission_rate"),
        clean_date("onboarded_date").alias("onboarded_date"),
        "_row_id")
    .filter(F.col("client_code").isNotNull()))

# If the same client is in the file twice, keep the last row
last_row = Window.partitionBy("client_code").orderBy(F.col("_row_id").desc())
clients = (clients
    .withColumn("rn", F.row_number().over(last_row))
    .filter("rn = 1")
    .drop("rn", "_row_id"))

clients = (clients
    .withColumn("row_hash", make_hash("client_name", "industry", "commission_rate", "onboarded_date"))
    .withColumn("last_updated_date", F.lit(RUN_DATE).cast("date")))

# COMMAND ----------

# Update a client only if something changed, and never let an older run_date (a backfill) overwrite newer data
inserted, updated = run_merge(clients, f"{SILVER}.clients", f"""
    MERGE INTO {SILVER}.clients t
    USING updates s
    ON t.client_code = s.client_code
    WHEN MATCHED AND t.row_hash <> s.row_hash AND s.last_updated_date >= t.last_updated_date
        THEN UPDATE SET *
    WHEN NOT MATCHED
        THEN INSERT *
""")

log_step("03_silver_clients", f"inserted={inserted}, updated={updated}")
display(spark.table(f"{SILVER}.clients"))
