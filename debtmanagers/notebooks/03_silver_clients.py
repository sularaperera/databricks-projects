# Databricks notebook source
# MAGIC %md
# MAGIC # 03_silver_clients: reference data for the companies that place debts with us
# MAGIC Small table with few changes, so it uses **SCD Type 1**: a MERGE on `client_code` that overwrites changed values.
# MAGIC (If commission history mattered for invoicing, you would switch this to SCD2 like the debtors.)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

bronze_t = tbl(BRONZE, "clients")
batch = spark.table(bronze_t).filter(F.col("_file_date") == RUN_DATE) if table_exists(bronze_t) else None

if batch is None or batch.limit(1).count() == 0:
    log_step("03_silver_clients", status="no_data")
    dbutils.notebook.exit("no client file for this date")

clients = (batch
    .select(clean_text("client_code", "upper").alias("client_code"),
            clean_text("client_name").alias("client_name"),
            clean_text("industry").alias("industry"),
            F.expr("try_cast(commission_rate AS DECIMAL(5,4))").alias("commission_rate"),
            clean_date("onboarded_date").alias("onboarded_date"),
            "_row_id")
    .filter(F.col("client_code").isNotNull())
    .withColumn("rn", F.row_number().over(Window.partitionBy("client_code").orderBy(F.col("_row_id").desc())))
    .filter("rn = 1").drop("rn", "_row_id")
    .withColumn("row_hash", row_hash("client_name", "industry", "commission_rate", "onboarded_date"))
    .withColumn("last_updated_date", F.lit(RUN_DATE).cast("date")))

m = upsert(clients, tbl(SILVER, "clients"), ["client_code"], only_if_newer="last_updated_date")
log_step("03_silver_clients", **m)
show(spark.table(tbl(SILVER, "clients")))
