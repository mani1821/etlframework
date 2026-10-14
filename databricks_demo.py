# Databricks notebook source
# MAGIC %md
# MAGIC # ETL parity framework: sample-data demo
# MAGIC Put `validator.py`, `generate_sample_data.py` and `tables_databricks.yaml` in one workspace folder (or Repo)
# MAGIC next to this notebook. Use a classic cluster in single-user mode (serverless does not support `.cache()`).

# COMMAND ----------

import os, sys

FOLDER = os.getcwd()                    # the notebook's folder (DBR 13+); or hard-code "/Workspace/Users/<you>/etl_parity"
CATALOG, DEMO = "main", "parity_demo"   # if you change these, change them in tables_databricks.yaml too
sys.path.insert(0, FOLDER)

# COMMAND ----------

# MAGIC %md ## 1. Create the sample tables (once) and the results Volume

# COMMAND ----------

import runpy
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DEMO}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{DEMO}.results")
sys.argv = ["generate_sample_data.py", "--tables", f"{CATALOG}.{DEMO}"]
runpy.run_path(f"{FOLDER}/generate_sample_data.py")   # -> {CATALOG}.{DEMO}_source.* and {CATALOG}.{DEMO}_target.*

# COMMAND ----------

# MAGIC %md ## 2. Run the validator

# COMMAND ----------

from validator import main

try:
    main([f"{FOLDER}/tables_databricks.yaml",
          "--param", "SRC_BATCH_ID=101", "--param", "TGT_BATCH_ID=9001",
          "--param", "SRC_RUN_START=2026-10-13 02:00:00", "--param", "TGT_RUN_START=2026-10-13 04:00:00"])
except SystemExit as e:          # main() ends with SystemExit(1) when any check FAILs; expected here (bugs are planted)
    print("exit code:", e.code)

# COMMAND ----------

# MAGIC %md ## 3. Look at the results

# COMMAND ----------

RESULTS = f"/Volumes/{CATALOG}/{DEMO}/results"
res = spark.read.parquet(f"{RESULTS}/results")
latest = res.agg({"run_ts": "max"}).first()[0]
display(res.where(res.run_ts == latest).orderBy("table_name", "check_name"))

# COMMAND ----------

# Example bad rows, e.g. FACT_SALES value differences in the sample
display(spark.read.parquet(f"{RESULTS}/mismatches").where("table = 'FACT_SALES' AND kind = 'column_mismatch_sample'"))
