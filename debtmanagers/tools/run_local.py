"""
Run the Databricks notebooks on a laptop with open-source Spark + Delta Lake (no Databricks needed).
Used to test the project before uploading it. It provides small stand-ins for `dbutils.widgets`,
`dbutils.notebook.run/exit` and `display()`, then runs the notebooks exactly as written (cells split on "# COMMAND ----------").

    pip install pyspark==4.0.1 delta-spark==4.0.0
    python tools/run_local.py                      # setup + pipeline (13-15 Nov) + idempotency tests

Optional: DELTA_JARS=/path/delta-spark_2.13-4.0.0.jar,/path/delta-storage-4.0.0.jar if Maven is blocked.
"""
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS = os.path.join(ROOT, "notebooks")
WORK = tempfile.mkdtemp(prefix="debt_local_")
os.environ["LOCAL_LANDING"] = os.path.join(WORK, "landing")
os.environ["SAMPLE_DATA"] = os.path.join(ROOT, "data", "landing")

from pyspark.sql import SparkSession  # noqa: E402

builder = (SparkSession.builder.master("local[2]").appName("debt-local")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.sql.warehouse.dir", os.path.join(WORK, "warehouse"))
           .config("spark.sql.shuffle.partitions", "2")
           .config("spark.ui.enabled", "false")
           .config("spark.sql.session.timeZone", "Pacific/Auckland"))
if os.environ.get("DELTA_JARS"):
    spark = builder.config("spark.jars", os.environ["DELTA_JARS"]).getOrCreate()
else:
    from delta import configure_spark_with_delta_pip
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


class NotebookExit(Exception):
    pass


class _Widgets:
    def __init__(self):
        self.stack = [{}]

    def text(self, name, default, label=None):
        self.stack[-1].setdefault(name, default)

    def get(self, name):
        return self.stack[-1][name]


class _Notebook:
    def run(self, path, timeout=0, arguments=None):
        return run_notebook(path, arguments or {})

    def exit(self, value):
        raise NotebookExit(value)


class DBUtils:
    widgets = _Widgets()
    notebook = _Notebook()


dbutils = DBUtils()


def _cells(path):
    src = open(path, encoding="utf-8").read().replace("# Databricks notebook source\n", "", 1)
    return [c.strip("\n") for c in src.split("# COMMAND ----------")]


def _exec_cells(path, g):
    for cell in _cells(path):
        lines = cell.splitlines()
        if lines and all(l.startswith("# MAGIC") for l in lines if l.strip()):
            magic = "\n".join(l[len("# MAGIC"):].lstrip(" ") for l in lines).strip()
            if magic.startswith("%run"):
                target = magic.split()[1]
                _exec_cells(_resolve(target, path), g)
            continue  # %md and other magics are documentation
        exec(compile(cell, path, "exec"), g)


def _resolve(target, from_path):
    base = os.path.dirname(from_path) if from_path else NOTEBOOKS
    p = os.path.normpath(os.path.join(base, target))
    return p if p.endswith(".py") else p + ".py"


def display(df):
    """Stand-in for Databricks display(): print the first 20 rows."""
    df.show(20, truncate=False)


def run_notebook(path, params):
    path = _resolve(path, os.path.join(NOTEBOOKS, "x.py"))
    # Plain Spark has no "workspace" catalog, so use its built-in one (spark_catalog.debt_silver.accounts, ...)
    dbutils.widgets.stack.append({"catalog": "spark_catalog", **params})
    g = {"spark": spark, "dbutils": dbutils, "display": display, "__name__": "__notebook__"}
    try:
        _exec_cells(path, g)
        return None
    except NotebookExit as e:
        return str(e)
    finally:
        dbutils.widgets.stack.pop()


if __name__ == "__main__":
    try:
        run_notebook("./01_setup", {})
        run_notebook("./11_run_pipeline", {"start_date": "2026-11-13", "end_date": "2026-11-15"})
        run_notebook("./12_idempotency_test", {})
        print("\nLOCAL TEST RUN FINISHED OK")
    finally:
        spark.stop()
        shutil.rmtree(WORK, ignore_errors=True)
