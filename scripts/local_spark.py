"""Run the project's Databricks notebooks on local PySpark before spending a serverless run.

    DATA_DIR=/path/to/raw ~/.local/venvs/pyspark/bin/python scripts/local_spark.py 02_silver 03_gold 04_demo_queries

Run it from the project root, with a Python that has PySpark 3.5 (see README: one-time venv setup).
- Bronze is built from the generator's local parquet in $DATA_DIR (files starting with "_" are skipped):
  every <name>.parquet becomes table bronze_<name> with _source_file and _ingested_at, like 01_bronze.
- Each notebook in notebooks/ runs cell by cell (split on "# COMMAND ----------"), sharing one namespace.
- spark.sql skips statements local Spark can't run: USE, COMMENT, ALTER, CREATE SCHEMA / VOLUME.
  So notebooks must use `USE catalog.schema` plus bare table names (not workspace.x.table) in queries.
- Cells whose code (not comments) uses Delta history, time travel or matplotlib are skipped and listed.
- display(df) prints the first 40 rows.
Tables live in a fresh temp warehouse per run, so nothing is left over. Settings: ANSI on, UTC, 8 shuffle partitions.
"""
import glob
import os
import shutil
import sys
import tempfile

if len(sys.argv) < 2 or not os.path.isdir("notebooks"):
    sys.exit(__doc__)
DATA_DIR = os.environ.get("DATA_DIR") or sys.exit("set DATA_DIR to the generator's local output folder")

# Java and the worker Python must be set before Spark starts (a different worker Python fails with PYTHON_VERSION_MISMATCH)
jdk = os.path.expanduser("~/.local/opt/jdk17")
os.environ.setdefault("JAVA_HOME", f"{jdk}/Contents/Home" if os.path.isdir(f"{jdk}/Contents/Home") else jdk)
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["TZ"] = "UTC"

from pyspark.sql import SparkSession, functions as F  # noqa: E402

WAREHOUSE = os.path.join(tempfile.gettempdir(), "local_spark", os.path.basename(os.getcwd()))
shutil.rmtree(WAREHOUSE, ignore_errors=True)  # managed tables from an earlier run would block saveAsTable
real = (SparkSession.builder.master("local[4]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate())
real.sparkContext.setLogLevel("ERROR")

SKIP_SQL = ("USE", "COMMENT", "ALTER", "CREATE SCHEMA", "CREATE VOLUME")
SKIP_CELL = ("DESCRIBE HISTORY", "versionAsOf", "VERSION AS OF", "matplotlib")


class LocalSpark:
    """The real SparkSession, except for Unity Catalog statements that only exist on Databricks."""
    def __getattr__(self, name):
        return getattr(real, name)

    def sql(self, query, *args, **kwargs):
        if " ".join(query.split()).upper().startswith(SKIP_SQL):
            return None
        return real.sql(query, *args, **kwargs)


def display(df, *_, **__):
    df.show(40, truncate=False)


for path in sorted(glob.glob(os.path.join(DATA_DIR, "*.parquet"))):
    name = os.path.basename(path)[:-len(".parquet")]
    if not name.startswith("_"):
        (real.read.parquet(path).withColumn("_source_file", F.lit(path)).withColumn("_ingested_at", F.current_timestamp())
         .write.mode("overwrite").saveAsTable(f"bronze_{name}"))

scope = {"spark": LocalSpark(), "display": display, "__name__": "__main__"}
for notebook in sys.argv[1:]:
    print(f"===== {notebook}")
    for i, cell in enumerate(open(f"notebooks/{notebook}.py").read().split("# COMMAND ----------")):
        code = "\n".join(line for line in cell.splitlines() if not line.lstrip().startswith("#"))
        hit = next((k for k in SKIP_CELL if k in code), None)
        if hit:
            first = next((line for line in cell.splitlines() if line.strip()), "")
            print(f"[skipped cell {i}: uses {hit}] {first[:90]}")
            continue
        exec(compile(cell, f"{notebook}.py cell {i}", "exec"), scope)
print("local run OK")
