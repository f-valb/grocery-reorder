"""Deploy the whole grocery build to one workspace, in order, from this repo.

    DATABRICKS_CONFIG_PROFILE=retail WAREHOUSE_ID=0599a8b59a3c62bc python3 deploy_all.py [--from STEP]

Steps (each one can be rerun; --from skips the earlier ones):
  schema     schema + raw volume (the generator also creates them; here so the document upload has a target)
  documents  upload the PDF, Word and image source files to raw/document_files/<doc_type>/
  vector     Vector Search endpoint grocery_docs_vs (STANDARD), waits until ONLINE
  job        pipeline + saved job (jobs/build_job.py), one run: generator, governance, pipeline, model, checks, documents
  dashboard  AI/BI dashboard (dashboards/build_dashboard.py)
  genie      Genie space, then its 5 test questions (genie/build_genie_space.py --test)
  apps       both Databricks Apps (apps/deploy_apps.py)
Saved ids (job, pipeline, dashboard, Genie space) go to *.<profile>.txt files, so each workspace keeps its own.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RAW = "/Volumes/workspace/grocery_intel/raw"
VS_ENDPOINT = "grocery_docs_vs"
DOC_FILES = {  # file -> doc_type folder (05_documents_rag reads raw/document_files recursively)
    "supplier_coffee.pdf": "supplier_agreement",
    "visit_st07_2026_09.docx": "store_visit",
    "delivery_note_st07_2026_10_12.png": "delivery_note",
}


def run(*cmd, cwd=ROOT):
    print(f"$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True, cwd=cwd)


def cli(*args, check=True):
    out = subprocess.run(["databricks", *args, "-o", "json"], capture_output=True, text=True)
    if out.returncode and check:
        raise SystemExit(f"databricks {' '.join(args[:3])}: {out.stderr.strip()}")
    return json.loads(out.stdout or "{}") if not out.returncode else None


def step_schema():
    for statement in ["CREATE SCHEMA IF NOT EXISTS workspace.grocery_intel",
                      "CREATE VOLUME IF NOT EXISTS workspace.grocery_intel.raw"]:
        run(sys.executable, "scripts/sql.py", statement)


def step_documents():
    for name, doc_type in DOC_FILES.items():
        run("databricks", "fs", "mkdir", f"dbfs:{RAW}/document_files/{doc_type}")  # cp doesn't create folders in a volume
        run("databricks", "fs", "cp", "--overwrite", str(ROOT / "docs" / "source_files" / name),
            f"dbfs:{RAW}/document_files/{doc_type}/{name}")


def step_vector():
    if not cli("vector-search-endpoints", "get-endpoint", VS_ENDPOINT, check=False):
        cli("vector-search-endpoints", "create-endpoint", VS_ENDPOINT, "STANDARD", "--no-wait")
    for _ in range(60):
        state = cli("vector-search-endpoints", "get-endpoint", VS_ENDPOINT)["endpoint_status"]["state"]
        print(f"vector search endpoint: {state}", flush=True)
        if state == "ONLINE":
            return
        time.sleep(20)
    raise SystemExit("vector search endpoint not ONLINE after 20 minutes")


STEPS = {
    "schema": step_schema,
    "documents": step_documents,
    "vector": step_vector,
    "job": lambda: run(sys.executable, "build_job.py", "--run", cwd=ROOT / "jobs"),
    "dashboard": lambda: run(sys.executable, "dashboards/build_dashboard.py"),
    "genie": lambda: run(sys.executable, "genie/build_genie_space.py", "--test"),
    "apps": lambda: run(sys.executable, "apps/deploy_apps.py"),
}

if __name__ == "__main__":
    for var in ("DATABRICKS_CONFIG_PROFILE", "WAREHOUSE_ID"):
        if not os.environ.get(var):
            raise SystemExit(f"set {var} first (see the docstring)")
    first = sys.argv[sys.argv.index("--from") + 1] if "--from" in sys.argv else "schema"
    names = list(STEPS)
    for name in names[names.index(first):]:
        print(f"\n=== {name}", flush=True)
        STEPS[name]()
