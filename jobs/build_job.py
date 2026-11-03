"""Lakeflow Job: the whole grocery pipeline as one saved, scheduled job (schedule paused), with an optional run.

    DATABRICKS_CONFIG_PROFILE=food python3 jobs/build_job.py [--run]

Uploads notebooks/ to /Users/<me>/<folder>/notebooks, then creates the job (or resets it, using the id in
jobs/job_id.<profile>.txt). Task graph:

    ingest_sources -> governance_setup -> data_product_pipeline -> model_forecast_reorder -> demo_checks
                                                                                       \\-> documents_vector_index

data_product_pipeline is a pipeline task: the Lakeflow Declarative Pipeline from jobs/build_pipeline.py (created or
updated first) builds bronze, silver and gold_store_product_week, so Unity Catalog lineage shows the pipeline as their
producer. governance_setup runs first because the pipeline declares the row filter and mask functions it creates.

--run starts one run, waits for it and prints each task's state and the run URL. Auth comes from the CLI profile.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from build_pipeline import ensure_pipeline

HERE = Path(__file__).resolve().parent
PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")  # ids are saved per workspace profile
ROOT = HERE.parent
JOB_NAME = "Grocery intelligence: sources to forecast, reorder and documents"
TASKS = [  # task key, notebook, upstream tasks, what it does (shown as the task description)
    ("ingest_sources", "00_generate_data", [], "Land the POS, ERP and SaaS extracts (mocked) in the raw volume"),
    ("governance_setup", "00b_governance", ["ingest_sources"],
     "Store manager mapping, row filter and cost mask functions (the pipeline's gold table uses them)"),
    ("data_product_pipeline", None, ["governance_setup"],
     "Pipeline: sources to bronze (explicit schemas), silver (one store-item key, timezones, quarantine), gold weekly facts"),
    ("model_forecast_reorder", "03_gold", ["data_product_pipeline"],
     "Train and gate the demand model, next week's forecast and reorder list, backtest, checks"),
    ("demo_checks", "04_demo_queries", ["model_forecast_reorder"], "Business answers, data product checks, rerun check, plan"),
    ("documents_vector_index", "05_documents_rag", ["model_forecast_reorder"],
     "Parse PDF/Word/image documents, sync the vector index, grounded answers and store briefs"),
]


def cli(*args):
    out = subprocess.run(["databricks", *args, "-o", "json"], capture_output=True, text=True)
    if out.returncode:
        raise SystemExit(out.stderr.strip())
    return json.loads(out.stdout or "{}")


def job_settings(notebook_dir, pipeline_id):
    # A task with no notebook is the pipeline task; full refresh rebuilds every materialized view from the raw files
    tasks = [{"task_key": key, "description": text, "depends_on": [{"task_key": d} for d in deps],
              **({"notebook_task": {"notebook_path": f"{notebook_dir}/{nb}"}} if nb else
                 {"pipeline_task": {"pipeline_id": pipeline_id, "full_refresh": True}})}
             for key, nb, deps, text in TASKS]
    # No dashboard refresh task: AI/BI dashboards query the live gold tables when opened, and on Free Edition a
    # dashboard task fails ("Failed to create delegation claim")
    return {"name": JOB_NAME, "tasks": tasks, "max_concurrent_runs": 1,
            "schedule": {"quartz_cron_expression": "0 0 6 ? * MON", "timezone_id": "UTC", "pause_status": "PAUSED"},
            "tags": {"project": "grocery-reorder", "layer": "end-to-end"}}


if __name__ == "__main__":
    user = cli("current-user", "me")["userName"]
    notebook_dir = f"/Users/{user}/{ROOT.name}/notebooks"
    subprocess.run(["databricks", "workspace", "import-dir", str(ROOT / "notebooks"), notebook_dir, "--overwrite"],
                   check=True, capture_output=True)
    pipeline_id = ensure_pipeline(user)
    print(f"pipeline: {pipeline_id}")
    settings = job_settings(notebook_dir, pipeline_id)
    id_file = HERE / f"job_id.{PROFILE}.txt"
    if id_file.exists():
        job_id = id_file.read_text().strip()
        cli("jobs", "reset", "--json", json.dumps({"job_id": int(job_id), "new_settings": settings}))
    else:
        job_id = str(cli("jobs", "create", "--json", json.dumps(settings))["job_id"])
        id_file.write_text(job_id + "\n")
    host = json.loads(subprocess.run(["databricks", "auth", "describe", "-o", "json"], capture_output=True, text=True).stdout)
    host = host["details"]["host"].rstrip("/")
    print(f"job: {host}/jobs/{job_id}")
    if "--run" in sys.argv:
        run_id = cli("jobs", "run-now", job_id, "--no-wait")["run_id"]
        while (run := cli("jobs", "get-run", str(run_id)))["state"]["life_cycle_state"] not in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            time.sleep(20)
        print(run.get("run_page_url", ""))
        for t in run.get("tasks", []):
            s = t["state"]
            print(f"{t['task_key']}: {s.get('result_state', s['life_cycle_state'])} {s.get('state_message', '')[:300]}")
