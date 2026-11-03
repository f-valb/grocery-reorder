"""Lakeflow Declarative Pipeline for bronze + silver (pipelines/grocery_sources.py), created or updated in place.

    DATABRICKS_CONFIG_PROFILE=food python3 jobs/build_pipeline.py [--run]

Uploads pipelines/ to /Users/<me>/<folder>/pipelines and creates the pipeline (or updates it, using the id in
jobs/pipeline_id.txt). Serverless, publishes to workspace.grocery_intel. jobs/build_job.py calls ensure_pipeline() and
runs it as a pipeline task; --run starts one update on its own and waits for it.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
PIPELINE_NAME = "Grocery sources: POS, ERP and SaaS extracts to bronze and silver"


def cli(*args):
    out = subprocess.run(["databricks", *args, "-o", "json"], capture_output=True, text=True)
    if out.returncode:
        raise SystemExit(out.stderr.strip())
    return json.loads(out.stdout or "{}")


def pipeline_settings(source_dir):
    return {"name": PIPELINE_NAME, "serverless": True, "catalog": "workspace", "schema": "grocery_intel",
            "libraries": [{"file": {"path": f"{source_dir}/grocery_sources.py"}}],
            "configuration": {"app.raw_dir": "/Volumes/workspace/grocery_intel/raw"},
            "continuous": False, "development": False, "channel": "CURRENT",
            "tags": {"project": "grocery-reorder", "layer": "bronze-silver"}}


def ensure_pipeline(user):
    source_dir = f"/Users/{user}/{ROOT.name}/pipelines"
    subprocess.run(["databricks", "workspace", "import-dir", str(ROOT / "pipelines"), source_dir, "--overwrite"],
                   check=True, capture_output=True)
    settings = pipeline_settings(source_dir)
    id_file = HERE / "pipeline_id.txt"
    if id_file.exists():
        pipeline_id = id_file.read_text().strip()
        cli("pipelines", "update", pipeline_id, "--json", json.dumps(settings))
    else:
        pipeline_id = cli("pipelines", "create", "--json", json.dumps(settings))["pipeline_id"]
        id_file.write_text(pipeline_id + "\n")
    return pipeline_id


if __name__ == "__main__":
    pipeline_id = ensure_pipeline(cli("current-user", "me")["userName"])
    print(f"pipeline: {pipeline_id}")
    if "--run" in sys.argv:
        update_id = cli("pipelines", "start-update", pipeline_id, "--full-refresh")["update_id"]
        while (state := cli("pipelines", "get-update", pipeline_id, update_id)["update"]["state"]) not in ("COMPLETED", "FAILED", "CANCELED"):
            time.sleep(15)
        print(f"update {update_id}: {state}")
