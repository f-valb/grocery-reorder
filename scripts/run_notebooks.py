"""Upload notebooks/ to the workspace and run the given notebooks in order as one serverless job.

    DATABRICKS_CONFIG_PROFILE=food python3 scripts/run_notebooks.py 00_generate_data 01_bronze

Run from the project root: the project name is the current folder's name (override with PROJECT=...).
Prints each task's state and the run URL. Auth comes from the CLI profile, never from this file.
"""

import json
import os
import subprocess
import sys
import time

CLI = "databricks"
PROJECT = os.environ.get("PROJECT") or os.path.basename(os.getcwd())


def cli(*args):
    out = subprocess.run([CLI, *args, "-o", "json"], capture_output=True, text=True)
    if out.returncode:
        sys.exit(out.stderr.strip())
    return json.loads(out.stdout) if out.stdout.strip() else {}


names = sys.argv[1:]
if not names or not os.path.isdir("notebooks"):
    sys.exit(__doc__)

user = cli("current-user", "me")["userName"]
dest = f"/Users/{user}/{PROJECT}/notebooks"
subprocess.run([CLI, "workspace", "mkdirs", dest], check=True)
subprocess.run([CLI, "workspace", "import-dir", "notebooks", dest, "--overwrite"], check=True, capture_output=True)

tasks = []
for i, n in enumerate(names):
    t = {"task_key": n, "notebook_task": {"notebook_path": f"{dest}/{n}"}}
    if i:
        t["depends_on"] = [{"task_key": names[i - 1]}]
    tasks.append(t)

submitted = cli("jobs", "submit", "--no-wait", "--json",
                json.dumps({"run_name": f"{PROJECT}: " + " > ".join(names), "tasks": tasks}))
while True:
    run = cli("jobs", "get-run", str(submitted["run_id"]))
    if run["state"]["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        break
    time.sleep(10)
print(run.get("run_page_url", ""))
for t in run.get("tasks", []):
    s = t["state"]
    print(f"{t['task_key']}: {s.get('result_state', s['life_cycle_state'])} {s.get('state_message', '')}")
    if s.get("result_state") not in ("SUCCESS", "UPSTREAM_FAILED"):
        print("  error:", cli("jobs", "get-run-output", str(t["run_id"])).get("error", "")[:2000])
