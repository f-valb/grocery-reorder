"""Pre-flight: prove compute works on every CLI profile, in parallel.

    python3 scripts/preflight.py            # all profiles in ~/.databrickscfg
    python3 scripts/preflight.py food dev   # only these

Per profile: one SQL statement on the first visible warehouse (or WAREHOUSE_ID) and one tiny
serverless notebook job (a 1-cell notebook imported to /Users/<me>/.preflight). Prints one line
per profile. Listing warehouses passes even when credits are gone, so both checks really run.
Auth comes from the CLI profiles, never from this file. Run from any folder.
"""

import configparser
import json
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

CLI = os.path.expanduser("~/.local/bin/databricks")
JOB_TIMEOUT_S = 600
NOTEBOOK = "# Databricks notebook source\nprint('preflight ok')\n"
OUT_OF_CREDITS = ("exhausted your available credits", "disabled temporarily")


def cli(profile, *args):
    """Run the CLI for one profile; parse stdout only (stderr carries warnings that break JSON)."""
    env = {k: v for k, v in os.environ.items() if k != "DATABRICKS_CONFIG_PROFILE"}
    out = subprocess.run([CLI, "-p", profile, *args], capture_output=True, text=True, env=env)
    data = {}
    if out.stdout.strip():
        try:
            data = json.loads(out.stdout)
        except json.JSONDecodeError:
            pass
    return out.returncode, data, out.stderr.strip()


def short(msg):
    msg = " ".join(str(msg).split())
    if any(s in msg for s in OUT_OF_CREDITS):
        return "OUT OF CREDITS"
    return msg[:120]


def check_sql(profile):
    warehouse = os.environ.get("WAREHOUSE_ID")
    if not warehouse:
        rc, whs, err = cli(profile, "warehouses", "list", "-o", "json")
        if rc or not whs:
            return "-", "FAIL " + short(err or "no warehouses visible")
        warehouse = whs[0]["id"]
    rc, d, err = cli(profile, "api", "post", "/api/2.0/sql/statements", "--json",
                     json.dumps({"warehouse_id": warehouse, "statement": "SELECT 1", "wait_timeout": "50s"}))
    # A cold warehouse can still be PENDING after 50s: poll the statement
    deadline = time.time() + 240
    while d.get("status", {}).get("state") in ("PENDING", "RUNNING") and time.time() < deadline:
        time.sleep(5)
        rc, d, err = cli(profile, "api", "get", f"/api/2.0/sql/statements/{d['statement_id']}")
    status = d.get("status", {})
    if status.get("state") == "SUCCEEDED":
        return warehouse, "SUCCEEDED"
    return warehouse, f"{status.get('state', 'FAIL')} " + short(status.get("error", {}).get("message") or err)


def check_job(profile):
    rc, me, err = cli(profile, "current-user", "me", "-o", "json")
    if rc:
        return "FAIL " + short(err)
    folder = f"/Users/{me['userName']}/.preflight"
    cli(profile, "workspace", "mkdirs", folder)
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(NOTEBOOK)
    try:
        rc, _, err = cli(profile, "workspace", "import", f"{folder}/preflight_check", "--file", f.name,
                         "--format", "SOURCE", "--language", "PYTHON", "--overwrite")
    finally:
        os.unlink(f.name)
    if rc:
        return "FAIL import " + short(err)
    rc, sub, err = cli(profile, "jobs", "submit", "--no-wait", "-o", "json", "--json", json.dumps(
        {"run_name": "preflight", "tasks": [{"task_key": "preflight",
                                             "notebook_task": {"notebook_path": f"{folder}/preflight_check"}}]}))
    if rc or "run_id" not in sub:
        return "FAIL " + short(err)
    deadline = time.time() + JOB_TIMEOUT_S
    while time.time() < deadline:
        rc, run, err = cli(profile, "jobs", "get-run", str(sub["run_id"]), "-o", "json")
        state = run.get("state", {})
        if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result = state.get("result_state", state.get("life_cycle_state"))
            return result if result == "SUCCESS" else f"{result} " + short(state.get("state_message", ""))
        time.sleep(10)
    return f"TIMEOUT after {JOB_TIMEOUT_S}s (run {sub['run_id']})"


def check(profile):
    with ThreadPoolExecutor(2) as pool:
        sql, job = pool.submit(check_sql, profile), pool.submit(check_job, profile)
        warehouse, sql_state = sql.result()
        return profile, warehouse, sql_state, job.result()


cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.databrickscfg"))
profiles = sys.argv[1:] or [p for p in ["DEFAULT", *cfg.sections()] if p != "__settings__" and
                            (p != "DEFAULT" or cfg.defaults())]
if not profiles:
    sys.exit("no profiles in ~/.databrickscfg")
print(f"checking {', '.join(profiles)} (SQL + serverless job; a cold start takes 1-3 min)", flush=True)
with ThreadPoolExecutor(len(profiles)) as pool:
    rows = list(pool.map(check, profiles))
w = [max(len(str(r[i])) for r in rows + [("profile", "warehouse", "sql", "job")]) for i in range(4)]
for r in [("profile", "warehouse", "sql", "job"), *rows]:
    print("  ".join(str(x).ljust(w[i]) for i, x in enumerate(r)))
ok = [r[0] for r in rows if r[2] == "SUCCEEDED" and r[3] == "SUCCESS"]
print("PASS:", ", ".join(ok) if ok else "none (ask for another workspace)")
