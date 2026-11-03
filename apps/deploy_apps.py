"""Create or update the two Databricks Apps, upload their code and deploy them.

    DATABRICKS_CONFIG_PROFILE=food python3 apps/deploy_apps.py            # stage, create/update, start, deploy both
    DATABRICKS_CONFIG_PROFILE=food python3 apps/deploy_apps.py --stage    # only copy apps/common into each app (local run)
    DATABRICKS_CONFIG_PROFILE=food python3 apps/deploy_apps.py --stop     # stop both apps (they use credits while running)

Each app gets user authorization scopes, so it runs SQL, Genie and the model endpoint as the signed-in viewer
(Unity Catalog row filter and column mask apply per viewer), plus resources for its service principal.
"""

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
WAREHOUSE_ID = "5d7ed27d256c7dcf"
GENIE_SPACE_ID = (ROOT / "genie" / "space_id.txt").read_text().strip()
LLM = "databricks-meta-llama-3-3-70b-instruct"
WAREHOUSE = {"name": "sql-warehouse", "sql_warehouse": {"id": WAREHOUSE_ID, "permission": "CAN_USE"}}
APPS = {
    "grocery-critical-items": {
        "folder": "critical_items",
        "description": "Critical items per store: stockouts, waste and next week's shortfalls, ranked by dollars at stake",
        "user_api_scopes": ["sql"],
        "resources": [WAREHOUSE]},
    "grocery-store-agent": {
        "folder": "store_agent",
        "description": "Store assistant agent: answers with the gold data, the supplier documents and Genie, showing its tools",
        "user_api_scopes": ["sql", "dashboards.genie", "serving.serving-endpoints"],
        "resources": [WAREHOUSE,
                      {"name": "genie-space", "genie_space": {"name": "Grocery store assistant", "space_id": GENIE_SPACE_ID,
                                                              "permission": "CAN_RUN"}},
                      {"name": "llm", "serving_endpoint": {"name": LLM, "permission": "CAN_QUERY"}}]},
}


def cli(*args, check=True):
    out = subprocess.run(["databricks", *args, "-o", "json"], capture_output=True, text=True)
    if out.returncode and check:
        raise SystemExit(f"databricks {' '.join(args[:3])}: {out.stderr.strip()}")
    return json.loads(out.stdout or "{}") if not out.returncode else None


def stage():
    # Apps upload one folder: copy the shared data module next to each app.py (the copies are gitignored)
    for spec in APPS.values():
        shutil.copy(HERE / "common" / "grocery_data.py", HERE / spec["folder"] / "grocery_data.py")


def wait_compute(name, wanted="ACTIVE"):
    while (state := cli("apps", "get", name)["compute_status"]["state"]) not in (wanted, "ERROR"):
        time.sleep(15)
    return state


def deploy(name, spec, user):
    settings = {"name": name, "description": spec["description"], "user_api_scopes": spec["user_api_scopes"],
                "resources": spec["resources"]}
    if cli("apps", "get", name, check=False):
        cli("apps", "update", name, "--json", json.dumps(settings))
    else:
        cli("apps", "create", "--no-compute", "--no-wait", "--json", json.dumps(settings), check=False)
    source = f"/Workspace/Users/{user}/{ROOT.name}/apps/{spec['folder']}"
    subprocess.run(["databricks", "workspace", "import-dir", str(HERE / spec["folder"]), source, "--overwrite"],
                   check=True, capture_output=True)
    state = cli("apps", "get", name)["compute_status"]["state"]
    if state != "ACTIVE":
        if state == "STOPPED":  # a just-created app may already be STARTING: then only wait
            cli("apps", "start", name, "--no-wait")
        print(f"{name}: compute {wait_compute(name)}")
    result = cli("apps", "deploy", name, "--source-code-path", source)
    app = cli("apps", "get", name)
    print(f"{name}: deployment {result.get('status', {}).get('state')} {result.get('status', {}).get('message', '')}")
    print(f"   {app['url']}")


if __name__ == "__main__":
    stage()
    if "--stage" in sys.argv:
        sys.exit()
    if "--stop" in sys.argv:
        for name in APPS:
            cli("apps", "stop", name, "--no-wait")
            print(f"{name}: stopping")
        sys.exit()
    me = cli("current-user", "me")["userName"]
    for app_name, app_spec in APPS.items():
        deploy(app_name, app_spec, me)
