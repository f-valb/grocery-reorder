"""Run SQL on the warehouse and print a table: python3 scripts/sql.py "SELECT ..." ["SELECT ..."]

Settings come from env vars, with defaults that fit a fresh project:
  WAREHOUSE_ID  default: first warehouse the CLI profile can see
  SQL_CATALOG   default: workspace
  SQL_SCHEMA    default: current folder name with dashes as underscores
"""
import json, os, subprocess, sys

if len(sys.argv) < 2:
    sys.exit(__doc__)
warehouse = os.environ.get("WAREHOUSE_ID")
if not warehouse:
    out = subprocess.run(["databricks", "warehouses", "list", "-o", "json"], capture_output=True, text=True)
    if out.returncode or not out.stdout.strip():
        sys.exit(out.stderr.strip() or "no warehouses visible; set WAREHOUSE_ID")
    warehouse = json.loads(out.stdout)[0]["id"]
catalog = os.environ.get("SQL_CATALOG", "workspace")
schema = os.environ.get("SQL_SCHEMA") or os.path.basename(os.getcwd()).replace("-", "_")

for stmt in sys.argv[1:]:
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(
        {"warehouse_id": warehouse, "statement": stmt, "wait_timeout": "50s",
         "catalog": catalog, "schema": schema})], capture_output=True, text=True)
    d = json.loads(out.stdout or "{}")
    if d.get("status", {}).get("state") != "SUCCEEDED":
        print(stmt, "\n ->", d.get("status", out.stderr)); continue
    cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
    if not cols:  # DROP / ALTER / COMMENT return no result set
        print(stmt.split()[0].upper(), "OK\n"); continue
    rows = d.get("result", {}).get("data_array", [])
    w = [max(len(str(x)) for x in [c] + [r[i] for r in rows]) for i, c in enumerate(cols)]
    print(" | ".join(c.ljust(w[i]) for i, c in enumerate(cols)))
    for r in rows:
        print(" | ".join(str(x).ljust(w[i]) for i, x in enumerate(r)))
    print()
