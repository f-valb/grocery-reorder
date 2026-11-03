# grocery-reorder

Interview build: sales, stock and waste for a 20-store grocery chain, plus next week's reorder quantities. Build rules live in `~/.claude/CLAUDE.md`.

- CLI profile: `export DATABRICKS_CONFIG_PROFILE=food` (DEFAULT is out of credits)
- `WAREHOUSE_ID=5d7ed27d256c7dcf`
- Catalog `workspace`, schema `grocery_reorder`, raw files in volume `/Volumes/workspace/grocery_reorder/raw` (v1, on `main`)
- Branch `multi-source-ml` (v2: mocked POS/ERP/SaaS sources, governance, trained demand model): schema `grocery_intel`, volume `/Volumes/workspace/grocery_intel/raw`
- Local test: `OUT_DIR=<dir> ~/.local/venvs/pyspark/bin/python notebooks/00_generate_data.py`, then `RAW_DIR=<dir> DATA_DIR=<dir> ~/.local/venvs/pyspark/bin/python scripts/local_spark.py 01_bronze 02_silver 03_gold 04_demo_queries`
- Demo assets (v2): Genie space `genie/build_genie_space.py --test` (id in genie/space_id.txt), dashboard `dashboards/build_dashboard.py` (id in dashboards/dashboard_id_intel.txt), run-of-show in docs/demo_flow.md
- Saved job: `jobs/build_job.py [--run]` (id in jobs/job_id.txt); runs 00-05 as one Lakeflow Job, schedule paused
