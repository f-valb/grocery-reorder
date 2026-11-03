# grocery-reorder

Interview build: sales, stock and waste for a 20-store grocery chain, plus next week's reorder quantities. Build rules live in `~/.claude/CLAUDE.md`.

- CLI profiles: `retail` (current demo workspace dbc-ce80c680-bd6d, `WAREHOUSE_ID=0599a8b59a3c62bc`, Chrome Profile 3) and `food` (dbc-0a174280-b54a, `WAREHOUSE_ID=5d7ed27d256c7dcf`). DEFAULT is out of credits
- Full redeploy to a workspace: `DATABRICKS_CONFIG_PROFILE=retail WAREHOUSE_ID=0599a8b59a3c62bc python3 deploy_all.py [--from STEP]`; saved ids are per profile (`*.<profile>.txt`)
- Catalog `workspace`, schema `grocery_reorder`, raw files in volume `/Volumes/workspace/grocery_reorder/raw` (v1, on `main`)
- Branch `multi-source-ml` (v2: mocked POS/ERP/SaaS sources, governance, trained demand model): schema `grocery_intel`, volume `/Volumes/workspace/grocery_intel/raw`
- Local test: `OUT_DIR=<dir> ~/.local/venvs/pyspark/bin/python notebooks/00_generate_data.py`, then `RAW_DIR=<dir> DATA_DIR=<dir> ~/.local/venvs/pyspark/bin/python scripts/local_spark.py 01_bronze 02_silver 03_gold 04_demo_queries`
- Demo assets (v2): Genie space `genie/build_genie_space.py --test` (id in genie/space_id.txt), dashboard `dashboards/build_dashboard.py` (id in dashboards/dashboard_id_intel.txt), run-of-show in docs/demo_flow.md
- Saved job: `jobs/build_job.py [--run]` (id in jobs/job_id.txt); runs 00, 00b governance, the pipeline, 03-05 as one Lakeflow Job, schedule paused
- Pipeline: `pipelines/grocery_sources.py` via `jobs/build_pipeline.py [--run]` (id in jobs/pipeline_id.txt); owns all bronze_* and silver_* (non-document) tables and gold_store_product_week
- Apps: `apps/deploy_apps.py` (deploy both) / `--stop` / `--stage` (copy apps/common for a local `streamlit run`); app venv `~/.local/venvs/apps`; agent test `python apps/store_agent/agent.py`
