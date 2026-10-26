# Templates

These are reusable pieces extracted from this build. Each one ran successfully on Databricks Free Edition (serverless), LocalStack 4.9.2 and GitHub Actions. Copy them, then change the lines marked `TODO` or the domain-specific names. Why each piece exists, and what it cost the first time, is in `~/Documents/GitHub/pos-lakehouse-demo/docs/retrospective.md`.

| Path | What it is | Change before use |
|---|---|---|
| `opening_prompt.md` | Opening prompt for the next build, with the environment facts filled in | Scope and time budget |
| `scripts/dbsql.py` | Runs a templated `.sql` file on a SQL warehouse (`-- @for_each_country` / `@for_each_env` blocks; nested country-in-env works) | Nothing |
| `scripts/run_for.py` | Runs a command for N seconds, then SIGINT, or `--kill` for SIGKILL. macOS has no `timeout` | Nothing |
| `scripts/run_notebooks.py` | Uploads `notebooks/` and runs the named notebooks as one chained serverless `jobs submit`; prints task states and the error of each failed task | Nothing (project name = folder name) |
| `scripts/sql.py` | Runs SQL statements on the warehouse and prints a table | Nothing (`WAREHOUSE_ID`, `SQL_CATALOG`, `SQL_SCHEMA` env vars) |
| `scripts/open_chrome_profile1.sh` | `BROWSER` for `databricks auth login`: opens the sign-in in Chrome "Profile 1" | Nothing |
| `scripts/job_outputs.py` | Prints every task's notebook result, including `for_each` iterations, for the latest run of a bundle job | Nothing |
| `scripts/model_versions.py` | Lists UC model versions, aliases and gate decisions | Nothing |
| `pipelines/medallion_template.py` | One-file SDP pipeline: Auto Loader, translate, quarantine, hash dedupe, AUTO CDC, gold views with row filter and freshness | Keys, rules, column mapping |
| `bundle/databricks.yml` | Bundle skeleton (dev/staging/prod, CI-safe auth, restricted roots) plus the pipeline resource snippet | Names |
| `bundle/job_foreach_condition_retrain.job.yml` | `for_each` over countries → monitor → `condition_task` → `run_job_task` retrain | Notebook paths |
| `bundle/alerts.yml` | Alerts v2 schema: freshness, drift and quarantine spike | Queries |
| `bundle/dashboards.yml` + `dashboards/build_dashboards.py` | AI/BI dashboards generated from Python: counters, lines, bars, tables, multi-dataset filters, theme | Datasets and widgets |
| `ingestion/resilience.py` | Circuit breaker, AIMD limiter, token bucket, adaptive window, backoff with jitter | Nothing |
| `ingestion/sinks.py`, `queues.py`, `state.py` | Write-once S3 and UC Volume sinks, SQS and in-memory queues, S3/DynamoDB/in-memory JSON stores | Nothing |
| `datagen/deterministic_generator.py` | Seeded per-minute generator with late updates, defects, duplicates and a traffic curve | Fields, defects |
| `datagen/shared_profiles.py` | Entity profiles shared by the live stream and the ML history seed | Entity, behaviour |
| `governance/10_security_functions.sql` | Row filter and mask UDFs that check both account and workspace groups | Group names |
| `governance/setup_identities.py` | Account groups through the workspace SCIM proxy, workspace assignment, entitlements, test service principals | Group list |
| `governance/rls_check.py` | Runs queries **as** test service principals, with short-lived secrets kept in memory | Expectations |
| `ml/registry.py` | Champion/challenger gate (pure, unit-tested), promotion with a `previous_champion` tag, rollback | Nothing |
| `ml/features.py` | One feature definition for train, score and monitor; PSI with time-trending exemptions | Features |
| `ml/forecast.py` | Many-models: `applyInPandas` fit plus one routing pyfunc | Model per group |
| `ml/notebook_train_gated.py` | Train → register → gate on a hash-of-entity holdout, split-policy aware | Model, features |
| `ml/notebook_psi_monitor.py` | PSI and data-quality monitor writing metrics plus task values for a retrain condition | Tables |
| `ci/ci.yml`, `ci/release.yml` | PR: lint, tests (Spark), validate ×3, deploy dev. Main: staging + integration tests → approval → prod | Nothing |
| `ci/wait_for_approval.sh` | Issue-based manual approval for private repos on the GitHub Free plan | Nothing |
| `ci/setup_ci.sh` | CI service principal: entitlements, group, warehouse, secret piped into `gh secret set` | Repo |
| `tests/conftest.py` | Local Spark fixture: ANSI on, `TZ=UTC`, JDK from `~/.local/opt/jdk17` | Nothing |
