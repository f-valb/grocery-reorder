# Opening prompt template

Paste the block below into Claude Code, run from the repo you want built (a fresh `git init`, or a
copy of this repo). Fill the `<...>` fields; delete lines that don't apply. Everything under
"Environment" is what this build spent its first ~45 minutes discovering.

```
You are building a <30–45 minute> demo: <one-sentence goal, e.g. "pull-only legacy API -> S3 ->
Lakeflow medallion on Databricks -> governed BI + one batch ML model">. Work autonomously; only stop
for the decisions listed under "Ask me".

## Environment (verified, don't re-discover)
- Repo: <path>. Python 3.11 via uv. Tools in ~/.local/bin (databricks CLI >= 1.22, terraform) and
  JDK 17 in ~/.local/opt/jdk17. Non-interactive shells: `export PATH=~/.local/bin:$PATH`.
- Databricks: <Free Edition | trial | paid> workspace <url>, CLI profile DEFAULT (OAuth, already logged in),
  one serverless SQL warehouse. Databricks agent skills installed in .claude/skills: read
  databricks-pipelines, databricks-dabs, databricks-aibi-dashboards before writing those parts.
- Free Edition facts: serverless only; UC grants need ACCOUNT groups (create via
  /api/2.0/account/scim/v2 from the workspace + /api/2.0/preview/permissionassignments + entitlements);
  membership takes minutes to propagate; system.billing.usage is empty; Lakehouse Monitoring works;
  no account console (no OIDC federation policies, no budget policies).
- Local AWS: LocalStack pinned to 4.9.2 via docker-compose (S3, SQS, CloudWatch, SNS). Terraform is
  written and `terraform validate`d, never applied.
- GitHub: <owner>/<repo>, <private|public>, plan <free|pro>. Private+free => no environment required
  reviewers (use templates/ci/wait_for_approval.sh). The gh token needs the `workflow` scope to push
  workflow files.

## Start from templates/ (copy, then adapt; see templates/README.md)
scripts/dbsql.py, pipelines/medallion_template.py, bundle/*, datagen/*, governance/*, ml/*,
dashboards/build_dashboards.py, ci/*, tests/conftest.py.

## Scope (in build order; commit after each with evidence)
1. Data source mock + deterministic generator (shared entity profiles if ML is in scope).
2. Landing: <direct to UC Volume | poller -> S3 + Volume>, idempotent keys.
3. Medallion pipeline (bronze/silver/gold + quarantine + freshness), local Spark tests first.
4. Governance: catalogs/schemas, groups, row filter + mask, rls_check as service principals.
5. <ML: one model, gate, batch score, PSI monitor>  /  <skip>
6. Bundle (dev/staging/prod), dashboards, alerts.  7. CI (<skip | PR + release>).  8. README + demo script.

## Acceptance (verify each by running it; report PASS/FAIL with numbers)
- <criterion 1> ...

## Ask me only about
Creating anything public, pushing to GitHub, deleting data outside this demo's schemas, handling secrets
(write a script that I run instead), and scope cuts if the time budget is at risk.

## Working rules
Run long Databricks jobs in the background and keep building; never wait idle on a job. Before the
first ML training run, sanity-check labels (class balance) and feature scale vs live data. Never deploy
staging/prod by hand if CI will deploy them later.
```
