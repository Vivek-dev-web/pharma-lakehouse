# Pharma Lakehouse

An end-to-end Azure data engineering project on a synthetic pharma
supply-chain / clinical-safety domain, built to exercise the full breadth of
an Azure Data Engineer role. Every claim below has actually been deployed
and run, not just designed — **including the cost-driven pivots**: this
project ran on real Azure Databricks (Premium) for a while, hit a real
recurring cost it hadn't accounted for, and consolidated back onto a $0
workspace. That history is documented, not hidden — see
[docs/HLD.md section 7](docs/HLD.md#7-deployment-topology-and-current-state).

- **Databricks** (`medallion`, AWS Free Edition, catalog `pharmalake_dbx`)
  runs the full bronze → silver → gold pipeline, data quality checks, and
  Unity Catalog governance — at **$0**. This is the project's primary,
  actively maintained target.
- **Azure Data Factory + Synapse serverless SQL + ADLS Gen2** (all
  pre-existing in `rg-customer360-legacy`) demonstrate real database + public
  API integration, using managed-identity auth only — no passwords or
  storage keys anywhere in this repo. Still fully live.
- **Power BI** DirectQueries Databricks SQL on the live `medallion`
  workspace. **Synapse serverless SQL** also reads gold Delta tables
  directly, but only as a static snapshot from when the (now decommissioned)
  Azure Databricks workspace last exported them — see the status table below.

See [docs/architecture.md](docs/architecture.md) for the full diagram and
the platform constraints this build actually ran into (Unity Catalog storage
credentials being cloud-bound, Lakeflow rejecting explicit table paths under
UC governance, ADF needing a SQL login in addition to storage RBAC) — all
discovered by running this for real, not from documentation.

**New here?**
[docs/HLD.md](docs/HLD.md) is the system-level design (components, tech
stack, current deployment state). [docs/LLD.md](docs/LLD.md) is the
module-by-module detail (exact schemas, configs, and logic) underneath it.
[docs/DESIGN.md](docs/DESIGN.md) has the why (problem statement, decisions
and tradeoffs, cost model).
[docs/DATA_FLOW.md](docs/DATA_FLOW.md) has step-by-step diagrams of how data
actually moves through ADF and through the Databricks bronze/silver/gold
pipeline, with real row counts.
[docs/RUNBOOK.md](docs/RUNBOOK.md) is day-2 operations — trigger, verify,
troubleshoot, check cost.
[docs/DATABRICKS_GETTING_STARTED.md](docs/DATABRICKS_GETTING_STARTED.md) is
a from-zero walkthrough for deploying and running the Databricks side if
you haven't used Databricks before.

## What's actually live right now

| Component | Status | Evidence |
|---|---|---|
| Databricks bundle, `dev` target (`medallion`, $0) | **Live, primary target** | Full job succeeds end-to-end against catalog `pharmalake_dbx`: generate → Lakeflow pipeline → DQ → governance → gold export (no-op here) |
| Gold tables (UC-managed, `pharmalake_dbx.pharma_lakehouse`) | **Populated with real data** | 4,000 shipments, 1,200 adverse events, 500 batches, 25/25 DQ checks passing — verified via SQL warehouse |
| Power BI serving | **Live** | DirectQuery to `medallion`'s Serverless Starter Warehouse, catalog `pharmalake_dbx` |
| ADF pipeline (`pl_pharma_orchestrate`) | **Fully succeeds** | Both activities succeed: 500 rows copied from Synapse serverless SQL, records copied from the ClinicalTrials.gov API |
| Storage RBAC (`infra/rbac.bicep`) | **Applied** | ADF's, the access connector's, and Synapse workspace's managed identities all granted Storage Blob Data Contributor |
| Budget alert | **Active** | $25/month on `rg-customer360-legacy`, alerts to `vivekt94@gmail.com` at 50/80/100% |
| Azure Databricks workspace (`pharmalake-dbx`) | **Decommissioned 2026-08-29** | Deleted to stop a standing ~Rs 1,500/month NAT Gateway charge — Azure now auto-provisions one for any new Databricks workspace regardless of VNet config, so it wasn't avoidable in place. All data was synthetic/regeneratable; see `databricks.yml`'s commented-out `azure` target to redeploy if ever needed again. |
| Synapse gold-layer views (`sql/synapse_serving_views.sql`) | **Static snapshot, not refreshing** | The 11 gold Delta views still resolve and return correct *historical* data (verified: 440-row `JOIN` across `gold_safety_signal_summary` + `dim_product`), but nothing writes to `pharma-gold` anymore since the `azure` target's decommission. Synapse's raw-layer views (`batch_master`, `clinical_registry_raw`) are unaffected and stay fully live. |
| CI/CD (Azure DevOps) | **Pipeline created, not yet fully connected** | `pharma-lakehouse-cicd` pipeline exists in the `VivekTiwari_Project1` Azure DevOps project, reading `devops/azure-pipelines.yml` from a mirror of this repo. Needs a service connection + a mirror push of the latest YAML before it can actually run — see [docs/RUNBOOK.md](docs/RUNBOOK.md). |

Six real issues were found and fixed by actually running this end to end
rather than stopping at "looks right" — see
[docs/architecture.md § Discoveries from testing this for real](docs/architecture.md#discoveries-from-testing-this-for-real)
for all of them (a missing SQL login, a missing bulk-operations grant, a
missing credential-reference grant, a database master key prerequisite, a
Delta protocol feature Synapse can't read, and an ADF output-filename
wildcard mismatch). None of these show up until you actually run the
pipeline against live infrastructure.

## Domain

Synthetic (non-PII, GxP-flavored) data across 7 entities: clinical trial
sites, drug products, distribution centers, drug batches (manufacturing/QC),
shipments, adverse events (de-identified), and inventory snapshots. See
[docs/data_dictionary.md](docs/data_dictionary.md).

## Re-running / redeploying

### Databricks (`medallion`, $0 — primary target)

```powershell
databricks bundle deploy --profile medallion
databricks bundle run pharma_lakehouse_job --profile medallion
```

**If you change the `catalog` variable on an existing target** (rather than
adding a new one), delete the old Lakeflow pipeline object first — it
carries internal state tied to its original catalog and `databricks bundle
deploy` updating it in place fails with `PERMISSION_DENIED: Can not move
tables across arclight catalogs`. See
[docs/RUNBOOK.md](docs/RUNBOOK.md#trigger-and-verify-databricks-pipeline)
for the exact recovery steps — hit exactly this moving `dev` from the
`workspace` catalog to `pharmalake_dbx`.

The `azure` target (real Azure Databricks) is commented out in
`databricks.yml` — decommissioned, see the status table above.

### ADF

```bash
pip install -r requirements.txt
python adf/deploy_adf_pipeline.py
```
See [adf/README.md](adf/README.md) for details, including the real issues
found by testing against the live factory.

### Power BI

Follow [powerbi/data_model.md](powerbi/data_model.md) — DirectQuery to
`medallion`'s Serverless Starter Warehouse, catalog `pharmalake_dbx`.

### CI/CD

[devops/azure-pipelines.yml](devops/azure-pipelines.yml) is connected to a
real Azure DevOps pipeline (`pharma-lakehouse-cicd` in the
`VivekTiwari_Project1` project, reading from a mirror of this repo at
`pharma-lakehouse-mirror`) — see [docs/RUNBOOK.md](docs/RUNBOOK.md) for what
still needs to happen before it can actually run (a service connection and
a mirror push of the latest pipeline YAML).

## Local development

```powershell
pip install -r requirements.txt
pytest tests/ -v          # 18 tests, all passing

python data_gen/generate_pharma_data.py --out-dir ./sample_data --seed 42
```

## JD coverage map

| JD responsibility | Where it lives |
|---|---|
| Data pipeline development | [transforms/](transforms/) (Lakeflow bronze/silver/gold) — **live** on `medallion` ($0), ran successfully on real Azure Databricks before its cost-driven decommission |
| Data integration (APIs, DBs, external datasets) | [adf/](adf/) — Synapse serverless SQL + ClinicalTrials.gov public API — **both activities succeed live** |
| Data modeling | [transforms/gold.py](transforms/gold.py) star schema; [docs/data_dictionary.md](docs/data_dictionary.md) |
| Database management | Reused existing Synapse/storage (see architecture doc for why no new Azure SQL DB) |
| Data quality | [dq/data_quality_checks.py](dq/data_quality_checks.py) — **25/25 checks passing live** |
| Automation | [databricks.yml](databricks.yml) job orchestration (5 tasks incl. gold export); [adf/](adf/) daily trigger |
| Documentation | [docs/](docs/) (architecture, data dictionary, backlog, GxP approach) |
| Agile/SDLC, user stories | [docs/backlog.md](docs/backlog.md) |
| Code review / testing | [tests/](tests/) — 18 pytest cases, all passing + [devops/azure-pipelines.yml](devops/azure-pipelines.yml) `test` stage |
| BI visualization (Power BI) | [powerbi/data_model.md](powerbi/data_model.md) — DirectQuery to Databricks SQL |
| DevOps (Azure DevOps) | [devops/azure-pipelines.yml](devops/azure-pipelines.yml) |
| GxP / Non-GxP SDLC | [docs/gxp_validation_approach.md](docs/gxp_validation_approach.md) |
| Data governance & security | [governance/apply_governance.py](governance/apply_governance.py) — **applied live** (masks, row filter, tags) |
| Cloud IaC | [infra/main.bicep](infra/main.bicep) + [infra/rbac.bicep](infra/rbac.bicep) — **deployed**, RBAC kept separate by design |
| Cost management | Budget alert active, $25/month — see [docs/architecture.md](docs/architecture.md#cost-real-numbers-not-just-estimates) |

Not covered (out of JD's *required* scope, noted for completeness): Power
Apps/Power Platform, Bitbucket/JIRA/Confluence tool usage itself (the
artifacts they'd hold are in `docs/`), formal GxP validation execution (see
caveats in [docs/gxp_validation_approach.md](docs/gxp_validation_approach.md)).
