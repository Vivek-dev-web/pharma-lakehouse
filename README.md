# Pharma Lakehouse

An end-to-end Azure data engineering project on a synthetic pharma
supply-chain / clinical-safety domain, built to exercise the full breadth of
an Azure Data Engineer role. Runs on **real Azure Databricks** (Premium,
Unity Catalog), with **Azure Data Factory, Synapse serverless SQL, and ADLS
Gen2** reused from an existing resource group — every claim below has
actually been deployed and run, not just designed.

- **Databricks** (`pharmalake-dbx`, Azure, Premium — `infra/main.bicep`)
  runs the full bronze → silver → gold pipeline, data quality checks, and
  Unity Catalog governance. A second target (`medallion`, AWS Free Edition)
  is kept as the $0 reference implementation this project validated first.
- **Azure Data Factory + Synapse serverless SQL + ADLS Gen2** (all
  pre-existing in `rg-customer360-legacy`) demonstrate real database + public
  API integration, using managed-identity auth only — no passwords or
  storage keys anywhere in this repo.
- **Power BI** DirectQueries Databricks SQL; **Synapse serverless SQL** also
  reads the same gold Delta tables directly via a dedicated export step —
  both serving paths verified working.

See [docs/architecture.md](docs/architecture.md) for the full diagram and
the platform constraints this build actually ran into (Unity Catalog storage
credentials being cloud-bound, Lakeflow rejecting explicit table paths under
UC governance, ADF needing a SQL login in addition to storage RBAC) — all
discovered by running this for real, not from documentation.

**New here?** [docs/DESIGN.md](docs/DESIGN.md) has the why (problem
statement, decisions and tradeoffs, cost model). [docs/DATABRICKS_GETTING_STARTED.md](docs/DATABRICKS_GETTING_STARTED.md)
is a from-zero walkthrough for deploying and running the Databricks side if
you haven't used Databricks before.

## What's actually live right now

Everything. Every component below has been deployed, run, and verified with
real data — not just designed:

| Component | Status | Evidence |
|---|---|---|
| Azure Databricks workspace (`pharmalake-dbx`) | **Provisioned & running** | `infra/main.bicep` deployed; Unity Catalog auto-enabled |
| Databricks bundle, `azure` target | **Deployed & run repeatedly** | Full job succeeds: generate → Lakeflow pipeline → DQ → governance → gold export, ~5 min end-to-end |
| Gold tables (UC-managed) | **Populated with real data** | 4,000 shipments, 1,200 adverse events, 500 batches — verified via SQL warehouse |
| Gold export (`pharma_lakehouse_gold` schema) | **11/11 tables exported, readable from Synapse** | Real `SELECT`/`JOIN` across `gold_safety_signal_summary` + `dim_product` via Synapse serverless SQL, 440 rows |
| Storage RBAC (`infra/rbac.bicep`) | **Applied** | ADF's, the access connector's, **and Synapse workspace's** managed identities all granted Storage Blob Data Contributor |
| UC storage credential + external location | **Created** | `stc360legacyws_cred` / `pharma_gold`, backed by `pharmalake-uc-access-connector` |
| Budget alert | **Active** | $25/month on `rg-customer360-legacy`, alerts to `vivekt94@gmail.com` at 50/80/100% |
| ADF pipeline (`pl_pharma_orchestrate`) | **Fully succeeds** | Both activities succeed: 500 rows copied from Synapse serverless SQL, 4 records from the ClinicalTrials.gov API |
| Synapse serving layer (`sql/synapse_serving_views.sql`) | **Applied, all views resolve** | Raw extracts (`batch_master`, `clinical_registry_raw`) and all 11 gold Delta views queryable with real row counts |
| Databricks bundle, `dev` target (AWS Free Edition) | **Still live from the first pass** | Kept as the $0 reference implementation |

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

### Databricks (Azure — primary)

```powershell
databricks bundle deploy --profile medallion-azure --target azure
databricks bundle run pharma_lakehouse_job --profile medallion-azure --target azure
```
`medallion-azure` profile uses `auth_type = azure-cli` in `~/.databrickscfg`
— no PAT needed, it rides your `az login` session.

### Databricks (AWS Free Edition — $0 reference)

```powershell
databricks bundle deploy --profile medallion
databricks bundle run pharma_lakehouse_job --profile medallion
```

### ADF

```bash
pip install -r requirements.txt
python adf/deploy_adf_pipeline.py
```
See [adf/README.md](adf/README.md) for details, including the two real
issues found by testing against the live factory.

### Power BI

Follow [powerbi/data_model.md](powerbi/data_model.md) — DirectQuery to
`pharmalake-dbx`'s Serverless Starter Warehouse.

### CI/CD

[devops/azure-pipelines.yml](devops/azure-pipelines.yml) — import into Azure
DevOps as a YAML pipeline.

## Local development

```powershell
pip install -r requirements.txt
pytest tests/ -v          # 18 tests, all passing

python data_gen/generate_pharma_data.py --out-dir ./sample_data --seed 42
```

## JD coverage map

| JD responsibility | Where it lives |
|---|---|
| Data pipeline development | [transforms/](transforms/) (Lakeflow bronze/silver/gold) — **live on real Azure Databricks** |
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
