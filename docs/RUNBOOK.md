# Runbook

Day-2 operations: how to trigger each component, confirm it worked, recover
from a failure, and keep an eye on cost. For *why* the system is built this
way, see [architecture.md](architecture.md); for *how data moves*, see
[DATA_FLOW.md](DATA_FLOW.md). This doc is the "I need to run/check/fix
something right now" reference.

## Quick reference

| Component | Where | Trigger command |
|---|---|---|
| Databricks pipeline (Azure) | `pharmalake-dbx` workspace | `databricks bundle run pharma_lakehouse_job --profile medallion-azure --target azure` |
| Databricks pipeline ($0 reference) | `medallion` (AWS Free Edition) | `databricks bundle run pharma_lakehouse_job --profile medallion` |
| ADF pipeline | `adf-c360-legacy` | `az datafactory pipeline create-run -g rg-customer360-legacy --factory-name adf-c360-legacy --name pl_pharma_orchestrate` |
| Synapse serving layer | `synapse-c360-legacy` | Not a "run" — DDL in `sql/synapse_serving_views.sql` is applied once; the views themselves resolve live on every query |

## Trigger and verify: Databricks pipeline

```bash
databricks bundle run pharma_lakehouse_job --profile medallion-azure --target azure
```

Prints a **Run URL** — open it to watch the 5-task job graph live. Expect
~5 minutes end to end. Task order: `generate_sample_data` → `run_pipeline`
(Lakeflow bronze/silver/gold) → `run_data_quality_checks` → `apply_governance`
→ `export_gold_to_adls`.

**Verify success:**
```bash
databricks api post /api/2.0/sql/statements --profile medallion-azure --json '{
  "warehouse_id": "5f9d8e808b188ac0",
  "statement": "SELECT (SELECT COUNT(*) FROM pharmalake_dbx.pharma_lakehouse.dq_results WHERE passed = false) AS failed_checks, (SELECT COUNT(*) FROM pharmalake_dbx.pharma_lakehouse.fact_shipment) AS shipments",
  "wait_timeout": "30s"
}'
```
`failed_checks` should be `0`. If the warehouse is stopped, this call
auto-starts it (takes a few seconds) — no separate start step needed.

**If a task fails:** open the Run URL, click the failed task, read the
notebook/pipeline output directly — it's almost always one of the causes in
[Common failure modes](#common-failure-modes) below.

## Trigger and verify: ADF pipeline

```bash
RUN_ID=$(az datafactory pipeline create-run -g rg-customer360-legacy --factory-name adf-c360-legacy --name pl_pharma_orchestrate --query runId -o tsv)
az datafactory pipeline-run show -g rg-customer360-legacy --factory-name adf-c360-legacy --run-id "$RUN_ID"
```

Two independent Copy Activities, ~15–25 seconds each. Check status via the
Azure Portal (Data Factory Studio → Monitor → Pipeline runs) or the CLI
command above (`status` field: `Succeeded` / `Failed` / `InProgress`).

**Verify success:** row counts are in the activity's own diagnostics —
```bash
az rest --method post --url "https://management.azure.com/subscriptions/<sub-id>/resourceGroups/rg-customer360-legacy/providers/Microsoft.DataFactory/factories/adf-c360-legacy/pipelineruns/$RUN_ID/queryActivityruns?api-version=2018-06-01" \
  --body '{"lastUpdatedAfter":"2026-01-01T00:00:00Z","lastUpdatedBefore":"2030-01-01T00:00:00Z"}' \
  --query "value[].{name:activityName, status:status, rowsCopied:output.rowsCopied}"
```
Expect `copy_batch_master_sql_to_adls`: 500 rows; `copy_clinical_registry_to_adls`:
a small number of records (however many the public API returned that call).

## Common failure modes

Ordered roughly by how early in a fresh setup you'd hit them. Full technical
explanation for each is in
[architecture.md § Discoveries from testing this for real](architecture.md#discoveries-from-testing-this-for-real).

| Symptom | Cause | Fix |
|---|---|---|
| `LocalFilesystemAccessDeniedException` | Serverless compute blocks local driver FS access | Already fixed in `transforms/00_generate_sample_data.py` — only relevant if you reintroduce a `/tmp`-staging pattern |
| `Cannot specify an explicit path for a table when using Unity Catalog` | Tried adding `path=` to a `dlt.table` | Don't — write named-path tables outside Lakeflow (see `export_gold_to_adls.py`) |
| ADF: `Login failed for user '<token-identified principal>'` | Storage RBAC granted, but no SQL login in the Synapse database | Re-run `sql/synapse_serving_views.sql`'s `CREATE USER ... FROM EXTERNAL PROVIDER` block |
| ADF: `You do not have permission to use the bulk load statement` | SQL login exists but lacks bulk-operations rights | `GRANT ADMINISTER DATABASE BULK OPERATIONS TO [adf-c360-legacy];` |
| ADF: `Cannot find the CREDENTIAL 'cred_adls' ... or you do not have permission` | Querying principal lacks `REFERENCES` on the credential | `GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::cred_adls TO [adf-c360-legacy];` |
| Synapse: `Content of directory on path '.../_delta_log/*.*' cannot be listed` | Either a real RBAC gap, **or** Databricks wrote the table with deletion vectors enabled | Check RBAC first (`az rest ... roleAssignments`); if that's fine, confirm via `DESCRIBE DETAIL` whether `tableFeatures` includes `deletionVectors` — if so, rerun the export with `.option("delta.enableDeletionVectors", "false")` (already the default in this repo's `export_gold_to_adls.py`) |
| Synapse view returns 0 rows, no error | `OPENROWSET BULK` wildcard doesn't match the actual filename (e.g. `*.csv` when ADF wrote no extension) | Widen the wildcard to `*`; compare against the producing pipeline's own row-count diagnostics to confirm data really landed |
| `SqlFailedToConnect` / `SQL pool is warming up, please try again` | Synapse serverless cold start after idle | Harmless — retry once |
| Databricks SQL query is slow to start | Warehouse was `STOPPED` (auto-stop after 5–10 min idle) | Expected — first query auto-starts it, takes a few seconds |

## Cost checks

```bash
# Any SQL warehouse left RUNNING?
databricks warehouses list --profile medallion-azure
databricks warehouses list --profile medallion

# Stop one if needed
databricks warehouses stop <id> --profile medallion-azure
```

Both warehouses have auto-stop configured (5 min on `pharmalake-dbx`, 10 min
on `medallion`) — a stray run self-corrects. The $25/month budget alert on
`rg-customer360-legacy` emails `vivekt94@gmail.com` at 50/80/100% regardless;
check `az consumption budget list -g rg-customer360-legacy` if you want the
current threshold state without waiting for an email.

Nothing else in this project has an idle cost — see
[architecture.md § Cost](architecture.md#cost-real-numbers-not-just-estimates)
for the full breakdown.

## Redeploying after a code change

```bash
# Databricks
databricks bundle deploy --profile medallion-azure --target azure
databricks bundle run pharma_lakehouse_job --profile medallion-azure --target azure

# ADF
python adf/deploy_adf_pipeline.py

# Synapse (only if sql/synapse_serving_views.sql changed)
# -- run interactively via Synapse Studio, or non-interactively with an
#    Azure AD access token (az account get-access-token --resource
#    https://database.windows.net/) passed to pyodbc -- see the script's
#    own header comment for the exact connection pattern.
```

## Tearing down

Not done as part of this project (kept provisioned-but-idle, $0 while
unused — see the cost section above). If you want to fully remove the new
resources this project added:

```bash
# Databricks workspace + access connector (irreversible -- deletes all
# pipelines, notebooks, and Unity Catalog objects created here)
az databricks workspace delete -g rg-customer360-legacy -n pharmalake-dbx
az resource delete --ids /subscriptions/<sub-id>/resourceGroups/rg-customer360-legacy/providers/Microsoft.Databricks/accessConnectors/pharmalake-uc-access-connector

# Blob containers (irreversible -- deletes the landed/exported data)
az storage container delete --account-name stc360legacyws --name pharma-raw --auth-mode login
az storage container delete --account-name stc360legacyws --name pharma-gold --auth-mode login

# ADF pipeline objects (leaves adf-c360-legacy itself untouched)
az datafactory pipeline delete -g rg-customer360-legacy --factory-name adf-c360-legacy --name pl_pharma_orchestrate
```

The RBAC grants (`infra/rbac.bicep`) and Synapse database/logins
(`sql/synapse_serving_views.sql`) become moot once their subjects are
deleted and don't need separate cleanup.
