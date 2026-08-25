# ADF orchestration

Azure Data Factory runs as an **independent** integration/orchestration
demo inside the existing `adf-c360-legacy` factory — it does not trigger the
Databricks pipeline. The two subsystems are deliberately decoupled; see
[docs/architecture.md](../docs/architecture.md#why-two-decoupled-subsystems-not-one-pipeline)
for why (Unity Catalog storage credentials are cloud-bound, so an AWS-hosted
Databricks workspace can't be handed files the same way an Azure Databricks
workspace could).

```
   erp_seed/*.csv         ┌───────────────────────┐
   (seed extract) ──────► │ dbo.batch_master_source│  Synapse serverless SQL
                          │ (external table, T-SQL)│  (synapse-c360-legacy)
                          └───────────┬────────────┘
                                      │ managed-identity SQL query
                                      ▼
                          ┌─────────────────────────┐
                          │ copy_batch_master_sql_   │
                          │ to_adls (Copy Activity)  │
                          └───────────┬─────────────┘
                                      │ managed-identity write
                                      ▼
   ClinicalTrials.gov API   ┌────────────────────┐   pharma-raw/
   (public, no auth) ─────► │ copy_clinical_      │──► drug_batches_from_erp/
                             │ registry_to_adls    │    clinical_registry/
                             └────────────────────┘   (ADLS Gen2: stc360legacyws)
```

Pipeline: `pl_pharma_orchestrate`, in `adf-c360-legacy`. Daily schedule
trigger `tr_daily_schedule` created but **not started** (start only after a
manual run succeeds).

## Why a database *and* an API source, and why the seed file

The JD explicitly calls out integrating "APIs, databases, and external
datasets." Synapse serverless SQL has no persisted storage engine of its
own (no `CREATE TABLE` + `INSERT` — only external tables over files), so
there's no way to hold a genuinely writable OLTP source without provisioning
a new database, which the zero-new-resource constraint here rules out. The
honest zero-cost stand-in: `erp_seed/batch_master_seed.csv` (a one-time
upload, already done) exposed as `dbo.batch_master_source`, so ADF's copy
activity still executes a real T-SQL query against a real SQL engine —
identical activity shape to querying Azure SQL DB — rather than just another
file-to-file copy. The ClinicalTrials.gov v2 API
(`https://clinicaltrials.gov/api/v2/studies`, public, unauthenticated) is
the external-API source, called directly, no seeding needed.

## Auth: managed identity only, no secrets in this repo

- **ADF -> ADLS Gen2**: the ADLS linked service has no key/SAS configured, so
  ADF falls back to its system-assigned managed identity. That identity
  needs the **Storage Blob Data Contributor** role on `stc360legacyws` --
  granted by `infra/rbac.bicep`. **Applied and verified** --
  `copy_clinical_registry_to_adls` writes successfully.
- **ADF -> Synapse serverless SQL**: `AzureSqlDatabaseLinkedService` with
  `authentication_type="SystemAssignedManagedIdentity"` -- no SQL login or
  password anywhere. (Note: this must be a dedicated typed field, not
  embedded in the connection string as `Authentication=...` -- ADF's copy
  engine uses an older SQL client that rejects that keyword outright.)
  Storage RBAC alone wasn't sufficient here, though -- getting
  `copy_batch_master_sql_to_adls` to actually succeed took **three more
  grants**, each discovered by running it and reading the next error:
  1. `CREATE USER [adf-c360-legacy] FROM EXTERNAL PROVIDER` -- storage RBAC
     doesn't grant a SQL login; without it, `Login failed for user
     '<token-identified principal>'`.
  2. `GRANT ADMINISTER DATABASE BULK OPERATIONS` -- ADF's Copy Activity uses
     bulk-load semantics under the hood regardless of query shape;
     `db_datareader` alone gets `You do not have permission to use the bulk
     load statement.`
  3. `GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::cred_adls` -- querying
     `dbo.batch_master_source` (an `OPENROWSET` view) doesn't inherit
     permission on the credential the view's data source depends on;
     without it, `Cannot find the CREDENTIAL 'cred_adls' ... or you do not
     have permission`.

  All three are in `sql/synapse_serving_views.sql`, applied and verified --
  `copy_batch_master_sql_to_adls` now copies all 500 rows successfully.

## Deploy

```bash
pip install -r ../requirements.txt
python deploy_adf_pipeline.py
```

Defaults to `--resource-group rg-customer360-legacy --factory-name
adf-c360-legacy`; override if pointing at a different environment. Creates/
updates: 3 linked services (ADLS Gen2, Synapse serverless SQL, REST API), 4
datasets, the `pl_pharma_orchestrate` pipeline (2 independent copy
activities), and the daily trigger (stopped).

## Manual test run

```bash
az datafactory pipeline create-run -g rg-customer360-legacy --factory-name adf-c360-legacy --name pl_pharma_orchestrate
```

Both activities were tested for real against the live factory, repeatedly,
as each prerequisite got resolved -- **both succeed now**:
- `copy_clinical_registry_to_adls`: writes the ClinicalTrials.gov API
  response to `pharma-raw/clinical_registry/`.
- `copy_batch_master_sql_to_adls`: copies all 500 rows from
  `dbo.batch_master_source` (Synapse serverless SQL) to
  `pharma-raw/drug_batches_from_erp/`. Along the way this one also hit
  `SqlFailedToConnect` (Synapse serverless cold-starting -- "SQL pool is
  warming up, please try again", clears on retry) before the three SQL
  grants above were even in play.

`sql/synapse_serving_views.sql`'s own `dbo.batch_master` view (over ADF's
*output*, independent of the pipeline that produced it) needed one more fix
after that: its `OPENROWSET BULK` wildcard was `drug_batches_from_erp/*.csv`,
but ADF's Copy Activity writes an auto-generated filename with **no
extension** when the sink dataset only specifies a folder path -- the
wildcard silently matched zero files (0 rows, no error) even though ADF's
own run diagnostics showed `rowsCopied: 500`. Fixed to `*` (no extension
filter).

## SDK quirk worth knowing about

`azure-mgmt-datafactory==10.0.0` doesn't reliably auto-populate the `type`
discriminator field from constructor kwargs for polymorphic models
(`LinkedService`, `Dataset`, `CopyActivity`, `Trigger` subclasses, and the
`*Reference` types) -- payloads get rejected with `"the 'type' nested in
payload is null"`, or worse, silently serialize as the wrong type (e.g. a
`ScheduleTrigger` silently became `MultiplePipelineTrigger`, its own parent
class, and `CopyActivity` silently became `Execution`, its parent). Every
model construction in `deploy_adf_pipeline.py` either passes `type=` explicitly
in the constructor (works for single-level discriminators) or sets `.type =
"..."` via attribute assignment *after* construction (required for
multi-level discriminator hierarchies, where the constructor kwarg gets
overwritten). Discovered by deploying against the real API and reading the
`BadRequest`/wrong-type responses -- not documented anywhere obvious.
