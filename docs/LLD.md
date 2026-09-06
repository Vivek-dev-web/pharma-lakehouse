# Low-Level Design (LLD)

Module-by-module detail — exact schemas, configurations, and logic. For
system-level context read [HLD.md](HLD.md) first. Full column-level
reference lives in [data_dictionary.md](data_dictionary.md); this document
focuses on *behavior* (what each module does and how), not a full schema
restatement.

## 1. Data generation — `data_gen/generate_pharma_data.py`, `transforms/00_generate_sample_data.py`

**Purpose:** produce fully synthetic, non-PII source data for 7 entities.

**Volumes generated** (seed 42, default CLI args):

| Entity | Rows | Generator function |
|---|---|---|
| `clinical_trial_sites` | 60 | `gen_sites` |
| `drug_products` | 12 | `gen_products` |
| `distribution_centers` | 8 | `gen_distribution_centers` |
| `drug_batches` | 500 | `gen_batches` (QC status weighted 92% Released / 5% Quarantined / 3% Rejected) |
| `shipments` | 4,000 | `gen_shipments` (drawn only from *Released* batches; 4% temperature-excursion rate) |
| `adverse_events` | 1,200 | `gen_adverse_events` (severity weighted 55/30/12/3% Mild/Moderate/Severe/SAE) |
| `inventory_snapshots` | 3,600 | `gen_inventory_snapshots` (90 days × 8 DCs × 5 sampled products) |

**Execution path:** `transforms/00_generate_sample_data.py` (a Databricks
notebook task) imports the generator functions and writes each entity
straight to `/Volumes/{catalog}/{schema}/raw_landing/{entity}/{entity}.csv`
via plain Python `open()` — **not** `dbutils.fs.cp` from a local `/tmp`
staging path, which serverless compute blocks
(`LocalFilesystemAccessDeniedException`; see
[architecture.md §1](architecture.md#discoveries-from-testing-this-for-real)).

## 2. Bronze — `transforms/bronze.py`

**Pattern:** one `@dlt.table` per entity, generated programmatically via a
`make_bronze_table(entity)` factory over the `ENTITIES` list (7 entities).
Each table:
- Reads via Auto Loader (`cloudFiles`, CSV format, schema location under
  `{raw_root}/_schemas/{entity}`) as a streaming source.
- Adds `_ingested_ts` (`current_timestamp()`) and `_source_file`
  (`_metadata.file_path`).
- No filtering, no joins, no business logic — a raw, schema-on-read copy.

**Configuration:** `raw_root` is read from the pipeline's `configuration`
block (`spark.conf.get("raw_root")`), set per-target in `databricks.yml`.

## 3. Silver — `transforms/silver.py`

**Pattern:** one table per entity, each reading its bronze counterpart via
`dlt.read_stream`, adding two audit columns, deduplicating on primary key,
and (for 4 of the 7) enforcing Lakeflow expectations.

| Table | Audit `_source_system` | Dedup key | Expectations |
|---|---|---|---|
| `silver_sites` | CTMS | `site_id` | none |
| `silver_products` | ERP | `product_id` | none |
| `silver_distribution_centers` | WMS | `dc_id` | none |
| `silver_batches` | MES | `batch_id` | `expect_or_drop`: `batch_id IS NOT NULL`; `expect_or_drop`: `qc_status IN ('Released','Quarantined','Rejected')`; `expect` (flag only): `expiry_date > manufacture_date` |
| `silver_shipments` | WMS | `shipment_id` | `expect_or_drop`: ids not null; `expect_or_drop`: `quantity_units > 0`; `expect` (flag only): `received_date >= ship_date` |
| `silver_adverse_events` | SAFETY_DB | `event_id` | `expect_or_drop`: `event_id IS NOT NULL`; `expect_or_drop`: valid severity enum |
| `silver_inventory_snapshots` | WMS | `snapshot_date, dc_id, product_id` | `expect_or_drop`: `on_hand_qty >= 0` |

`_valid_from` is set to the bronze row's `_ingested_ts` on every table (load
timestamp, not a business date).

## 4. Gold — `transforms/gold.py`

**Star schema**, 11 tables, all Unity-Catalog-managed (Lakeflow rejects an
explicit `path=` on a UC-governed `dlt.table` — see
[architecture.md §2](architecture.md#discoveries-from-testing-this-for-real)):

- **Dimensions (4):** `dim_date` (generated calendar 2023-01-01 →
  2026-12-31, `date_key` = `yyyyMMdd` int), `dim_site`, `dim_product`,
  `dim_distribution_center`.
- **Facts (4):** `fact_batch_release` (grain: 1/batch; derives
  `is_released`, `is_rejected`, `days_to_release`), `fact_shipment` (grain:
  1/shipment; derives `transit_days`, `ship_date_key`), `fact_adverse_event`
  (grain: 1/event; derives `report_date_key`, `is_serious`),
  `fact_inventory_snapshot` (grain: 1/DC/product/day; derives
  `is_below_reorder`).
- **Aggregates (3):** `gold_batch_quality_summary` (product × month:
  release rate, avg days-to-release), `gold_supply_chain_kpis` (DC × month:
  avg transit days, excursion rate), `gold_safety_signal_summary` (product ×
  month, joined to `fact_batch_release` for product attribution: event
  count, SAE count/rate).

**`gold_table()` helper:** as of the current state, this is a thin wrapper
returning a plain `dlt.table(comment=...)` — a leftover interface from a
since-reverted `path=`-injection attempt, kept because the call sites
(`@gold_table("dim_site", comment=...)`) are harmless and self-documenting
(the `name` argument isn't otherwise used).

## 5. Data quality — `dq/data_quality_checks.py`

Runs as a plain notebook task (not inside Lakeflow) after the pipeline
completes. **Exactly 25 checks**, each recorded as a row in `dq_results`
(`check_name`, `table_name`, `passed`, `detail`, `run_ts`):

| Category | Count | Detail |
|---|---|---|
| Completeness | 11 | Non-null checks: 4 cols on `fact_shipment`, 4 on `fact_adverse_event`, 3 on `fact_batch_release` |
| Uniqueness | 6 | PK-uniqueness on `fact_shipment`, `fact_adverse_event`, `fact_batch_release`, `dim_site`, `dim_product`, `dim_distribution_center` |
| Referential integrity | 5 | `fact_shipment.dc_id → dim_distribution_center`, `fact_shipment.site_id → dim_site`, `fact_shipment.batch_id → fact_batch_release`, `fact_adverse_event.site_id → dim_site`, `fact_adverse_event.batch_id → fact_batch_release` (all via `left_anti` join, orphan count must be 0) |
| Freshness | 3 | `bronze_shipments`, `bronze_adverse_events`, `bronze_inventory_snapshots` each must have a non-null max `_ingested_ts` |

**Failure behavior:** results are always written (`append` mode,
`mergeSchema: true`); if any check's `passed` is `false`, the notebook
raises an exception after writing, which fails the Databricks job task and
blocks `apply_governance` from running (`depends_on` in `databricks.yml`).

## 6. Governance — `governance/apply_governance.py`

Three Unity Catalog mechanisms, each gated on
`is_account_group_member('admins')` (workspace owner always sees unmasked
data — a non-admin group/user is needed to observe the restriction):

1. **Column mask** — `mask_pi_name(pi STRING)` function, applied to
   `dim_site.principal_investigator`: returns the real value for admins,
   `'REDACTED'` otherwise.
2. **Row filter** — `restrict_sae_rows(severity STRING)` function, applied
   to `fact_adverse_event`: hides rows where `severity = 'SAE'` from
   non-admins.
3. **Classification tags** — 4 tag assignments: `dim_site.principal_investigator`
   (`sensitivity=confidential`), `fact_adverse_event.severity`
   (`sensitivity=safety_signal`), `fact_adverse_event.patient_age_band`
   (`sensitivity=deidentified_phi_adjacent`), `fact_batch_release.qc_status`
   (`domain=gxp_quality`).

Every statement is wrapped in try/except and logs `[OK]`/`[SKIPPED]` rather
than failing the job — some object types (e.g. materialized views) silently
reject masks/row filters, a known Lakeflow/UC interaction documented inline
in the script.

## 7. Gold export — `transforms/09_export_gold_to_adls.py`

Only executes meaningfully when the `gold_export_root` job parameter is
non-empty (`dev` target leaves it unset → no-op print and exit). For each
of the 11 gold tables:

1. `dbutils.fs.rm(target_path, recurse=True)` — wipes any prior run's
   transaction log so a stale protocol feature can't linger.
2. `df.write.format("delta").mode("overwrite").option("overwriteSchema",
   "true").option("delta.enableDeletionVectors", "false").save(target_path)`
   — the `enableDeletionVectors: false` option is load-bearing: Databricks
   enables it by default (protocol reader v3/writer v7), which Synapse
   serverless SQL's Delta reader cannot read at all (see
   [architecture.md §7](architecture.md#discoveries-from-testing-this-for-real)).
3. `DROP TABLE IF EXISTS` + `CREATE TABLE ... USING DELTA LOCATION` against
   a dedicated `{catalog}.{gold_schema}` schema (default
   `pharma_lakehouse_gold`) — registers a real, named-path **external**
   table, distinct from the UC-managed original.

**Known limitation:** the exported copies do not inherit the governance
rules from §6 — they're a separate physical table. Documented, not hidden;
see [architecture.md § Decision 3](DESIGN.md#decision-3-gold-tables-stay-uc-managed-a-separate-export-step-lands-named-path-copies).

## 8. Databricks Asset Bundle — `databricks.yml`

**Variables:** `catalog` (default `workspace`), `schema` (default
`pharma_lakehouse`), `gold_export_root` (default `""`), `gold_schema`
(default `pharma_lakehouse_gold`).

**Pipeline resource** `pharma_pipeline`: serverless, Photon-enabled,
`configuration.raw_root` set from `catalog`/`schema` variables, libraries =
`bronze.py`, `silver.py`, `gold.py`.

**Job resource** `pharma_lakehouse_job`: 5 tasks in a linear DAG —
`generate_sample_data` → `run_pipeline` (pipeline task, references
`pharma_pipeline`) → `run_data_quality_checks` → `apply_governance` →
`export_gold_to_adls`. Job parameters (`catalog`, `schema`,
`gold_export_root`, `gold_schema`) are threaded through as
`base_parameters` on every notebook task.

**Targets:**
- `dev` (default, `mode: development`): host
  `https://dbc-fbe3df8d-7ffc.cloud.databricks.com` (Databricks Free Edition,
  AWS). `gold_export_root` left unset.
- `azure`: **commented out** as of 2026-08-29 (workspace deleted — see
  [HLD.md §7](HLD.md#7-deployment-topology-and-current-state)). When it
  existed: host `https://adb-7405613897559086.6.azuredatabricks.net`,
  `catalog: pharmalake_dbx`, `gold_export_root:
  abfss://pharma-gold@stc360legacyws.dfs.core.windows.net`.

## 9. ADF — `adf/deploy_adf_pipeline.py`

Deploys into the **existing** `adf-c360-legacy` factory
(`rg-customer360-legacy`) via `azure-mgmt-datafactory` (Python SDK,
`DefaultAzureCredential`).

**Linked services (3):**
| Name | Type | Auth |
|---|---|---|
| `ls_adls_pharma` | `AzureBlobFS` | System-assigned managed identity (no key/SAS set) |
| `ls_synapse_serverless_sql` | `AzureSqlDatabase` (pointed at the `-ondemand` serverless endpoint) | `authenticationType: SystemAssignedManagedIdentity` |
| `ls_clinical_trials_api` | `RestService` | Anonymous |

**Datasets (4):** `ds_batch_master_source` (Azure SQL table
`dbo.batch_master_source`), `ds_adls_drug_batches` (delimited text,
`pharma-raw/drug_batches_from_erp`, header row), `ds_clinical_registry_api`
(REST resource), `ds_adls_clinical_registry` (JSON, `pharma-raw/clinical_registry`).

**Pipeline `pl_pharma_orchestrate`:** 2 independent Copy Activities (no
`dependsOn` between them):
1. `copy_batch_master_sql_to_adls`: `TabularSource` → `DelimitedTextSink`.
   Verified: 500 rows read/written, 1 file, ~62 KB.
2. `copy_clinical_registry_to_adls`: `RestSource` (GET) → `JsonSink`.

**Trigger `tr_daily_schedule`:** daily `ScheduleTrigger`, created but never
started — confirmed `runtimeState: Stopped`; nothing in this project runs
on a schedule.

**SDK note:** `azure-mgmt-datafactory==10.0.0` does not reliably
auto-populate the `type` discriminator on polymorphic models from
constructor kwargs — every model in this script either passes `type=`
explicitly (single-level discriminators) or sets `.type = "..."` via
attribute assignment post-construction (multi-level hierarchies, where the
constructor kwarg gets silently overwritten). Full detail:
[adf/README.md § SDK quirk](../adf/README.md#sdk-quirk-worth-knowing-about).

## 10. Synapse serving layer — `sql/synapse_serving_views.sql`

Executed against `synapse-c360-legacy-ondemand.sql.azuresynapse.net`,
database `pharma_lakehouse_raw`. In dependency order:

1. `CREATE USER [adf-c360-legacy] FROM EXTERNAL PROVIDER` + `ALTER ROLE
   db_datareader ADD MEMBER` — SQL login for ADF (storage RBAC alone
   authenticates but doesn't authorize a database session).
2. `GRANT ADMINISTER DATABASE BULK OPERATIONS TO [adf-c360-legacy]` — ADF's
   Copy Activity uses bulk-load semantics regardless of query shape.
3. `CREATE MASTER KEY ENCRYPTION BY PASSWORD = '...'` — prerequisite for
   any scoped credential, including Managed-Identity ones with no secret to
   protect. (Committed file has a placeholder; a real password is generated
   at execution time and never persisted.)
4. `CREATE DATABASE SCOPED CREDENTIAL cred_adls WITH IDENTITY = 'Managed
   Identity'` — authenticates as the **Synapse workspace's own** managed
   identity, not the querying user's (see `infra/rbac.bicep`'s third role
   assignment).
5. `GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::cred_adls TO
   [adf-c360-legacy]` — querying a view doesn't inherit permission on the
   credential its data source depends on.
6. Two `EXTERNAL DATA SOURCE`s (`ds_pharma_raw`, `ds_pharma_gold`) and one
   `EXTERNAL FILE FORMAT` (`ff_csv`).
7. **3 raw-layer views:** `dbo.batch_master_source` (seed CSV, the "ERP
   source" ADF reads from), `dbo.batch_master` (ADF's *output* —
   `BULK 'drug_batches_from_erp/*'`, not `*.csv`: ADF writes an
   extensionless filename), `dbo.clinical_registry_raw`.
8. **11 gold-layer views**, one per exported table, each a one-line
   `OPENROWSET(BULK '<table_name>', DATA_SOURCE = 'ds_pharma_gold', FORMAT
   = 'DELTA')`. Static since the `azure` Databricks target's decommission —
   see [HLD.md §7](HLD.md#7-deployment-topology-and-current-state).

## 11. Infrastructure as code — `infra/main.bicep`, `infra/rbac.bicep`

**`main.bicep`** (provisioning, no IAM): 2 blob containers (`pharma-raw`,
`pharma-gold`) on the existing `stc360legacyws` storage account; an Azure
Databricks Premium workspace (`pharmalake-dbx`); a Databricks Access
Connector (`pharmalake-uc-access-connector`, system-assigned identity — the
identity Unity Catalog's storage credential authenticates as); a
`Microsoft.Consumption/budgets` resource (`pharmalake-monthly-budget`,
$25/month, alerts at 50/80/100% to `vivekt94@gmail.com`).

**Important:** this template's Databricks workspace resource was **not**
updated after the `azure` target's decommission — only the deployed
instance was removed (`az databricks workspace delete`) and only the
bundle target commented out in `databricks.yml`. Re-running `az deployment
group create -g rg-customer360-legacy --template-file main.bicep` today
would **recreate `pharmalake-dbx`**, including the NAT Gateway charge that
caused the decommission in the first place (see
[HLD.md §7](HLD.md#7-deployment-topology-and-current-state)). If the intent
is to keep the workspace decommissioned, remove the `databricksWorkspace`
resource block from this file before it's redeployed for any other reason
(e.g. re-running it just to add a future container or budget change).

**`rbac.bicep`** (IAM, deployed separately by design — see
[architecture.md § Why the RBAC role assignments are a separate
file](architecture.md#why-the-rbac-role-assignments-are-a-separate-file)):
3 `Storage Blob Data Contributor` role assignments on `stc360legacyws` —
`adf-c360-legacy`'s identity, `pharmalake-uc-access-connector`'s identity,
and `synapse-c360-legacy`'s identity (the last one only became apparent as
a gap once `CREATE DATABASE SCOPED CREDENTIAL ... WITH IDENTITY = 'Managed
Identity'` turned out to authenticate as the *workspace's* identity, not
the querying user's).

## 12. CI/CD pipeline definition — `devops/azure-pipelines.yml`

**Defined, not connected to a live Azure DevOps org/project** — see the
conversation history / project status for why. Five stages: `test` (pytest,
no cloud needed) → `validate` (`databricks bundle validate` +
`az bicep build`) → `deploy_infra` (approval-gated — the only stage
touching IAM) → `deploy_lakehouse` (bundle deploy + run) → `deploy_adf`
(runs `adf/deploy_adf_pipeline.py`). Variable group `pharma-lakehouse-dev`
expected: `AZURE_SERVICE_CONNECTION`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
(secret), `RESOURCE_GROUP`.

## 13. Testing — `tests/test_business_logic.py`

18 pytest cases against `src/business_logic.py` — pure-Python
reimplementations of the gold-layer business rules (`days_to_release`,
`transit_days`, `is_below_reorder`, `is_serious_event`, `safe_rate_pct` and
its 3 specializations), covering edge cases: zero-denominator rates,
same-day transit, boundary reorder points, not-yet-released batches. Run
with `pytest tests/ -v` — no Spark/cloud dependency.
