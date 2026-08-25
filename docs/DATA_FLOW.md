# Data Flow

How data actually moves through each half of this project, step by step,
with the real row counts and file shapes seen while testing it (not
estimates). See [architecture.md](architecture.md) for the system-level
diagram and the two subsystems' relationship; this doc goes one level
deeper into each.

## ADF: source → Synapse/API → ADLS Gen2

```mermaid
flowchart TD
    SEED["erp_seed/batch_master_seed.csv<br/>(one-time upload, 500 rows)"]
    SRCVIEW["dbo.batch_master_source<br/>(Synapse serverless SQL view,<br/>OPENROWSET over the seed CSV)"]
    API["ClinicalTrials.gov v2 API<br/>GET /api/v2/studies<br/>(public, unauthenticated)"]

    SEED --> SRCVIEW

    subgraph PIPE["ADF pipeline: pl_pharma_orchestrate"]
        direction LR
        A1["copy_batch_master_sql_to_adls<br/>(Copy Activity)"]
        A2["copy_clinical_registry_to_adls<br/>(Copy Activity)"]
    end

    SRCVIEW -- "SELECT * (T-SQL,<br/>managed-identity auth)" --> A1
    API -- "GET (REST linked service)" --> A2

    A1 -- "500 rows, 1 file,<br/>~62 KB, CSV,<br/>no file extension" --> RAW1["pharma-raw/<br/>drug_batches_from_erp/"]
    A2 -- "1 file, JSON" --> RAW2["pharma-raw/<br/>clinical_registry/"]

    RAW1 --> V1["dbo.batch_master<br/>(Synapse view, OPENROWSET<br/>BULK 'drug_batches_from_erp/*')"]
    RAW2 --> V2["dbo.clinical_registry_raw<br/>(Synapse view, OPENROWSET<br/>BULK 'clinical_registry/*.json')"]
```

**Step by step:**

1. `erp_seed/batch_master_seed.csv` was uploaded once (`az storage blob
   upload`, AAD auth, no key) — a stand-in ERP extract, since Synapse
   serverless SQL has no persisted storage engine of its own to hold a real
   writable source table (see [architecture.md](architecture.md)).
2. `dbo.batch_master_source` is a Synapse view over that CSV — this is what
   ADF's Copy Activity actually queries, via T-SQL, exactly as it would
   query a real Azure SQL DB table.
3. **`copy_batch_master_sql_to_adls`** runs `SELECT *` against that view
   (`AzureSqlDatabaseLinkedService`, `authenticationType:
   SystemAssignedManagedIdentity` — no password) and writes the result to
   `pharma-raw/drug_batches_from_erp/` as a single CSV. Verified via the
   activity's own run diagnostics: **500 rows read, 500 rows written, 1
   file, ~62 KB**. The output file gets an auto-generated name with **no
   extension** (the sink dataset only specifies a folder path) — worth
   knowing if you're writing your own `OPENROWSET` against it.
4. **`copy_clinical_registry_to_adls`** calls the public ClinicalTrials.gov
   API (`RestServiceLinkedService`, anonymous auth) and writes the JSON
   response to `pharma-raw/clinical_registry/`.
5. Two Synapse views read the *landed files* back out, independent of the
   pipeline that produced them: `dbo.batch_master` (500 rows) and
   `dbo.clinical_registry_raw` (4 records, one per API result on this run).

Every hop uses managed-identity or Azure AD auth — see
[architecture.md § Discoveries from testing this for real](architecture.md#discoveries-from-testing-this-for-real)
for the four separate grants it actually took to get step 3 working
(storage RBAC, a SQL login, a bulk-operations grant, and a credential
reference grant — each one only surfaced by running the pipeline and
reading the next error).

## Databricks: synthetic source → bronze → silver → gold → serving

```mermaid
flowchart TD
    GEN["transforms/00_generate_sample_data.py<br/>(synthetic data generator)"]
    VOL["/Volumes/.../raw_landing/<br/>7 CSVs: 60 sites, 12 products, 8 DCs,<br/>500 batches, 4000 shipments,<br/>1200 adverse events, 3600 inventory rows"]

    GEN --> VOL

    subgraph BRONZE["Bronze (transforms/bronze.py) -- Auto Loader, 1:1 per source"]
        direction LR
        B1[bronze_clinical_trial_sites]
        B2[bronze_drug_products]
        B3[bronze_distribution_centers]
        B4[bronze_drug_batches]
        B5[bronze_shipments]
        B6[bronze_adverse_events]
        B7[bronze_inventory_snapshots]
    end
    VOL --> B1 & B2 & B3 & B4 & B5 & B6 & B7

    subgraph SILVER["Silver (transforms/silver.py) -- dedup + expectations + audit cols"]
        direction LR
        S1[silver_sites]
        S2[silver_products]
        S3[silver_distribution_centers]
        S4[silver_batches]
        S5[silver_shipments]
        S6[silver_adverse_events]
        S7[silver_inventory_snapshots]
    end
    B1 --> S1
    B2 --> S2
    B3 --> S3
    B4 -- "drop: bad batch_id/qc_status;<br/>flag: expiry before manufacture" --> S4
    B5 -- "drop: bad ids/qty;<br/>flag: received before shipped" --> S5
    B6 -- "drop: bad event_id/severity" --> S6
    B7 -- "drop: negative qty" --> S7

    subgraph GOLD["Gold (transforms/gold.py) -- star schema, UC-managed"]
        direction TB
        DIMS["4 dimensions:<br/>dim_date, dim_site,<br/>dim_product, dim_distribution_center"]
        FACTS["4 facts:<br/>fact_batch_release, fact_shipment,<br/>fact_adverse_event, fact_inventory_snapshot"]
        AGGS["3 aggregates:<br/>gold_batch_quality_summary,<br/>gold_supply_chain_kpis,<br/>gold_safety_signal_summary"]
    end
    S1 & S2 & S3 --> DIMS
    S4 & S5 & S6 & S7 --> FACTS
    FACTS --> AGGS

    GOLD --> DQ["dq/data_quality_checks.py<br/>25 checks: completeness,<br/>uniqueness, referential integrity,<br/>freshness -> dq_results table"]
    DQ --> GOV["governance/apply_governance.py<br/>PI-name mask, SAE row filter,<br/>4 classification tags"]
    GOV --> WH["Databricks SQL<br/>Serverless Starter Warehouse<br/>(DirectQuery source for Power BI)"]
    GOV -- "azure target only" --> EXPORT["transforms/09_export_gold_to_adls.py<br/>plain Spark write, deletion vectors off,<br/>11 tables -> pharma_lakehouse_gold schema"]
    EXPORT --> GOLDSTORE["ADLS Gen2 pharma-gold/<br/>named-path external Delta tables"]
    GOLDSTORE --> SYN2["Synapse serverless SQL<br/>OPENROWSET ... FORMAT='DELTA'<br/>(sql/synapse_serving_views.sql)"]
```

**Step by step (row counts from a real verified run):**

1. **Generate** (`generate_sample_data` task): writes 7 CSVs straight into
   the UC Volume — 60 clinical trial sites, 12 drug products, 8 distribution
   centers, 500 batches, 4,000 shipments, 1,200 adverse events, 3,600
   inventory snapshots (90 days × 8 DCs × 5 sampled products).
2. **Bronze** (`run_pipeline` task, first stage): Auto Loader picks up each
   CSV as-is, adding `_ingested_ts` and `_source_file` — one bronze table
   per source entity, no business logic.
3. **Silver**: deduplicated on primary key, tagged with `_source_system`
   (which upstream system each entity would really come from — CTMS, ERP,
   WMS, MES, SAFETY_DB) and `_valid_from`, with Lakeflow expectations
   enforced per table (some rows dropped outright, others flagged but kept —
   see the per-table rules in `transforms/silver.py`).
4. **Gold**: joins and aggregates silver into a star schema — 4 dimensions,
   4 facts, 3 pre-aggregated business summaries (11 tables total), all
   Unity-Catalog-managed (an explicit storage `path=` here is rejected
   outright by Lakeflow under UC governance — see
   [architecture.md](architecture.md)).
5. **Data quality** (`run_data_quality_checks` task): 25 checks across
   completeness, uniqueness, referential integrity, and freshness, logged
   to `dq_results` — **25/25 passing** on every verified run. Any failure
   fails the whole job rather than shipping bad data downstream.
6. **Governance** (`apply_governance` task): column mask on
   `dim_site.principal_investigator`, row filter restricting `severity =
   'SAE'` rows in `fact_adverse_event`, and 4 classification tags — all
   gated on `is_account_group_member('admins')`.
7. **Serving, path A — Databricks SQL**: the gold tables (still
   UC-managed, governed) are queried directly by the Serverless Starter
   Warehouse; Power BI DirectQueries this.
8. **Serving, path B — Synapse (`azure` target only)**: `export_gold_to_adls`
   copies all 11 gold tables to `pharma-gold` as named-path external Delta
   tables (deletion vectors explicitly disabled at write time — Synapse's
   Delta reader can't handle that protocol feature), registered under a
   separate `pharma_lakehouse_gold` schema. Synapse serverless SQL reads
   these directly via `OPENROWSET ... FORMAT = 'DELTA'` — verified with a
   real cross-table `JOIN` (`gold_safety_signal_summary` × `dim_product`,
   440 rows) returning correct results with no ETL in between.

**Note on the export copies:** step 8's tables are physically separate from
the governed originals in step 6 — the mask/row filter apply to
`pharma_lakehouse.*`, not automatically to `pharma_lakehouse_gold.*`. Anyone
querying the export schema directly (including via Synapse) sees ungoverned
data. Documented tradeoff, not a hidden gap — see
[architecture.md § Decision 3](DESIGN.md#decision-3-gold-tables-stay-uc-managed-a-separate-export-step-lands-named-path-copies).

## Why these two flows don't connect to each other

Bronze (Databricks side) ingests the synthetic generator's output, not the
files ADF lands in `pharma-raw`. See
[architecture.md § What's still decoupled, on purpose](architecture.md#whats-still-decoupled-on-purpose)
for why that's a deliberate choice, not an oversight — and exactly what
change would connect them if you wanted to.
