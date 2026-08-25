-- Synapse serverless SQL: exposes both (1) the raw ADF-landed extracts
-- (see adf/) and (2) the Databricks gold Delta tables, as queryable T-SQL
-- views -- using Managed Identity auth only, no storage keys anywhere.
--
-- The gold-layer views only work because Databricks now runs on a real
-- Azure Databricks workspace (infra/main.bicep), not the AWS Free Edition
-- workspace this project used originally. Unity Catalog storage credentials
-- are cloud-bound -- an AWS-hosted metastore can't register an Azure ADLS
-- external location -- so gold Delta tables only land in `pharma-gold` once
-- `transforms/09_export_gold_to_adls.py` has run on the `azure` Databricks
-- target (a plain Spark write + external-table registration, run *outside*
-- the Lakeflow pipeline -- Lakeflow serverless pipelines reject an explicit
-- `path=` on a Unity-Catalog-governed `dlt.table` outright, so a named-path
-- export can't happen from inside transforms/gold.py itself). Power BI
-- still DirectQueries Databricks SQL directly (powerbi/data_model.md) --
-- that doesn't need to change, this just adds a second, independent way to
-- reach the same gold data via plain T-SQL.
--
-- Run against the existing Synapse workspace's serverless SQL endpoint:
--   synapse-c360-legacy-ondemand.sql.azuresynapse.net
-- (workspace: synapse-c360-legacy, resource group: rg-customer360-legacy)
--
-- Verified end to end: every statement below has been applied against the
-- live endpoint, all 14 views resolve, and both ADF copy activities that
-- depend on this script (adf/deploy_adf_pipeline.py) succeed.

CREATE DATABASE pharma_lakehouse_raw;
GO
USE pharma_lakehouse_raw;
GO

-- Storage RBAC (infra/rbac.bicep) is necessary but not sufficient -- ADF's
-- managed identity also needs an actual SQL login in this database.
-- Discovered by testing the real pipeline: without this, ADF's copy
-- activity gets exactly as far as authenticating to the server and then
-- fails with "Login failed for user '<token-identified principal>'".
CREATE USER [adf-c360-legacy] FROM EXTERNAL PROVIDER;
GO
ALTER ROLE db_datareader ADD MEMBER [adf-c360-legacy];
GO

-- db_datareader alone isn't enough either -- ADF's Copy Activity reads a
-- SQL source using bulk-load semantics under the hood. Without this grant,
-- the pipeline gets past authentication and authorization for SELECT, then
-- fails at the very last step with "You do not have permission to use the
-- bulk load statement." (again, only found by actually running it).
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [adf-c360-legacy];
GO

-- Required before any CREATE DATABASE SCOPED CREDENTIAL, even a
-- Managed-Identity one with no embedded secret of its own -- discovered by
-- testing: "Please create a master key in the database..." (15581). Pick
-- your own strong password; nothing sensitive is actually encrypted by it
-- here (managed-identity credentials store no secret), but don't reuse a
-- real password and don't commit whatever you choose to a public repo.
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<REPLACE_WITH_A_STRONG_PASSWORD>';
GO

CREATE DATABASE SCOPED CREDENTIAL cred_adls
WITH IDENTITY = 'Managed Identity';
GO

-- Querying a view built on OPENROWSET doesn't automatically chain
-- permission to the credential the underlying external data source uses --
-- ADF got all the way to "Login failed" -> SQL login -> bulk-operations
-- grant -> and only then hit "Cannot find the CREDENTIAL 'cred_adls' ...
-- or you do not have permission" reading dbo.batch_master_source. Every
-- principal that queries a view referencing this credential needs this.
GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::cred_adls TO [adf-c360-legacy];
GO

CREATE EXTERNAL DATA SOURCE ds_pharma_raw
WITH (
    LOCATION = 'https://stc360legacyws.dfs.core.windows.net/pharma-raw',
    CREDENTIAL = cred_adls
);
GO

CREATE EXTERNAL DATA SOURCE ds_pharma_gold
WITH (
    LOCATION = 'https://stc360legacyws.dfs.core.windows.net/pharma-gold',
    CREDENTIAL = cred_adls
);
GO

CREATE EXTERNAL FILE FORMAT ff_csv
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (FIELD_TERMINATOR = ',', FIRST_ROW = 2)
);
GO

-- "ERP database" source: a one-time-seeded extract at erp_seed/, exposed as
-- a T-SQL-queryable view so ADF's copy_batch_master_sql_to_adls activity has
-- a genuine database endpoint (JDBC/T-SQL over Synapse serverless) to read
-- from -- not just another file copy. This is what a real ERP source table
-- would look like from ADF's point of view; Synapse serverless SQL has no
-- persisted storage engine of its own, so "seeded once, queried via SQL" is
-- the honest zero-new-resource stand-in for a real OLTP source.
CREATE OR ALTER VIEW dbo.batch_master_source AS
SELECT *
FROM OPENROWSET(
    BULK 'erp_seed/*.csv',
    DATA_SOURCE = 'ds_pharma_raw',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    batch_id VARCHAR(20),
    product_id VARCHAR(20),
    manufacture_date DATE,
    expiry_date DATE,
    batch_size_units INT,
    qc_status VARCHAR(20),
    qc_release_date DATE
) AS r;
GO

-- What ADF actually lands (copy_batch_master_sql_to_adls activity output) --
-- a conformed extract at drug_batches_from_erp/, queryable independently of
-- the pipeline that produced it. Wildcard is `*`, not `*.csv` -- ADF's Copy
-- Activity writes an auto-generated filename with no extension when the
-- sink dataset only specifies a folder path, so `*.csv` silently matches
-- nothing (returns an empty result, not an error) -- found by comparing
-- rowsCopied=500 in the ADF activity's own run diagnostics against a 0-row
-- result here.
CREATE OR ALTER VIEW dbo.batch_master AS
SELECT *
FROM OPENROWSET(
    BULK 'drug_batches_from_erp/*',
    DATA_SOURCE = 'ds_pharma_raw',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    batch_id VARCHAR(20),
    product_id VARCHAR(20),
    manufacture_date DATE,
    expiry_date DATE,
    batch_size_units INT,
    qc_status VARCHAR(20),
    qc_release_date DATE
) AS r;
GO

-- External registry enrichment landed by the copy_clinical_registry_to_adls
-- activity (ClinicalTrials.gov v2 API JSON response).
CREATE OR ALTER VIEW dbo.clinical_registry_raw AS
SELECT *
FROM OPENROWSET(
    BULK 'clinical_registry/*.json',
    DATA_SOURCE = 'ds_pharma_raw',
    FORMAT = 'CSV',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE = '0x0b',
    ROWTERMINATOR = '0x0a'
) WITH (doc NVARCHAR(MAX)) AS r;
GO

-- ============================================================================
-- Gold layer: Databricks Lakeflow output, read directly as Delta -- no ETL,
-- no duplicate copy. Only populated once transforms/gold.py has run on the
-- `azure` Databricks target (databricks bundle run pharma_lakehouse_job
-- --profile medallion-azure --target azure).
-- ============================================================================

CREATE OR ALTER VIEW dbo.dim_site AS
SELECT * FROM OPENROWSET(BULK 'dim_site', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.dim_product AS
SELECT * FROM OPENROWSET(BULK 'dim_product', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.dim_distribution_center AS
SELECT * FROM OPENROWSET(BULK 'dim_distribution_center', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.dim_date AS
SELECT * FROM OPENROWSET(BULK 'dim_date', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.fact_shipment AS
SELECT * FROM OPENROWSET(BULK 'fact_shipment', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.fact_adverse_event AS
SELECT * FROM OPENROWSET(BULK 'fact_adverse_event', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.fact_batch_release AS
SELECT * FROM OPENROWSET(BULK 'fact_batch_release', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.fact_inventory_snapshot AS
SELECT * FROM OPENROWSET(BULK 'fact_inventory_snapshot', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.gold_batch_quality_summary AS
SELECT * FROM OPENROWSET(BULK 'gold_batch_quality_summary', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.gold_supply_chain_kpis AS
SELECT * FROM OPENROWSET(BULK 'gold_supply_chain_kpis', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

CREATE OR ALTER VIEW dbo.gold_safety_signal_summary AS
SELECT * FROM OPENROWSET(BULK 'gold_safety_signal_summary', DATA_SOURCE = 'ds_pharma_gold', FORMAT = 'DELTA') AS r;
GO

-- Example: a stakeholder asking "which products had the worst SAE rate last
-- quarter, and were any of their batches also running behind on QC release?"
-- -- straight T-SQL over Databricks-produced Delta tables, no ETL between them.
SELECT
    s.product_id,
    p.product_name,
    s.sae_total,
    s.events_total,
    s.sae_rate_pct,
    q.avg_days_to_release,
    q.release_rate_pct
FROM dbo.gold_safety_signal_summary s
JOIN dbo.dim_product p ON p.product_id = s.product_id
LEFT JOIN dbo.gold_batch_quality_summary q
    ON q.product_id = s.product_id AND q.mfg_month = s.report_month
WHERE s.report_month >= DATEADD(month, -3, GETDATE())
ORDER BY s.sae_rate_pct DESC;
