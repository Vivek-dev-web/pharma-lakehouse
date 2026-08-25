# Backlog (Jira-style epics & user stories)

Kept here as a plain-text mirror of what would live in Jira in a real
engagement — useful to show Agile/SDLC ways-of-working in an interview
without needing an actual Jira instance for a portfolio project.

## Epic 1: Ingestion & Orchestration
- **PHLK-1** As a data engineer, I want ADF to copy `batch_master` from Azure
  SQL DB into `raw_landing` daily, so manufacturing data is available for the
  lakehouse without a manual export. *(AC: copy activity succeeds on schedule;
  failure triggers the alert webhook.)*
- **PHLK-2** As a data engineer, I want ADF to pull enrichment data from the
  ClinicalTrials.gov public API, so site metadata can be cross-referenced
  against the public registry. *(AC: JSON lands in `raw_landing/clinical_registry/`
  with today's date partition.)*
- **PHLK-3** As a data engineer, I want the ADF pipeline to trigger the
  Databricks job on successful ingestion, so bronze/silver/gold refresh
  automatically without manual intervention.

## Epic 2: Lakehouse Transformation
- **PHLK-4** As a data engineer, I want Auto Loader bronze tables for every
  source entity, so new files are picked up incrementally without reprocessing
  history.
- **PHLK-5** As a data engineer, I want silver-layer expectations (valid IDs,
  positive quantities, valid QC status) enforced at write time, so bad rows
  are dropped or flagged before they reach gold.
- **PHLK-6** As a BI analyst, I want a conformed star schema in gold (dims +
  facts + pre-aggregated summaries), so Power BI reports don't need
  Spark-side joins.

## Epic 3: Data Quality & Governance
- **PHLK-7** As a data steward, I want a post-pipeline data-quality checkpoint
  (completeness, uniqueness, referential integrity, freshness) logged to a
  queryable table, so quality is trackable over time and the job fails loudly
  on regression.
- **PHLK-8** As a compliance owner, I want PI names masked and SAE-severity
  adverse-event rows restricted to the `admins` group, so sensitive fields
  aren't exposed to every report viewer.
- **PHLK-9** As an auditor, I want every silver row tagged with its source
  system and load timestamp, so any gold-layer number can be traced back to
  its origin (GxP-style audit trail).

## Epic 4: Serving & Visualization
- **PHLK-10** As a BI analyst, I want Synapse serverless SQL views over gold
  Delta tables, so Power BI can query the lakehouse directly with no
  duplicate ETL step.
- **PHLK-11** As a supply-chain manager, I want a Power BI dashboard showing
  on-time shipment rate, temperature-excursion rate, and inventory-below-reorder
  alerts, so I can react to supply risk same-day.
- **PHLK-12** As a safety officer, I want a Power BI page tracking SAE rate by
  product/month, so emerging safety signals are visible without waiting for a
  manual report.

## Epic 5: Platform & DevOps
- **PHLK-13** As a platform engineer, I want infra defined in Bicep and
  deployed via a gated CI/CD pipeline, so environment changes are
  reviewable, repeatable, and never applied by hand.
- **PHLK-14** As a data engineer, I want pytest coverage on every pure-Python
  business rule used in gold aggregations, so a rule change can't silently
  break a KPI without a failing test.
- **PHLK-15** As a reviewer, I want every PR to run bundle validation and unit
  tests before merge, so broken pipeline code never reaches `main`.

## Definition of Done (applies to every story above)
- Code peer-reviewed (PR approval required on `main`).
- Unit tests passing in CI (`devops/azure-pipelines.yml` `test` stage).
- `databricks bundle validate` clean.
- Documentation updated (`docs/data_dictionary.md` and/or this backlog) if the
  schema or a business rule changed.
