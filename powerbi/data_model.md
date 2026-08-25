# Power BI semantic model

Connects **directly to Databricks SQL** via **DirectQuery** using the
`medallion` workspace's existing Serverless Starter Warehouse — no
Synapse hop needed. Gold Delta tables live in that workspace's Unity
Catalog (AWS-hosted Free Edition); Synapse serverless SQL can't read them
because Unity Catalog storage credentials are cloud-bound (an AWS
metastore can't register an Azure ADLS external location, and vice
versa) — see [docs/architecture.md](../docs/architecture.md) for the full
rationale. Databricks SQL DirectQuery is also just a cleaner, more
standard lakehouse-serving pattern than round-tripping through a
warehouse anyway.

## Connect (Power BI Desktop)

1. Get Data -> More -> Azure Databricks (native connector; Power BI
   Desktop ships with it, install the Simba ODBC driver if prompted).
2. Server hostname: `dbc-fbe3df8d-7ffc.cloud.databricks.com`
3. HTTP path: `/sql/1.0/warehouses/601d14523dd66ac2`
4. Authentication: Personal Access Token (use a token generated for the
   `medallion` workspace — do not reuse the one in `~/.databrickscfg`
   directly in a shared/published report; generate a report-scoped token).
5. Mode: **DirectQuery**.
6. Catalog: `workspace`, Schema: `pharma_lakehouse`. Import all `dim_*`,
   `fact_*`, and `gold_*` tables.

The warehouse auto-suspends when idle (Serverless Starter Warehouse,
2X-Small) and cold-starts in a few seconds on the next query — expect a
short pause on the first DirectQuery hit after a period of inactivity.

## Model relationships (star schema)

```
dim_date ──────────┬──────────────┬───────────────────┐
                    │              │                   │
             fact_shipment  fact_adverse_event   fact_inventory_snapshot
                    │              │                   │
dim_site ───────────┼──────────────┘                   │
dim_distribution_center ─┘                              │
dim_product ─────────────────────────────────────────────┘
                    │
             fact_batch_release
```

| From | To | Cardinality | Cross-filter |
|---|---|---|---|
| `dim_date[date_key]` | `fact_shipment[ship_date_key]` | 1:* | Single |
| `dim_date[date_key]` | `fact_adverse_event[report_date_key]` | 1:* | Single |
| `dim_date[date_key]` | `fact_inventory_snapshot[snapshot_date_key]` | 1:* | Single |
| `dim_site[site_id]` | `fact_shipment[site_id]` | 1:* | Single |
| `dim_site[site_id]` | `fact_adverse_event[site_id]` | 1:* | Single |
| `dim_distribution_center[dc_id]` | `fact_shipment[dc_id]` | 1:* | Single |
| `dim_distribution_center[dc_id]` | `fact_inventory_snapshot[dc_id]` | 1:* | Single |
| `dim_product[product_id]` | `fact_batch_release[product_id]` | 1:* | Single |
| `dim_product[product_id]` | `fact_inventory_snapshot[product_id]` | 1:* | Single |
| `fact_batch_release[batch_id]` | `fact_shipment[batch_id]` | 1:* | Single |
| `fact_batch_release[batch_id]` | `fact_adverse_event[batch_id]` | 1:* | Single |

Mark `dim_date` as a **Date table** (Model view -> table tools) so
time-intelligence DAX functions work.

## Key DAX measures

```dax
Batch Release Rate % :=
DIVIDE(SUM(fact_batch_release[is_released]), COUNTROWS(fact_batch_release))

Avg Days to Release :=
AVERAGE(fact_batch_release[days_to_release])

Temp Excursion Rate % :=
DIVIDE(
    SUM(fact_shipment[temperature_excursion_flag]),
    COUNTROWS(fact_shipment)
)

SAE Rate % :=
DIVIDE(
    CALCULATE(COUNTROWS(fact_adverse_event), fact_adverse_event[severity] = "SAE"),
    COUNTROWS(fact_adverse_event)
)

On-Time Shipments :=
CALCULATE(COUNTROWS(fact_shipment), fact_shipment[transit_days] <= 5)

Inventory Below Reorder Point :=
CALCULATE(
    DISTINCTCOUNT(fact_inventory_snapshot[dc_id]),
    fact_inventory_snapshot[is_below_reorder] = TRUE(),
    fact_inventory_snapshot[snapshot_date] = MAX(fact_inventory_snapshot[snapshot_date])
)

SAE Rate % YoY Change :=
VAR CurrentRate = [SAE Rate %]
VAR PriorYearRate = CALCULATE([SAE Rate %], SAMEPERIODLASTYEAR(dim_date[calendar_date]))
RETURN CurrentRate - PriorYearRate
```

## Suggested report pages

1. **Supply Chain Overview** -- shipment volume trend, on-time %, temperature
   excursion rate by DC (map visual using `dim_distribution_center` country),
   inventory-below-reorder alerts.
2. **Batch Quality & Release** -- release rate and avg days-to-release by
   product/month, rejected-batch drill-through to `fact_batch_release`.
3. **Safety Signals** -- SAE rate trend by product, event-type breakdown,
   drill-through from a product to its `fact_adverse_event` detail (row-filtered
   by the `restrict_sae_rows` Unity Catalog row filter for non-admin viewers --
   see `governance/apply_governance.py`).
4. **Site Performance** -- shipments received per site, PI name shown as
   `REDACTED` for non-admin report viewers (Unity Catalog column mask flows
   through DirectQuery, so masking is enforced at the data layer, not just in
   the report).

## Refresh / access

DirectQuery has no scheduled refresh to configure -- queries hit the
Databricks SQL warehouse live. Publish to a Power BI workspace, then manage
row/column security by mapping Power BI viewers to Databricks account groups
consistent with the `admins` group Unity Catalog governance checks
(`is_account_group_member('admins')`), so masking behaves the same whether
someone queries via Databricks SQL directly or through the published report.
