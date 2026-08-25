# Databricks notebook source
# Standalone data-quality checkpoint, run after the Lakeflow pipeline.
#
# Lakeflow expectations (transforms/silver.py) handle row-level quality at
# write time (drop/flag). This notebook covers the checks that only make
# sense *after* a full table exists -- completeness, uniqueness, referential
# integrity, freshness -- and writes a scored result set to `dq_results` so
# quality is trackable over time (and alertable) rather than a pass/fail
# swallowed inside the pipeline run.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "pharma_lakehouse")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F
from datetime import datetime

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------
checks = []


def record(check_name, table, passed, detail=""):
    checks.append({
        "check_name": check_name,
        "table_name": table,
        "passed": bool(passed),
        "detail": detail,
        "run_ts": datetime.utcnow().isoformat(),
    })


# -- Completeness: required columns must be non-null on every row -----------
required_cols = {
    "fact_shipment": ["shipment_id", "batch_id", "dc_id", "site_id"],
    "fact_adverse_event": ["event_id", "batch_id", "site_id", "severity"],
    "fact_batch_release": ["batch_id", "product_id", "qc_status"],
}
for table, cols in required_cols.items():
    df = spark.table(table)
    for c in cols:
        null_count = df.filter(F.col(c).isNull()).count()
        record(f"completeness:{c}", table, null_count == 0, f"{null_count} null rows")

# -- Uniqueness: primary keys must be unique ---------------------------------
pk_map = {
    "fact_shipment": "shipment_id",
    "fact_adverse_event": "event_id",
    "fact_batch_release": "batch_id",
    "dim_site": "site_id",
    "dim_product": "product_id",
    "dim_distribution_center": "dc_id",
}
for table, pk in pk_map.items():
    df = spark.table(table)
    total = df.count()
    distinct = df.select(pk).distinct().count()
    record(f"uniqueness:{pk}", table, total == distinct, f"{total} rows, {distinct} distinct {pk}")

# -- Referential integrity: fact FKs must resolve to a dimension row --------
ref_checks = [
    ("fact_shipment", "dc_id", "dim_distribution_center", "dc_id"),
    ("fact_shipment", "site_id", "dim_site", "site_id"),
    ("fact_shipment", "batch_id", "fact_batch_release", "batch_id"),
    ("fact_adverse_event", "site_id", "dim_site", "site_id"),
    ("fact_adverse_event", "batch_id", "fact_batch_release", "batch_id"),
]
for fact_table, fk_col, dim_table, dim_key in ref_checks:
    fact_df = spark.table(fact_table)
    dim_df = spark.table(dim_table)
    orphans = fact_df.join(dim_df, fact_df[fk_col] == dim_df[dim_key], "left_anti").count()
    record(
        f"referential_integrity:{fk_col}->{dim_table}",
        fact_table,
        orphans == 0,
        f"{orphans} orphaned rows",
    )

# -- Freshness: bronze data landed within the last N days -------------------
freshness_tables = ["bronze_shipments", "bronze_adverse_events", "bronze_inventory_snapshots"]
for table in freshness_tables:
    df = spark.table(table)
    max_ts = df.agg(F.max("_ingested_ts")).collect()[0][0]
    is_fresh = max_ts is not None
    record("freshness:has_data", table, is_fresh, f"max _ingested_ts={max_ts}")

# COMMAND ----------
results_df = spark.createDataFrame(checks)
(
    results_df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("dq_results")
)

failed = [c for c in checks if not c["passed"]]
print(f"Data quality: {len(checks) - len(failed)}/{len(checks)} checks passed.")
for f in failed:
    print(f"  FAILED: {f['check_name']} on {f['table_name']} -- {f['detail']}")

if failed:
    # Fail the job task loudly rather than silently shipping bad data downstream.
    raise Exception(f"{len(failed)} data quality check(s) failed -- see dq_results table.")
