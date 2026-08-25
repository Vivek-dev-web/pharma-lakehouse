# Databricks notebook source
# Exports every gold table to a real, named-path external Delta table under
# ADLS Gen2, so Synapse serverless SQL can read it directly via a plain
# OPENROWSET BULK '<table_name>' (sql/synapse_serving_views.sql) -- Lakeflow
# serverless pipelines refuse an explicit `path=` on a `dlt.table` when the
# pipeline is Unity-Catalog-governed, so this has to happen as a separate,
# ordinary Spark write outside the DLT pipeline (see transforms/gold.py).
#
# No-ops (skips entirely) when `gold_export_root` isn't set, which is the
# case on the `dev` (AWS Free Edition) target -- Unity Catalog storage
# credentials are cloud-bound, so this step is meaningless there; gold just
# stays UC-managed on `dev` and is served via Databricks SQL DirectQuery only.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "pharma_lakehouse")
dbutils.widgets.text("gold_export_root", "")
dbutils.widgets.text("gold_schema", "pharma_lakehouse_gold")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
gold_export_root = dbutils.widgets.get("gold_export_root")
gold_schema = dbutils.widgets.get("gold_schema")

GOLD_TABLES = [
    "dim_date", "dim_site", "dim_product", "dim_distribution_center",
    "fact_batch_release", "fact_shipment", "fact_adverse_event", "fact_inventory_snapshot",
    "gold_batch_quality_summary", "gold_supply_chain_kpis", "gold_safety_signal_summary",
]

# COMMAND ----------
if not gold_export_root:
    print("gold_export_root not set -- skipping export (expected on the dev/AWS Free Edition target).")
else:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

    for table in GOLD_TABLES:
        source_table = f"{catalog}.{schema}.{table}"
        target_path = f"{gold_export_root}/{table}"
        target_table = f"{catalog}.{gold_schema}.{table}"

        df = spark.table(source_table)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)

        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(f"CREATE TABLE {target_table} USING DELTA LOCATION '{target_path}'")
        print(f"exported {source_table} -> {target_path} (registered as {target_table})")
