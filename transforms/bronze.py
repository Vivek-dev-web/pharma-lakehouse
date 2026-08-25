# Databricks notebook source
# Bronze layer: raw, schema-on-read ingestion via Auto Loader (cloudFiles).
# Every bronze table is a straight pass-through of the source file plus
# ingestion metadata -- no business logic here. This mirrors a GxP-style
# "as received from source" audit copy of the data.
import dlt
from pyspark.sql import functions as F

RAW_ROOT = spark.conf.get("raw_root")

ENTITIES = [
    "clinical_trial_sites",
    "drug_products",
    "distribution_centers",
    "drug_batches",
    "shipments",
    "adverse_events",
    "inventory_snapshots",
]


def make_bronze_table(entity: str):
    @dlt.table(
        name=f"bronze_{entity}",
        comment=f"Raw {entity} records as landed from source, via Auto Loader.",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{RAW_ROOT}/_schemas/{entity}")
            .option("header", "true")
            .load(f"{RAW_ROOT}/{entity}/")
            .withColumn("_ingested_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

    return _bronze


for entity in ENTITIES:
    make_bronze_table(entity)
