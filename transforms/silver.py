# Databricks notebook source
# Silver layer: typed, deduplicated, conformed records with Lakeflow
# **expectations** enforcing data quality, plus GxP-style audit columns
# (_source_system, _valid_from) so every row is traceable back to its
# origin and load time -- a lightweight stand-in for full audit-trail
# requirements (who/what/when) expected in a regulated pipeline.
import dlt
from pyspark.sql import functions as F

SOURCE_SYSTEM = {
    "clinical_trial_sites": "CTMS",
    "drug_products": "ERP",
    "distribution_centers": "WMS",
    "drug_batches": "MES",
    "shipments": "WMS",
    "adverse_events": "SAFETY_DB",
    "inventory_snapshots": "WMS",
}


def _with_audit_cols(df, entity):
    return (
        df.withColumn("_source_system", F.lit(SOURCE_SYSTEM[entity]))
        .withColumn("_valid_from", F.col("_ingested_ts"))
    )


@dlt.table(comment="Deduplicated clinical trial sites.", table_properties={"quality": "silver"})
def silver_sites():
    df = dlt.read_stream("bronze_clinical_trial_sites")
    df = _with_audit_cols(df, "clinical_trial_sites")
    return df.dropDuplicates(["site_id"])


@dlt.table(comment="Deduplicated drug products.", table_properties={"quality": "silver"})
def silver_products():
    df = dlt.read_stream("bronze_drug_products")
    df = _with_audit_cols(df, "drug_products")
    return df.dropDuplicates(["product_id"])


@dlt.table(comment="Deduplicated distribution centers.", table_properties={"quality": "silver"})
def silver_distribution_centers():
    df = dlt.read_stream("bronze_distribution_centers")
    df = _with_audit_cols(df, "distribution_centers")
    return df.dropDuplicates(["dc_id"])


@dlt.table(comment="Batch manufacturing/QC records.", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_batch_id", "batch_id IS NOT NULL")
@dlt.expect_or_drop("valid_qc_status", "qc_status IN ('Released', 'Quarantined', 'Rejected')")
@dlt.expect("expiry_after_manufacture", "expiry_date > manufacture_date")
def silver_batches():
    df = dlt.read_stream("bronze_drug_batches")
    df = _with_audit_cols(df, "drug_batches")
    return df.dropDuplicates(["batch_id"])


@dlt.table(comment="Batch shipments to distribution centers / sites.", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_ids", "shipment_id IS NOT NULL AND batch_id IS NOT NULL")
@dlt.expect_or_drop("positive_quantity", "quantity_units > 0")
@dlt.expect("received_after_shipped", "received_date >= ship_date")
def silver_shipments():
    df = dlt.read_stream("bronze_shipments")
    df = _with_audit_cols(df, "shipments")
    return df.dropDuplicates(["shipment_id"])


@dlt.table(comment="De-identified adverse event reports.", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_severity", "severity IN ('Mild', 'Moderate', 'Severe', 'SAE')")
def silver_adverse_events():
    df = dlt.read_stream("bronze_adverse_events")
    df = _with_audit_cols(df, "adverse_events")
    return df.dropDuplicates(["event_id"])


@dlt.table(comment="Daily inventory snapshots per DC/product.", table_properties={"quality": "silver"})
@dlt.expect_or_drop("non_negative_qty", "on_hand_qty >= 0")
def silver_inventory_snapshots():
    df = dlt.read_stream("bronze_inventory_snapshots")
    df = _with_audit_cols(df, "inventory_snapshots")
    return df.dropDuplicates(["snapshot_date", "dc_id", "product_id"])
