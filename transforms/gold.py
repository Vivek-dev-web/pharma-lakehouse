# Databricks notebook source
# Gold layer: dimensional star schema + business aggregates for BI
# (Power BI / Synapse serverless SQL consume these directly).
#
# Every gold table here is a plain Unity-Catalog-managed Delta table --
# Lakeflow serverless pipelines reject an explicit `path=` on a `dlt.table`
# outright ("Cannot specify an explicit path for a table when using Unity
# Catalog"), so there's no way to land these directly at a chosen ADLS
# location from inside the pipeline. On the `azure` target,
# dq/export_gold_to_adls.py runs after this pipeline and copies each table
# out to a real, named-path external Delta table under `pharma-gold` --
# that's what Synapse serverless SQL actually reads (sql/synapse_serving_views.sql).
import dlt
from pyspark.sql import functions as F


def gold_table(name: str, comment: str):
    return dlt.table(comment=comment)


# --------------------------------------------------------------------------
# Dimensions
# --------------------------------------------------------------------------

@gold_table("dim_date", comment="Date dimension, generated from min/max activity dates.")
def dim_date():
    return (
        spark.sql("SELECT explode(sequence(to_date('2023-01-01'), to_date('2026-12-31'), interval 1 day)) AS calendar_date")
        .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("day_of_week", F.date_format("calendar_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("calendar_date").isin(1, 7))
    )


@gold_table("dim_site", comment="Clinical trial site dimension.")
def dim_site():
    return dlt.read("silver_sites").select(
        "site_id", "site_name", "country", "region", "therapeutic_area",
        "principal_investigator", "activation_date", "status",
    )


@gold_table("dim_product", comment="Drug product dimension.")
def dim_product():
    return dlt.read("silver_products").select(
        "product_id", "product_name", "ndc_code", "dosage_form",
        "strength_mg", "therapeutic_area", "is_active",
    )


@gold_table("dim_distribution_center", comment="Distribution center dimension.")
def dim_distribution_center():
    return dlt.read("silver_distribution_centers").select(
        "dc_id", "dc_name", "country", "region", "capacity_units",
    )


# --------------------------------------------------------------------------
# Facts
# --------------------------------------------------------------------------

@gold_table("fact_batch_release", comment="Fact: one row per manufactured/QC-released batch.")
def fact_batch_release():
    df = dlt.read("silver_batches")
    return (
        df.withColumn("is_released", F.col("qc_status") == "Released")
        .withColumn("is_rejected", F.col("qc_status") == "Rejected")
        .withColumn(
            "days_to_release",
            F.when(
                F.col("qc_release_date").isNotNull() & (F.col("qc_release_date") != ""),
                F.datediff(F.col("qc_release_date"), F.col("manufacture_date")),
            ),
        )
        .select(
            "batch_id", "product_id", "manufacture_date", "expiry_date",
            "batch_size_units", "qc_status", "is_released", "is_rejected",
            "days_to_release",
        )
    )


@gold_table("fact_shipment", comment="Fact: one row per shipment (batch -> DC -> site).")
def fact_shipment():
    df = dlt.read("silver_shipments")
    return (
        df.withColumn("transit_days", F.datediff("received_date", "ship_date"))
        .withColumn("ship_date_key", F.date_format("ship_date", "yyyyMMdd").cast("int"))
        .select(
            "shipment_id", "batch_id", "dc_id", "site_id", "ship_date",
            "received_date", "ship_date_key", "quantity_units", "carrier",
            "temperature_excursion_flag", "transit_days",
        )
    )


@gold_table("fact_adverse_event", comment="Fact: one row per adverse event report.")
def fact_adverse_event():
    df = dlt.read("silver_adverse_events")
    return (
        df.withColumn("report_date_key", F.date_format("report_date", "yyyyMMdd").cast("int"))
        .withColumn("is_serious", F.col("severity") == "SAE")
        .select(
            "event_id", "batch_id", "site_id", "report_date", "report_date_key",
            "severity", "event_type", "patient_age_band", "patient_sex",
            "reconciled_flag", "is_serious",
        )
    )


@gold_table("fact_inventory_snapshot", comment="Fact: one row per DC/product/day inventory snapshot.")
def fact_inventory_snapshot():
    df = dlt.read("silver_inventory_snapshots")
    return (
        df.withColumn("snapshot_date_key", F.date_format("snapshot_date", "yyyyMMdd").cast("int"))
        .withColumn("is_below_reorder", F.col("on_hand_qty") < F.col("reorder_point"))
        .select(
            "snapshot_date", "snapshot_date_key", "dc_id", "product_id",
            "on_hand_qty", "reorder_point", "is_below_reorder",
        )
    )


# --------------------------------------------------------------------------
# Business aggregates (BI-ready summary tables)
# --------------------------------------------------------------------------

@gold_table("gold_batch_quality_summary", comment="Batch QC release rate and cycle time, by product/month.")
def gold_batch_quality_summary():
    return (
        dlt.read("fact_batch_release")
        .withColumn("mfg_month", F.date_trunc("month", "manufacture_date"))
        .groupBy("product_id", "mfg_month")
        .agg(
            F.count("*").alias("batches_total"),
            F.sum(F.col("is_released").cast("int")).alias("batches_released"),
            F.sum(F.col("is_rejected").cast("int")).alias("batches_rejected"),
            F.avg("days_to_release").alias("avg_days_to_release"),
        )
        .withColumn("release_rate_pct", F.round(F.col("batches_released") / F.col("batches_total") * 100, 1))
    )


@gold_table("gold_supply_chain_kpis", comment="Supply-chain KPIs (on-time / excursion rate), by DC/month.")
def gold_supply_chain_kpis():
    return (
        dlt.read("fact_shipment")
        .withColumn("ship_month", F.date_trunc("month", "ship_date"))
        .groupBy("dc_id", "ship_month")
        .agg(
            F.count("*").alias("shipments_total"),
            F.avg("transit_days").alias("avg_transit_days"),
            F.sum(F.col("temperature_excursion_flag").cast("int")).alias("excursions_total"),
        )
        .withColumn(
            "excursion_rate_pct",
            F.round(F.col("excursions_total") / F.col("shipments_total") * 100, 2),
        )
    )


@gold_table("gold_safety_signal_summary", comment="Adverse-event / safety-signal summary, by product/month.")
def gold_safety_signal_summary():
    return (
        dlt.read("fact_adverse_event")
        .join(dlt.read("fact_batch_release").select("batch_id", "product_id"), "batch_id", "left")
        .withColumn("report_month", F.date_trunc("month", "report_date"))
        .groupBy("product_id", "report_month")
        .agg(
            F.count("*").alias("events_total"),
            F.sum(F.col("is_serious").cast("int")).alias("sae_total"),
        )
        .withColumn("sae_rate_pct", F.round(F.col("sae_total") / F.col("events_total") * 100, 2))
    )
