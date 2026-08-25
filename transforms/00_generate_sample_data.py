# Databricks notebook source
# Generates synthetic pharma data and writes it directly as CSV into the
# raw_landing Unity Catalog volume, where Auto Loader (bronze.py) picks it
# up. In production this task is replaced by ADF (see adf/) landing real
# extracts from the ERP/CTMS/WMS source systems into the same volume path.
#
# Writes go straight to the /Volumes/... path via plain Python file I/O
# (Volumes are POSIX-accessible) rather than staging to local /tmp and using
# dbutils.fs.cp -- serverless compute blocks dbutils access to the driver's
# local filesystem, so the /tmp-then-copy pattern that works on classic
# clusters fails here with LocalFilesystemAccessDeniedException.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "pharma_lakehouse")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
raw_root = f"/Volumes/{catalog}/{schema}/raw_landing"

# COMMAND ----------
import sys
sys.path.append("../data_gen")
from generate_pharma_data import (
    gen_sites, gen_products, gen_distribution_centers, gen_batches,
    gen_shipments, gen_adverse_events, gen_inventory_snapshots, write_csv,
)
import random
from pathlib import Path

rng = random.Random(42)

sites = gen_sites(60, rng)
products = gen_products(12, rng)
dcs = gen_distribution_centers(8, rng)
batches = gen_batches(500, products, rng)
shipments = gen_shipments(4000, batches, dcs, sites, rng)
adverse_events = gen_adverse_events(1200, batches, sites, rng)
inventory = gen_inventory_snapshots(90, dcs, products, rng)

entities = {
    "clinical_trial_sites": sites,
    "drug_products": products,
    "distribution_centers": dcs,
    "drug_batches": batches,
    "shipments": shipments,
    "adverse_events": adverse_events,
    "inventory_snapshots": inventory,
}

# COMMAND ----------
# Write each entity's CSV straight into its own raw_landing subfolder (Volume
# path is a normal filesystem path) so Auto Loader schema inference /
# evolution is scoped per-entity.
for name, rows in entities.items():
    dst = Path(f"{raw_root}/{name}/{name}.csv")
    write_csv(dst, rows)
    print(f"landed {name} -> {dst}")
