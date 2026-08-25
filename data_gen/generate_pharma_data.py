"""
Synthetic data generator for the pharma supply-chain / clinical-safety lakehouse.

Generates fully synthetic, non-PII CSVs that mimic a GxP-adjacent pharma domain:
  - clinical_trial_sites   (dimension source)
  - drug_products          (dimension source)
  - drug_batches           (manufacturing / QC release records)
  - distribution_centers   (dimension source)
  - shipments              (batch -> DC -> site logistics)
  - adverse_events         (aggregated, de-identified safety reports)
  - inventory_snapshots    (daily on-hand inventory per DC/product)

No real patient, employee, or site data is used anywhere -- all names/ids are
faker-generated or synthetically constructed. Adverse events carry only an
age *band* and sex, never a birthdate or identifier, consistent with a
de-identified safety-reporting extract.

Usage:
    python generate_pharma_data.py --out-dir ./sample_data --seed 42
"""
import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

COUNTRIES = ["USA", "Germany", "India", "Brazil", "Japan", "UK", "Canada", "Australia"]
REGIONS = {
    "USA": "North America", "Canada": "North America",
    "Germany": "Europe", "UK": "Europe",
    "India": "APAC", "Japan": "APAC", "Australia": "APAC",
    "Brazil": "LATAM",
}
THERAPEUTIC_AREAS = ["Oncology", "Cardiology", "Immunology", "Neurology", "Endocrinology"]
DOSAGE_FORMS = ["Tablet", "Capsule", "Injection", "Infusion", "Oral Solution"]
SEVERITIES = ["Mild", "Moderate", "Severe", "SAE"]
EVENT_TYPES = ["Headache", "Nausea", "Injection Site Reaction", "Fatigue", "Rash", "Dizziness", "GI Upset"]
AGE_BANDS = ["18-29", "30-44", "45-59", "60-74", "75+"]
CARRIERS = ["DHL", "FedEx", "UPS", "World Courier"]
QC_STATUSES = ["Released", "Quarantined", "Rejected"]


def gen_sites(n, rng):
    rows = []
    for i in range(1, n + 1):
        country = rng.choice(COUNTRIES)
        rows.append({
            "site_id": f"SITE{i:04d}",
            "site_name": f"Clinical Site {i:04d}",
            "country": country,
            "region": REGIONS[country],
            "therapeutic_area": rng.choice(THERAPEUTIC_AREAS),
            "principal_investigator": f"PI-{rng.randint(1000, 9999)}",
            "activation_date": (date(2019, 1, 1) + timedelta(days=rng.randint(0, 1800))).isoformat(),
            "status": rng.choice(["Active", "Active", "Active", "Closed"]),
        })
    return rows


def gen_products(n, rng):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "product_id": f"PROD{i:03d}",
            "product_name": f"Drug-{chr(64 + i)}",
            "ndc_code": f"{rng.randint(10000,99999)}-{rng.randint(100,999)}-{rng.randint(10,99)}",
            "dosage_form": rng.choice(DOSAGE_FORMS),
            "strength_mg": rng.choice([5, 10, 25, 50, 100, 250]),
            "therapeutic_area": rng.choice(THERAPEUTIC_AREAS),
            "is_active": rng.choice([True, True, True, False]),
        })
    return rows


def gen_distribution_centers(n, rng):
    rows = []
    for i in range(1, n + 1):
        country = rng.choice(COUNTRIES)
        rows.append({
            "dc_id": f"DC{i:03d}",
            "dc_name": f"Distribution Center {i:03d}",
            "country": country,
            "region": REGIONS[country],
            "capacity_units": rng.randint(50000, 500000),
        })
    return rows


def gen_batches(n, products, rng):
    rows = []
    for i in range(1, n + 1):
        product = rng.choice(products)
        mfg_date = date(2023, 1, 1) + timedelta(days=rng.randint(0, 800))
        qc_status = rng.choices(QC_STATUSES, weights=[92, 5, 3])[0]
        rows.append({
            "batch_id": f"BATCH{i:06d}",
            "product_id": product["product_id"],
            "manufacture_date": mfg_date.isoformat(),
            "expiry_date": (mfg_date + timedelta(days=730)).isoformat(),
            "batch_size_units": rng.randint(10000, 200000),
            "qc_status": qc_status,
            "qc_release_date": (mfg_date + timedelta(days=rng.randint(5, 21))).isoformat()
            if qc_status == "Released" else "",
        })
    return rows


def gen_shipments(n, batches, dcs, sites, rng):
    rows = []
    released = [b for b in batches if b["qc_status"] == "Released"]
    for i in range(1, n + 1):
        batch = rng.choice(released)
        dc = rng.choice(dcs)
        site = rng.choice(sites)
        ship_date = date.fromisoformat(batch["qc_release_date"]) + timedelta(days=rng.randint(1, 30))
        transit_days = rng.randint(1, 10)
        rows.append({
            "shipment_id": f"SHIP{i:07d}",
            "batch_id": batch["batch_id"],
            "dc_id": dc["dc_id"],
            "site_id": site["site_id"],
            "ship_date": ship_date.isoformat(),
            "received_date": (ship_date + timedelta(days=transit_days)).isoformat(),
            "quantity_units": rng.randint(100, 5000),
            "carrier": rng.choice(CARRIERS),
            "temperature_excursion_flag": rng.choices([True, False], weights=[4, 96])[0],
        })
    return rows


def gen_adverse_events(n, batches, sites, rng):
    rows = []
    for i in range(1, n + 1):
        batch = rng.choice(batches)
        site = rng.choice(sites)
        rows.append({
            "event_id": f"AE{i:07d}",
            "batch_id": batch["batch_id"],
            "site_id": site["site_id"],
            "report_date": (date.fromisoformat(batch["manufacture_date"]) + timedelta(days=rng.randint(30, 700))).isoformat(),
            "severity": rng.choices(SEVERITIES, weights=[55, 30, 12, 3])[0],
            "event_type": rng.choice(EVENT_TYPES),
            "patient_age_band": rng.choice(AGE_BANDS),
            "patient_sex": rng.choice(["M", "F", "U"]),
            "reconciled_flag": rng.choices([True, False], weights=[80, 20])[0],
        })
    return rows


def gen_inventory_snapshots(days, dcs, products, rng):
    rows = []
    start = date.today() - timedelta(days=days)
    for d in range(days):
        snap_date = start + timedelta(days=d)
        for dc in dcs:
            for product in rng.sample(products, k=min(5, len(products))):
                rows.append({
                    "snapshot_date": snap_date.isoformat(),
                    "dc_id": dc["dc_id"],
                    "product_id": product["product_id"],
                    "on_hand_qty": rng.randint(0, 20000),
                    "reorder_point": rng.randint(1000, 5000),
                })
    return rows


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):>7,} rows -> {path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="./sample_data")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sites", type=int, default=60)
    ap.add_argument("--products", type=int, default=12)
    ap.add_argument("--dcs", type=int, default=8)
    ap.add_argument("--batches", type=int, default=500)
    ap.add_argument("--shipments", type=int, default=4000)
    ap.add_argument("--adverse-events", type=int, default=1200)
    ap.add_argument("--inventory-days", type=int, default=90)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out_dir)

    sites = gen_sites(args.sites, rng)
    products = gen_products(args.products, rng)
    dcs = gen_distribution_centers(args.dcs, rng)
    batches = gen_batches(args.batches, products, rng)
    shipments = gen_shipments(args.shipments, batches, dcs, sites, rng)
    adverse_events = gen_adverse_events(args.adverse_events, batches, sites, rng)
    inventory = gen_inventory_snapshots(args.inventory_days, dcs, products, rng)

    write_csv(out / "clinical_trial_sites.csv", sites)
    write_csv(out / "drug_products.csv", products)
    write_csv(out / "distribution_centers.csv", dcs)
    write_csv(out / "drug_batches.csv", batches)
    write_csv(out / "shipments.csv", shipments)
    write_csv(out / "adverse_events.csv", adverse_events)
    write_csv(out / "inventory_snapshots.csv", inventory)


if __name__ == "__main__":
    main()
