# Data dictionary

## Source entities (landed to `raw_landing`, bronze pass-through)

### clinical_trial_sites (source: CTMS)
| Column | Type | Notes |
|---|---|---|
| site_id | string | PK |
| site_name | string | |
| country / region | string | |
| therapeutic_area | string | |
| principal_investigator | string | masked for non-admins in gold (`dim_site`) |
| activation_date | date | |
| status | string | Active / Closed |

### drug_products (source: ERP)
| Column | Type | Notes |
|---|---|---|
| product_id | string | PK |
| product_name, ndc_code, dosage_form, strength_mg | | |
| therapeutic_area | string | |
| is_active | boolean | |

### distribution_centers (source: WMS)
| Column | Type | Notes |
|---|---|---|
| dc_id | string | PK |
| dc_name, country, region, capacity_units | | |

### drug_batches (source: MES)
| Column | Type | Notes |
|---|---|---|
| batch_id | string | PK |
| product_id | string | FK -> drug_products |
| manufacture_date, expiry_date | date | expiry = manufacture + 730d |
| batch_size_units | int | |
| qc_status | string | Released / Quarantined / Rejected |
| qc_release_date | date, nullable | null unless Released |

### shipments (source: WMS)
| Column | Type | Notes |
|---|---|---|
| shipment_id | string | PK |
| batch_id | string | FK -> drug_batches |
| dc_id | string | FK -> distribution_centers |
| site_id | string | FK -> clinical_trial_sites |
| ship_date, received_date | date | |
| quantity_units | int | |
| carrier | string | |
| temperature_excursion_flag | boolean | cold-chain breach indicator |

### adverse_events (source: SAFETY_DB) -- de-identified
| Column | Type | Notes |
|---|---|---|
| event_id | string | PK |
| batch_id | string | FK -> drug_batches |
| site_id | string | FK -> clinical_trial_sites |
| report_date | date | |
| severity | string | Mild / Moderate / Severe / **SAE** (serious) |
| event_type | string | |
| patient_age_band | string | 5-year-plus bands only, never exact age |
| patient_sex | string | M / F / U |
| reconciled_flag | boolean | safety-team sign-off |

### inventory_snapshots (source: WMS)
| Column | Type | Notes |
|---|---|---|
| snapshot_date | date | grain: 1 row per DC/product/day |
| dc_id, product_id | string | FKs |
| on_hand_qty, reorder_point | int | |

## Silver

Same shape as bronze plus: `_source_system`, `_valid_from` (audit lineage),
deduplicated on primary key, and Lakeflow expectations enforced (see
`transforms/silver.py` for the exact rule per table).

## Gold (star schema)

**Dimensions:** `dim_date`, `dim_site`, `dim_product`, `dim_distribution_center`

**Facts** (grain noted): `fact_batch_release` (1/batch), `fact_shipment`
(1/shipment), `fact_adverse_event` (1/event), `fact_inventory_snapshot`
(1/DC/product/day)

**Business aggregates:** `gold_batch_quality_summary` (product x month),
`gold_supply_chain_kpis` (DC x month), `gold_safety_signal_summary`
(product x month)

## dq_results (data-quality checkpoint log)

| Column | Type | Notes |
|---|---|---|
| check_name | string | e.g. `completeness:shipment_id` |
| table_name | string | |
| passed | boolean | |
| detail | string | human-readable detail (counts, thresholds) |
| run_ts | timestamp | append-only, one row per check per run |
