# Databricks notebook source
# Unity Catalog governance: column masking, row filters, and classification
# tags. Gated on `is_account_group_member('admins')` so non-admin viewers
# (e.g. a "site_operations" group) see restricted data, while admins see
# everything -- add a non-admin group/user to your workspace to observe the
# restriction take effect (the workspace owner always sees unmasked data).
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "pharma_lakehouse")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")


def run(sql, label):
    try:
        spark.sql(sql)
        print(f"[OK]      {label}")
    except Exception as e:
        print(f"[SKIPPED] {label} -- {e}")


# -- Column masking: PI name is sensitive; mask for non-admins --------------
run(
    """
    CREATE OR REPLACE FUNCTION mask_pi_name(pi STRING)
    RETURNS STRING
    RETURN CASE
        WHEN is_account_group_member('admins') THEN pi
        ELSE 'REDACTED'
    END
    """,
    "create mask_pi_name function",
)
run(
    "ALTER TABLE dim_site ALTER COLUMN principal_investigator SET MASK mask_pi_name",
    "apply mask to dim_site.principal_investigator",
)

# -- Row filter: restrict SAE (serious adverse event) rows to admins --------
run(
    """
    CREATE OR REPLACE FUNCTION restrict_sae_rows(severity STRING)
    RETURNS BOOLEAN
    RETURN severity != 'SAE' OR is_account_group_member('admins')
    """,
    "create restrict_sae_rows function",
)
run(
    "ALTER TABLE fact_adverse_event SET ROW FILTER restrict_sae_rows ON (severity)",
    "apply row filter to fact_adverse_event",
)

# -- Classification tags (documentation / discovery, not enforcement) -------
tag_statements = [
    ("dim_site", "principal_investigator", "sensitivity", "confidential"),
    ("fact_adverse_event", "severity", "sensitivity", "safety_signal"),
    ("fact_adverse_event", "patient_age_band", "sensitivity", "deidentified_phi_adjacent"),
    ("fact_batch_release", "qc_status", "domain", "gxp_quality"),
]
for table, column, tag_key, tag_value in tag_statements:
    run(
        f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ('{tag_key}' = '{tag_value}')",
        f"tag {table}.{column} ({tag_key}={tag_value})",
    )

print("Governance pass complete.")
