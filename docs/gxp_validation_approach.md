# GxP-adjacent validation approach

This project is a **portfolio demo**, not a validated GxP system — no real
patient data, no real regulatory submission depends on it. This document
exists to show the *shape* of GxP-style computer-system-validation (CSV)
thinking applied to a data pipeline, which is what the JD's "GxP and Non-GxP
SDLC experience" bullet is really asking about: can you reason about a
pipeline the way a regulated environment requires, even outside a formally
validated system.

## Where GxP thinking shows up in this repo

| GxP concept | Where it's applied here |
|---|---|
| **Audit trail** (who/what/when changed data) | `_source_system` + `_valid_from` columns on every silver table (`transforms/silver.py`); `_ingested_ts` + `_source_file` on bronze (`transforms/bronze.py`) |
| **Data integrity (ALCOA+)** | Delta Lake's transaction log gives Attributable, Legible, Contemporaneous, Original, Accurate by construction (immutable history, `DESCRIBE HISTORY`, time travel) |
| **Change control** | All schema/pipeline changes go through PR review + CI (`devops/azure-pipelines.yml`) rather than ad-hoc notebook edits in production |
| **Quality checkpoints** | `dq/data_quality_checks.py` acts as an explicit, loggable QC gate between silver and "released" gold data — analogous to a batch QC release gate in `fact_batch_release` itself |
| **Access control / segregation of duties** | Unity Catalog masks + row filters (`governance/apply_governance.py`) restrict sensitive fields (PI names, SAE-severity events) to an `admins` group, modeling role-based access a validated system would require |
| **Traceability from requirement to test** | `docs/backlog.md` user stories map to specific files/checks; `tests/test_business_logic.py` covers the pure-Python rules those stories depend on |

## What a *real* GxP validation package would add (not implemented here)

- **IQ/OQ/PQ documentation** (Installation/Operational/Performance
  Qualification) with signed evidence per environment.
- **21 CFR Part 11 compliant e-signatures** on change approvals, not just a
  PR approval.
- **Formal User Requirement Specification (URS) -> Functional Spec -> Test
  Script traceability matrix**, typically in a dedicated validation tool
  (e.g., a GxP-qualified ALM), not a markdown backlog.
- **Periodic revalidation** and a documented risk assessment (GAMP 5
  category) for the system as a whole.
- **Vendor qualification** if any third-party service (e.g., the
  ClinicalTrials.gov API) fed data into a decision with regulatory impact.

## Talking points for interviews

- "I know GxP change control means no unreviewed change reaches a production
  data source of truth — that's why every deploy in this repo goes through a
  CI pipeline with a manual approval gate on infra changes, not manual `az`
  commands."
- "Audit trail isn't just a nice-to-have column — Delta Lake's transaction
  log plus explicit `_source_system`/`_ingested_ts` columns mean any number in
  a Power BI report can be traced back to the exact source file and load
  time that produced it."
- "I'd separate a demo/dev pipeline like this from an actual validated GxP
  system the same way this repo does implicitly: synthetic data only, no PII,
  and a clear boundary between 'shows I understand the pattern' and 'is
  actually validated for regulatory use.'"
