# Design Document: Pharma Lakehouse

## 1. Problem statement

Build a portfolio-grade, end-to-end Azure data engineering project that
exercises the full breadth of a real Azure Data Engineer job description:
pipeline development, multi-source integration (databases + APIs), data
modeling, data quality, automation, BI visualization, DevOps/CI-CD, IaC, and
data governance — on a pharma supply-chain / clinical-safety domain (chosen
to echo the JD's "GxP and Non-GxP SDLC" preference).

**Constraint that shaped every decision below:** minimize real cloud spend by
reusing existing Azure resources wherever possible, rather than provisioning
a parallel environment from scratch.

## 2. Goals

- Cover as many JD responsibilities as possible with something **actually
  deployed and verified running**, not just designed.
- Demonstrate real Azure service integration (ADF, Synapse, ADLS Gen2, Azure
  Databricks) using production-grade auth patterns (managed identity only,
  zero passwords/keys in the repo).
- Keep cost near-zero and auditable (a real budget alert, not just a promise).
- Produce documentation good enough that a stranger — or a future version of
  the person who built it — could redeploy it and explain every decision in
  an interview.

## 3. Non-goals

- A formally validated GxP system (see [gxp_validation_approach.md](gxp_validation_approach.md)
  for exactly where the line is drawn).
- ML/predictive modeling — the JD lists "basic data science" as a preferred,
  not required, skill; out of scope here to keep focus on the core data
  engineering surface area.
- Production-grade disaster recovery, multi-region failover, or SLAs — this
  is a portfolio project, not a production system.

## 4. Architecture summary

Full diagram and layer-by-layer breakdown: [architecture.md](architecture.md).
In short:

```
ADF (Synapse serverless SQL source + public API) --> ADLS Gen2 (pharma-raw)
Azure Databricks (Lakeflow): bronze --> silver --> gold (Unity Catalog managed)
gold --> DQ checks --> UC governance --> export step --> ADLS Gen2 (pharma-gold, named-path external Delta)
Synapse serverless SQL reads both pharma-raw and pharma-gold directly
Power BI DirectQueries Databricks SQL
```

Two Databricks targets exist side by side (`dev` = AWS Free Edition, $0;
`azure` = real Azure Databricks Premium) — a deliberate artifact of how this
was built in two passes, kept rather than discarded because it's a genuinely
useful demonstration of Unity Catalog's cloud-bound storage credential
constraint (see Decision 3 below).

## 5. Key design decisions and tradeoffs

### Decision 1: Reuse existing Azure resources instead of provisioning new ones

**Chosen:** ADF, Synapse workspace, and ADLS Gen2 storage all point at
resources that already existed in `rg-customer360-legacy` (from an earlier,
unrelated project in this portfolio), namespaced with a `pharma-` prefix to
avoid collisions.

**Alternative considered:** provision a dedicated resource group with its
own ADF/Synapse/SQL DB/storage (an early draft of `infra/main.bicep` did
exactly this).

**Why reuse won:** the marginal cost of adding a pipeline/container/schema
to existing resources is close to zero; a parallel environment would have
meant paying for infrastructure that sits idle 99% of the time purely so it
"looks separate." The tradeoff is namespace discipline (every new resource
is prefixed `pharma-`) instead of hard isolation.

### Decision 2: Azure Databricks (real) over AWS Free Edition (free) — after starting with the latter

**Chosen (final):** a real Azure Databricks Premium workspace
(`pharmalake-dbx`), provisioned via `infra/main.bicep`.

**Started with:** the `medallion` Databricks CLI profile — an already-live
AWS Free Edition workspace, genuinely $0, with Unity Catalog and Lakeflow
already enabled. This proved the entire pipeline (bronze → silver → gold →
DQ → governance) end to end before any new Azure spend happened.

**Why the switch:** "Azure Databricks" is what the JD asks for, and
Unity Catalog storage credentials are cloud-bound — an AWS-hosted metastore
categorically cannot register an Azure ADLS external location. That
limitation blocked the more interesting integration goal (Synapse reading
gold Delta tables directly), so once cost was understood to be small and
controllable (see Decision 5), moving to real Azure Databricks was the right
call. The `dev` (AWS) target was kept rather than deleted — it's a
legitimate $0 fallback and a clean before/after reference.

### Decision 3: Gold tables stay UC-managed; a separate export step lands named-path copies

**Discovered, not planned:** the original intent was for `transforms/gold.py`
to write gold tables directly to `pharma-gold` via `path=` on each
`@dlt.table`. Deploying this against the real workspace failed:
`Cannot specify an explicit path for a table when using Unity Catalog`.
Unity Catalog managed tables (even under a schema with a custom storage
root) also use opaque UUID-based storage paths internally, not
human-readable ones — so even routing around the first error wouldn't have
produced a path Synapse could discover.

**Resolution:** `transforms/gold.py` stays a plain, fully UC-managed
pipeline (unchanged from the `dev`/AWS version). A separate notebook,
`transforms/09_export_gold_to_adls.py`, runs after the Lakeflow pipeline as
an ordinary Spark job (outside Lakeflow's governance constraints) and does a
plain `df.write.format("delta").save(<named path>)` + `CREATE TABLE ...
LOCATION`, producing real external Delta tables in a dedicated
`pharma_lakehouse_gold` schema — verified at
`abfss://pharma-gold@stc360legacyws.dfs.core.windows.net/<table>`.

**Named tradeoff:** the exported copies are physically separate from the
governed originals — Unity Catalog masks/row filters
(`governance/apply_governance.py`) apply to `pharma_lakehouse.*`, not
automatically to `pharma_lakehouse_gold.*`. Anyone querying the export
schema directly (including via Synapse) sees ungoverned data. Acceptable for
synthetic demo data; a production version would need row/column security
re-applied at the Synapse view layer, or the export schema access-restricted.

### Decision 4: ADF and Databricks stay orchestration-decoupled

**Chosen:** ADF's `pl_pharma_orchestrate` pipeline does not trigger the
Databricks job, even on the `azure` target where it technically could.

**Why:** bronze still ingests synthetic seed data via its own generator, not
the files ADF lands in `pharma-raw`. Wiring a trigger between two pipelines
that process unrelated data would be a cosmetic connection dressed up as a
real one. Making ADF's landed extracts an actual Auto Loader source is a
legitimate next step (`transforms/bronze.py`'s `raw_root` config would just
need a second source) but wasn't done, in service of the project's guiding
principle: every claim in this repo is backed by something that was actually
run, not merely assembled.

### Decision 5: Bicep over Terraform, RBAC kept in a separate file

**Chosen:** `infra/main.bicep` (provisioning) + `infra/rbac.bicep` (IAM),
deployed separately.

**Why Bicep:** the JD lists Terraform as *preferred*, not required; Bicep is
the Azure-native equivalent (no state backend to manage) and every concept
maps onto Terraform directly if that's what a target environment mandates.

**Why RBAC is split out:** granting IAM permissions is categorically
different from provisioning a resource — it changes who can read/write
*existing* data. Keeping it in its own file made it independently reviewable
and meant a permission system correctly declined to apply it automatically
on first pass, exactly as it should have; it was reviewed and applied
deliberately on a second pass.

### Decision 6: A monthly budget alert instead of trusting estimates

**Chosen:** `Microsoft.Consumption/budgets` in `infra/main.bicep`, $25/month,
email alerts at 50/80/100%.

**Why:** Azure Databricks/Synapse/ADF's "pay only for what you use" pricing
means realistic usage here is a few dollars total — but the actual risk was
never the workload, it was a forgotten always-on cluster or an accidentally
un-suspended warehouse. A budget alert catches that class of mistake without
requiring anyone to remember to check.

## 6. Data model

Star schema (3 dimensions + `dim_date`, 4 facts, 3 pre-aggregated summary
tables) over 7 synthetic entities. Full column-level reference:
[data_dictionary.md](data_dictionary.md).

## 7. Security & governance

- **No passwords or storage keys anywhere in this repo.** Every
  cross-service auth path is managed identity (ADF ↔ storage, ADF ↔
  Synapse, Databricks ↔ storage via access connector) or Azure AD (Synapse
  SQL logins, `az login`-backed Databricks CLI auth).
- **Unity Catalog governance**: column masking (PI names), row filtering
  (SAE-severity adverse events restricted to an `admins` group),
  classification tags — all gated on `is_account_group_member('admins')`.
- **Audit trail**: `_source_system` / `_ingested_ts` / `_valid_from` columns
  on every silver table, plus Delta Lake's own transaction log (`DESCRIBE
  HISTORY`) — a lightweight stand-in for the audit-trail expectations of a
  GxP-adjacent pipeline.

## 8. Cost model (verified, not estimated)

See [architecture.md § Cost](architecture.md#cost-real-numbers-not-just-estimates)
for the full breakdown. Headline number: comfortably under $5 total across
every run, redeploy, and debugging iteration in this build — backstopped by
a $25/month budget alert.

## 9. What's explicitly out of scope / left as next steps

- Wiring ADF's landed extracts into Auto Loader as a real bronze source
  (Decision 4).
- Re-applying row/column security at the Synapse view layer over the
  exported gold copies (Decision 3).
- Formal GxP computer-system-validation artifacts (IQ/OQ/PQ, e-signatures,
  traceability matrix) — see [gxp_validation_approach.md](gxp_validation_approach.md).
- ML/predictive scoring (out of scope per §3).

## 10. JD coverage

See the [README](../README.md#jd-coverage-map) for the full responsibility
→ artifact mapping, with live/verified status per item.
