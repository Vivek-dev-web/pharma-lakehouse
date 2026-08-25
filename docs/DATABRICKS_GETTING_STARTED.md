# Databricks Getting-Started Guide (for someone new to Databricks)

This walks through deploying and running this project's Databricks pipeline
from zero Databricks experience. It assumes you're comfortable with a
terminal and basic Python, but have never used Databricks before.

## 1. Concepts you need before touching anything

| Term | What it means here |
|---|---|
| **Workspace** | A Databricks environment tied to a cloud account. This project uses two: one on AWS (free), one on Azure (real, small cost) — see [architecture.md](architecture.md). |
| **Unity Catalog (UC)** | Databricks' governance layer. Everything is addressed as `catalog.schema.table` (e.g. `pharmalake_dbx.pharma_lakehouse.fact_shipment`), like a three-level namespace instead of SQL's usual two. |
| **Volume** | A UC-managed folder for raw files (not tables) — e.g. `/Volumes/pharmalake_dbx/pharma_lakehouse/raw_landing/`. This is where CSVs land before being turned into tables. |
| **Lakeflow Declarative Pipeline (formerly "Delta Live Tables"/DLT)** | A pipeline defined as a set of Python functions decorated `@dlt.table`, where you describe *what* each table should contain and Databricks figures out *how* and *in what order* to compute it. This project's `transforms/bronze.py`, `silver.py`, `gold.py` are one such pipeline. |
| **Databricks Asset Bundle (DAB)** | A YAML file (`databricks.yml` here) that declares everything to deploy — pipelines, jobs, notebooks — as one versioned unit, deployed with `databricks bundle deploy`. Think "infrastructure as code," but for Databricks objects instead of cloud resources. |
| **Job** | An orchestrated sequence of tasks (notebooks, pipeline runs) with dependencies — this project's `pharma_lakehouse_job` runs 5 tasks in order. |
| **SQL Warehouse** | Compute for running SQL queries (as opposed to a general-purpose cluster for Python/Spark code). Auto-starts on the first query, auto-stops after an idle timeout. |
| **Serverless compute** | Databricks manages the underlying VMs for you — no cluster to size or babysit. Everything in this project runs serverless, which is also why nothing here has an idle cost: compute only exists while something is actually running. |

## 2. Prerequisites

- Python 3.11+ (`python --version`)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.230+
  (`databricks --version`; this project used v1.10.0)
- Access to a Databricks workspace. Two options, cheapest first:
  - **Free**: sign up for [Databricks Free Edition](https://www.databricks.com/product/pricing) —
    no credit card, includes Unity Catalog and Lakeflow.
  - **Azure**: an Azure subscription with rights to create a Premium
    Databricks workspace (see [architecture.md](architecture.md) for the
    Bicep template that provisions one).
- `git`, to clone this repo.

## 3. Authenticate the CLI

Add a profile to `~/.databrickscfg` (created automatically the first time
you configure a profile):

**Free Edition / any workspace with a Personal Access Token:**
```ini
[my-profile]
host = https://<your-workspace-url>
token = dapi...  # Settings -> Developer -> Access tokens, in the workspace UI
```

**Azure Databricks, using your `az login` session (no token to manage):**
```ini
[my-profile]
host = https://adb-<workspace-id>.<n>.azuredatabricks.net
auth_type = azure-cli
```
Requires `az login` already run and the Azure CLI on your PATH. This is what
this project's `medallion-azure` profile uses.

Verify it works:
```bash
databricks current-user me --profile my-profile
```

## 4. Clone and look around

```bash
git clone https://github.com/Vivek-dev-web/pharma-lakehouse.git
cd pharma-lakehouse
```

Folder layout that matters for a first deploy:
```
databricks.yml          # the bundle definition -- start here
transforms/              # the Lakeflow pipeline (bronze.py, silver.py, gold.py)
                          # + two plain notebooks (data generation, gold export)
dq/                       # data-quality checkpoint notebook
governance/               # Unity Catalog masks/row filters/tags notebook
```
Open `databricks.yml` first — it's short and everything else in `transforms/`
is referenced from there.

## 5. One-time setup: create the catalog objects

Before the first deploy, the pipeline needs somewhere to write. Pick a
catalog you have `CREATE SCHEMA` rights on (list them with `databricks
catalogs list --profile my-profile`) and create a schema + volume:

```bash
databricks schemas create pharma_lakehouse <your-catalog> --profile my-profile
databricks volumes create <your-catalog> pharma_lakehouse raw_landing MANAGED --profile my-profile
```

If your catalog name isn't `workspace` (Azure Unity Catalog auto-names the
default catalog after the workspace, e.g. `pharmalake_dbx`), override the
bundle variable at deploy time — see step 7.

## 6. Validate before deploying

```bash
databricks bundle validate --profile my-profile
```
This checks the YAML and your auth without changing anything. Fix any
errors here before moving on — a clean validate doesn't guarantee the
pipeline logic is correct, but it does mean the bundle itself is well-formed.

## 7. Deploy

```bash
databricks bundle deploy --profile my-profile
```
Uploads the notebooks and creates the pipeline/job objects in your
workspace (nothing runs yet). If your catalog isn't `workspace`:
```bash
databricks bundle deploy --profile my-profile --var="catalog=<your-catalog>"
```
Or add a new `targets:` entry in `databricks.yml` (see how `dev` and `azure`
differ — copy that pattern).

## 8. Run it

```bash
databricks bundle run pharma_lakehouse_job --profile my-profile
```
This streams progress in your terminal and prints a **Run URL** — open it in
a browser to watch the job graph execute live. Five tasks run in order:

1. `generate_sample_data` — writes synthetic CSVs into the UC volume.
2. `run_pipeline` — the Lakeflow pipeline: bronze (raw ingest) → silver
   (cleaned, deduplicated, expectations enforced) → gold (star schema).
3. `run_data_quality_checks` — completeness/uniqueness/referential-integrity
   checks, logged to a `dq_results` table; fails the job if anything's wrong.
4. `apply_governance` — Unity Catalog column masks, row filters, tags.
5. `export_gold_to_adls` — (Azure target only) copies gold tables to ADLS
   Gen2 as named-path external tables. No-ops elsewhere.

A first run takes 5–10 minutes (serverless compute has to spin up); reruns
are usually faster.

## 9. Check what you got

Find (or create) a SQL Warehouse: `databricks warehouses list --profile
my-profile`. Then, from the Databricks UI's SQL Editor, or via the CLI:

```bash
databricks api post /api/2.0/sql/statements --profile my-profile --json '{
  "warehouse_id": "<id-from-above>",
  "statement": "SELECT COUNT(*) FROM <catalog>.pharma_lakehouse.fact_shipment",
  "wait_timeout": "30s"
}'
```

Or just open the workspace UI → Catalog → your catalog → `pharma_lakehouse`
schema and browse the tables directly — `dq_results` is a good first stop to
confirm every check passed.

## 10. Troubleshooting — real issues hit building this project

These aren't hypothetical; each was hit and fixed while building this repo.
If you're extending this project rather than just running it, you'll likely
run into the same class of issues:

- **`LocalFilesystemAccessDeniedException` on `dbutils.fs.cp`** — serverless
  compute blocks `dbutils` access to the driver's local filesystem (e.g.
  writing to `/tmp` then copying). Write straight to the `/Volumes/...` path
  with plain Python `open()` instead — Volumes are POSIX-accessible.
  (`transforms/00_generate_sample_data.py` does this.)

- **`Cannot specify an explicit path for a table when using Unity Catalog`**
  — a Lakeflow (`dlt.table`) definition can't take a custom `path=` once the
  pipeline is UC-governed. If you need a table at a specific storage
  location, write it with a separate, plain Spark job *outside* the Lakeflow
  pipeline instead (`transforms/09_export_gold_to_adls.py` is exactly this
  pattern).

- **A SQL Warehouse left running** — every query against a stopped warehouse
  auto-starts it; it doesn't auto-stop again until its idle timeout (default
  10 minutes here, tightened to 5 on the Azure workspace). If you're
  cost-conscious, check `databricks warehouses list --profile my-profile`
  after a session and `databricks warehouses stop <id>` if anything's
  `RUNNING` you don't need.

- **Managed identity / cross-cloud auth** — if you're wiring Databricks to
  read/write cloud storage, remember Unity Catalog storage credentials are
  cloud-bound: an AWS-hosted workspace's metastore cannot register an Azure
  storage external location, and vice versa. This is a platform limitation,
  not a config mistake — see [architecture.md](architecture.md) for how this
  project worked around it.

## 11. Where to go next

- Read [architecture.md](architecture.md) for the full system diagram and
  the reasoning behind every non-obvious decision.
- Read [DESIGN.md](DESIGN.md) for the higher-level "why" behind the whole
  project.
- Look at `governance/apply_governance.py` to see Unity Catalog masks/row
  filters in action — add a non-admin user/group to your workspace to
  observe the restriction actually apply (the workspace owner always sees
  unmasked data).
- Try breaking something on purpose — e.g. edit a Lakeflow expectation in
  `transforms/silver.py` to be stricter, redeploy, rerun, and watch
  `dq_results` or the pipeline's own expectation metrics reflect it.
