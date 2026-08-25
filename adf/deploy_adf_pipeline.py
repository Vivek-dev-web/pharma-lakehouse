"""
Creates/updates the ADF orchestration pipeline for pharma-lakehouse inside
the EXISTING `adf-c360-legacy` Data Factory (rg-customer360-legacy) --
no new Data Factory is provisioned.

Auth is Managed-Identity-only end to end:
  - ADLS Gen2 linked service: no key/SAS set, so ADF falls back to its
    system-assigned managed identity, which needs the "Storage Blob Data
    Contributor" role on stc360legacyws (see infra/main.bicep -- that role
    assignment needs to be applied once, by someone with permission to
    modify IAM on the storage account, before this pipeline can actually
    write files; creating the pipeline definition itself doesn't need it).
  - Synapse serverless SQL linked service: connection string uses
    `Authentication=Active Directory Managed Identity` -- no SQL login/password
    anywhere in this repo.

Idempotent: re-running updates existing resources in place.

Requires: pip install azure-mgmt-datafactory azure-identity
Auth: uses DefaultAzureCredential (picks up your `az login` session).
"""
import argparse

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    AzureSqlDatabaseLinkedService,
    AzureBlobFSLinkedService,
    LinkedServiceReference,
    AzureSqlTableDataset,
    DelimitedTextDataset,
    JsonDataset,
    RestResourceDataset,
    DatasetResource,
    DatasetReference,
    LinkedServiceResource,
    PipelineResource,
    CopyActivity,
    TabularSource,
    DelimitedTextSink,
    JsonSink,
    RestSource,
    RestServiceLinkedService,
    ScheduleTrigger,
    ScheduleTriggerRecurrence,
    RecurrenceFrequency,
    TriggerResource,
    TriggerPipelineReference,
    PipelineReference,
)

SYNAPSE_SQL_ENDPOINT = "synapse-c360-legacy-ondemand.sql.azuresynapse.net"
SYNAPSE_DB = "pharma_lakehouse_raw"
STORAGE_ACCOUNT = "stc360legacyws"
CLINICAL_TRIALS_API = "https://clinicaltrials.gov/api/v2/studies"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--resource-group", default="rg-customer360-legacy")
    ap.add_argument("--factory-name", default="adf-c360-legacy")
    ap.add_argument("--subscription-id", default=None, help="Defaults to `az account show`'s current subscription.")
    args = ap.parse_args()

    credential = DefaultAzureCredential()
    client = DataFactoryManagementClient(credential, subscription_id=args.subscription_id or _current_subscription_id())
    rg, factory = args.resource_group, args.factory_name

    # -- ADLS Gen2 linked service (system-assigned managed identity) --------
    # NOTE: azure-mgmt-datafactory 10.0.0 doesn't auto-populate the `type`
    # discriminator from flattened kwargs (SDK quirk) -- pass it explicitly
    # everywhere below or the API rejects the payload with "type is null".
    adls_ls = AzureBlobFSLinkedService(type="AzureBlobFS", url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net")
    client.linked_services.create_or_update(rg, factory, "ls_adls_pharma", LinkedServiceResource(properties=adls_ls))

    # -- Synapse serverless SQL linked service (managed identity, no password)
    # `authentication_type` is a dedicated typed field, not a connection-string
    # keyword -- ADF's copy engine uses an older SQL client that rejects
    # `Authentication=...` embedded in the connection string outright
    # (UserErrorInvalidDbConnectionString: "Invalid value for key 'authentication'").
    synapse_ls = AzureSqlDatabaseLinkedService(
        type="AzureSqlDatabase",
        server=SYNAPSE_SQL_ENDPOINT,
        database=SYNAPSE_DB,
        authentication_type="SystemAssignedManagedIdentity",
        encrypt="mandatory",
    )
    client.linked_services.create_or_update(rg, factory, "ls_synapse_serverless_sql", LinkedServiceResource(properties=synapse_ls))

    # -- REST linked service for the public ClinicalTrials.gov API ----------
    rest_ls = RestServiceLinkedService(
        type="RestService", url=CLINICAL_TRIALS_API, enable_server_certificate_validation=True, authentication_type="Anonymous"
    )
    client.linked_services.create_or_update(rg, factory, "ls_clinical_trials_api", LinkedServiceResource(properties=rest_ls))

    # -- Datasets --------------------------------------------------------------
    batch_master_source_dataset = AzureSqlTableDataset(
        type="AzureSqlTable",
        linked_service_name=LinkedServiceReference(type="LinkedServiceReference", reference_name="ls_synapse_serverless_sql"),
        table_name="dbo.batch_master_source",
    )
    client.datasets.create_or_update(rg, factory, "ds_batch_master_source", DatasetResource(properties=batch_master_source_dataset))

    batch_master_sink_dataset = DelimitedTextDataset(
        type="DelimitedText",
        linked_service_name=LinkedServiceReference(type="LinkedServiceReference", reference_name="ls_adls_pharma"),
        location={"type": "AzureBlobFSLocation", "fileSystem": "pharma-raw", "folderPath": "drug_batches_from_erp"},
        column_delimiter=",",
        first_row_as_header=True,
    )
    client.datasets.create_or_update(rg, factory, "ds_adls_drug_batches", DatasetResource(properties=batch_master_sink_dataset))

    clinical_registry_source_dataset = RestResourceDataset(
        type="RestResource",
        linked_service_name=LinkedServiceReference(type="LinkedServiceReference", reference_name="ls_clinical_trials_api"),
    )
    client.datasets.create_or_update(rg, factory, "ds_clinical_registry_api", DatasetResource(properties=clinical_registry_source_dataset))

    clinical_registry_sink_dataset = JsonDataset(
        type="Json",
        linked_service_name=LinkedServiceReference(type="LinkedServiceReference", reference_name="ls_adls_pharma"),
        location={"type": "AzureBlobFSLocation", "fileSystem": "pharma-raw", "folderPath": "clinical_registry"},
    )
    client.datasets.create_or_update(rg, factory, "ds_adls_clinical_registry", DatasetResource(properties=clinical_registry_sink_dataset))

    # -- Pipeline: two independent copies, no cross-dependency ------------------
    # Same multi-level discriminator quirk as the trigger below (CopyActivity
    # -> ExecutionActivity -> Activity): the constructor stomps an explicit
    # type="Copy" back to "Execution", so set it via attribute assignment.
    copy_batch_master = CopyActivity(
        name="copy_batch_master_sql_to_adls",
        source=TabularSource(type="TabularSource"),
        sink=DelimitedTextSink(type="DelimitedTextSink"),
        inputs=[DatasetReference(type="DatasetReference", reference_name="ds_batch_master_source")],
        outputs=[DatasetReference(type="DatasetReference", reference_name="ds_adls_drug_batches")],
    )
    copy_batch_master.type = "Copy"

    copy_clinical_registry = CopyActivity(
        name="copy_clinical_registry_to_adls",
        source=RestSource(type="RestSource", request_method="GET"),
        sink=JsonSink(type="JsonSink"),
        inputs=[DatasetReference(type="DatasetReference", reference_name="ds_clinical_registry_api")],
        outputs=[DatasetReference(type="DatasetReference", reference_name="ds_adls_clinical_registry")],
    )
    copy_clinical_registry.type = "Copy"

    pipeline = PipelineResource(
        activities=[copy_batch_master, copy_clinical_registry],
        description=(
            "Lands the ERP batch extract (via Synapse serverless SQL, managed-identity "
            "auth) and the ClinicalTrials.gov public API into the pharma-raw ADLS "
            "container -- independent of the Databricks pipeline, which runs "
            "separately on the medallion Free Edition workspace."
        ),
    )
    client.pipelines.create_or_update(rg, factory, "pl_pharma_orchestrate", pipeline)

    # -- Daily schedule trigger (created stopped) --------------------------------
    # ScheduleTrigger's own multi-level discriminator (ScheduleTrigger ->
    # MultiplePipelineTrigger -> Trigger) gets stomped back to the parent's
    # default by the constructor even when passed explicitly -- set it via
    # attribute assignment after construction instead.
    trigger = ScheduleTrigger(
        pipelines=[TriggerPipelineReference(pipeline_reference=PipelineReference(type="PipelineReference", reference_name="pl_pharma_orchestrate"))],
        recurrence=ScheduleTriggerRecurrence(frequency=RecurrenceFrequency.DAY, interval=1, start_time="2026-01-01T02:00:00Z"),
    )
    trigger.type = "ScheduleTrigger"
    client.triggers.create_or_update(rg, factory, "tr_daily_schedule", TriggerResource(properties=trigger))

    print("ADF pipeline, datasets, and linked services deployed to", factory)
    print("Trigger created but not started -- start it once a manual test run succeeds:")
    print(f"  az datafactory trigger start -g {rg} --factory-name {factory} --name tr_daily_schedule")
    print()
    print("Requires the storage RBAC role assignment in infra/main.bicep to be applied")
    print("first (grants this factory's managed identity write access to pharma-raw),")
    print("and sql/synapse_serving_views.sql run once against the Synapse serverless")
    print("endpoint to create dbo.batch_master_source.")


def _current_subscription_id() -> str:
    import subprocess, json, shutil
    az = shutil.which("az") or shutil.which("az.cmd") or "az"
    out = subprocess.check_output([az, "account", "show", "-o", "json"], shell=(az == "az"))
    return json.loads(out)["id"]


if __name__ == "__main__":
    main()
