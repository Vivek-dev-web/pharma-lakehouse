// Adds pharma-lakehouse-specific resources on top of your EXISTING Azure
// footprint instead of provisioning duplicates -- reuses:
//   - Storage account:   stc360legacyws        (rg-customer360-legacy)
//   - Data Factory:      adf-c360-legacy       (rg-customer360-legacy)
//   - Synapse workspace: synapse-c360-legacy   (rg-customer360-legacy)
//
// New resources this template adds:
//   - `pharma-raw` and `pharma-gold` blob containers in the existing storage
//     account.
//   - A real Azure Databricks (Premium) workspace, replacing the AWS Free
//     Edition `medallion` workspace this project used for its first pass --
//     see docs/architecture.md for why: only an Azure-hosted workspace can
//     register an ADLS Gen2 external location in Unity Catalog, which is
//     what lets Synapse read the gold Delta tables directly and lets ADF
//     land files straight into the path Auto Loader watches.
//   - A Databricks Access Connector (system-assigned managed identity) --
//     the identity Unity Catalog's storage credential/external location will
//     authenticate as. Its role assignment lives in rbac.bicep, not here.
//   - A monthly cost budget on the resource group with email alerts at
//     50/80/100% of the threshold, as a safety net against a forgotten
//     always-on cluster.
//
// Deliberately NOT here: the two storage role assignments (for ADF's and
// the access connector's managed identities). Granting IAM/RBAC is a
// security-setting change kept in a separate file (rbac.bicep) so it can be
// reviewed and applied on its own -- see that file's header.
//
// Deploy at resource-group scope against the EXISTING resource group:
//   az deployment group create -g rg-customer360-legacy --template-file main.bicep

@description('Name of the existing storage account to add containers to and grant access on.')
param existingStorageAccountName string = 'stc360legacyws'

@description('Name of the existing Data Factory to grant storage access to.')
param existingDataFactoryName string = 'adf-c360-legacy'

@description('Location for the new Databricks workspace + access connector. Defaults to the resource group location.')
param location string = resourceGroup().location

@description('Short project prefix used to build the new resource names.')
param projectPrefix string = 'pharmalake'

@description('Monthly cost budget amount (USD) that triggers alert emails.')
param monthlyBudgetAmount int = 25

@description('Email address to send budget alerts to.')
param budgetAlertEmail string = 'vivekt94@gmail.com'

var databricksWorkspaceName = '${projectPrefix}-dbx'
var accessConnectorName = '${projectPrefix}-uc-access-connector'
var managedResourceGroupName = '${projectPrefix}-dbx-managed-${uniqueString(resourceGroup().id)}'

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: existingStorageAccountName
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' existing = {
  parent: storageAccount
  name: 'default'
}

resource pharmaRawContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'pharma-raw'
}

resource pharmaGoldContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'pharma-gold'
}

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: existingDataFactoryName
}

// -- Azure Databricks (Premium -- required for Unity Catalog) ---------------
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: databricksWorkspaceName
  location: location
  sku: { name: 'premium' }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedResourceGroupName)
  }
}

// -- Unity Catalog storage credential identity -------------------------------
resource accessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: accessConnectorName
  location: location
  identity: { type: 'SystemAssigned' }
}

// -- Cost safety net ----------------------------------------------------------
resource costBudget 'Microsoft.Consumption/budgets@2023-11-01' = {
  name: '${projectPrefix}-monthly-budget'
  properties: {
    category: 'Cost'
    amount: monthlyBudgetAmount
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: '2026-08-01'
      endDate: '2027-08-01'
    }
    notifications: {
      actual_50pct: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 50
        thresholdType: 'Actual'
        contactEmails: [budgetAlertEmail]
      }
      actual_80pct: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 80
        thresholdType: 'Actual'
        contactEmails: [budgetAlertEmail]
      }
      actual_100pct: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        thresholdType: 'Actual'
        contactEmails: [budgetAlertEmail]
      }
    }
  }
}

output pharmaRawContainerUrl string = '${storageAccount.properties.primaryEndpoints.dfs}${pharmaRawContainer.name}'
output pharmaGoldContainerUrl string = '${storageAccount.properties.primaryEndpoints.dfs}${pharmaGoldContainer.name}'
output dataFactoryPrincipalId string = dataFactory.identity.principalId
output databricksWorkspaceUrl string = 'https://${databricksWorkspace.properties.workspaceUrl}'
output accessConnectorId string = accessConnector.id
output accessConnectorPrincipalId string = accessConnector.identity.principalId
