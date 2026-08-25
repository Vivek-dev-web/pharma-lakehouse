// Storage RBAC for pharma-lakehouse -- kept separate from main.bicep because
// granting IAM/access-control permissions is a security-setting change, not
// a resource-provisioning one. Run this yourself after reviewing it; it
// isn't something automated on your behalf.
//
// Grants "Storage Blob Data Contributor" on stc360legacyws to:
//   1. adf-c360-legacy's system-assigned managed identity -- so ADF can
//      read/write pharma-raw without a storage account key.
//   2. pharmalake-uc-access-connector's system-assigned managed identity --
//      the identity Unity Catalog's storage credential authenticates as, so
//      Databricks (and, via the same storage, Synapse serverless SQL) can
//      read/write the pharma-gold container.
//
// Deploy AFTER main.bicep (needs both existing resources' managed identities
// to already exist):
//   az deployment group create -g rg-customer360-legacy --template-file rbac.bicep

@description('Name of the existing storage account to grant access on.')
param existingStorageAccountName string = 'stc360legacyws'

@description('Name of the existing Data Factory.')
param existingDataFactoryName string = 'adf-c360-legacy'

@description('Name of the Databricks access connector created by main.bicep.')
param accessConnectorName string = 'pharmalake-uc-access-connector'

var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: existingStorageAccountName
}

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: existingDataFactoryName
}

resource accessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' existing = {
  name: accessConnectorName
}

resource adfStorageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, dataFactory.id, storageBlobDataContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource accessConnectorStorageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, accessConnector.id, storageBlobDataContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalId: accessConnector.identity.principalId
    principalType: 'ServicePrincipal'
  }
}
