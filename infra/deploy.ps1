# Deploys the pharma-lakehouse additions (infra/main.bicep) into your
# EXISTING rg-customer360-legacy resource group -- adds a `pharma-raw`
# blob container and grants the existing Data Factory's managed identity
# access to it. No new storage account / Data Factory / SQL server / Databricks
# workspace is created.
#
# Prereqs: `az login` already run, correct subscription selected.
#
# Usage:
#   ./deploy.ps1

param(
    [string]$ResourceGroupName = "rg-customer360-legacy"
)

$ErrorActionPreference = "Stop"

Write-Host "Validating template against $ResourceGroupName..."
az deployment group validate `
    --resource-group $ResourceGroupName `
    --template-file main.bicep

Write-Host "Deploying..."
$deployment = az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file main.bicep `
    | ConvertFrom-Json

Write-Host ""
Write-Host "Deployment complete. Outputs:"
$deployment.properties.outputs | Format-List
