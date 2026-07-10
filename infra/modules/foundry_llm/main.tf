# Microsoft Foundry account + LLM deployment for the Chunk 8 compliance
# agents' SAR generation.
#
# HISTORY (Chunk 8): originally written for Claude Opus 4.8 "Hosted on
# Azure" (azapi deployment with modelProviderData, per the official
# Azure-Samples/claude starter kit -- azurerm can't express Anthropic
# deployments, issue #31140). The account/project/RBAC below deployed fine,
# but the model deployment failed with InsufficientQuota: EVERY Claude model
# has a hard 0-TPM quota limit on this subscription (credit-grant
# classification -- the eligibility risk Microsoft's docs warn about).
# With user approval, the model was switched to gpt-5-mini (Azure OpenAI,
# first-party): 500K TPM quota available, native azurerm support, bills as
# normal Azure consumption (draws from the credit grant and IS covered by
# the rg budget alert, unlike Claude's Marketplace CCU path), same Entra ID
# auth. Also closer to the PDD's literal "Azure OpenAI via Foundry" wording.
#
# The Foundry account stays azapi: `allowProjectManagement = true` still
# isn't exposed by azurerm_cognitive_account.

terraform {
  required_providers {
    azapi = {
      source  = "Azure/azapi"
      version = "~> 2.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

resource "azapi_resource" "foundry" {
  type      = "Microsoft.CognitiveServices/accounts@2025-10-01-preview"
  name      = var.account_name
  parent_id = var.resource_group_id
  location  = var.location
  tags      = var.tags

  identity {
    type = "SystemAssigned"
  }

  body = {
    kind = "AIServices"
    sku = {
      name = "S0"
    }
    properties = {
      customSubDomainName    = var.account_name
      allowProjectManagement = true
      publicNetworkAccess    = "Enabled"
      disableLocalAuth       = false
    }
  }

  response_export_values = ["name", "identity.principalId"]
}

resource "azapi_resource" "project" {
  type      = "Microsoft.CognitiveServices/accounts/projects@2025-10-01-preview"
  name      = var.project_name
  parent_id = azapi_resource.foundry.id
  location  = var.location
  tags      = var.tags

  identity {
    type = "SystemAssigned"
  }

  body = {
    properties = {}
  }
}

# Entra-token inference access for the dev identity. "Cognitive Services
# User" carries the Microsoft.CognitiveServices/* data actions, which cover
# OpenAI inference -- same dev-only-bridge caveat as the Event Hubs and
# Gremlin RBAC grants (Chunk 10 moves to the Container App's managed
# identity).
resource "azurerm_role_assignment" "dev_cognitive_services_user" {
  scope                = azapi_resource.foundry.id
  role_definition_name = "Cognitive Services User"
  principal_id         = var.principal_id
}

# gpt-5-mini, Azure OpenAI first-party -- native azurerm support, no
# Marketplace terms, no modelProviderData. An explicit version is REQUIRED:
# omitting it 400s with DeploymentModelNotSupported (found empirically;
# available versions via `az cognitiveservices account list-models`).
resource "azurerm_cognitive_deployment" "llm" {
  name                 = var.deployment_name
  cognitive_account_id = azapi_resource.foundry.id

  model {
    format  = "OpenAI"
    name    = var.model_name
    version = var.model_version
  }

  sku {
    name     = "GlobalStandard"
    capacity = var.capacity
  }

  version_upgrade_option = "OnceNewDefaultVersionAvailable"

  depends_on = [azapi_resource.project]
}
