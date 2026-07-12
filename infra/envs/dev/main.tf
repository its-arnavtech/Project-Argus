terraform {
  required_version = ">= 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    # Added in Chunk 8 for the Foundry/Claude deployment -- azurerm can't
    # express modelProviderData or allowProjectManagement (see
    # modules/foundry_claude/main.tf). Deliberate new dependency, approved
    # with the Chunk 8 deployment plan.
    azapi = {
      source  = "Azure/azapi"
      version = "~> 2.0"
    }
  }
}

provider "azurerm" {
  subscription_id = var.subscription_id
  features {}
}

provider "azapi" {
  subscription_id = var.subscription_id
}

data "azurerm_client_config" "current" {}

# Global uniqueness for Event Hubs namespace / Cosmos account / Key Vault names.
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_resource_group" "this" {
  name     = "rg-argus-${var.tier}"
  location = var.location

  tags = local.common_tags
}

module "event_hubs" {
  source = "../../modules/event_hubs"

  name_prefix         = "evhns-argus-${var.tier}"
  name_suffix         = random_string.suffix.result
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  sku                    = local.cfg.eventhub_sku
  capacity               = coalesce(var.load_test_eventhub_capacity, local.cfg.eventhub_capacity)
  partition_count        = local.cfg.eventhub_partition_count
  message_retention_days = local.cfg.eventhub_retention_days

  tags = local.common_tags
}

module "cosmos_db" {
  source = "../../modules/cosmos_db"

  name_prefix         = "cosmos-argus-${var.tier}"
  name_suffix         = random_string.suffix.result
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  free_tier_enabled = local.cfg.cosmos_free_tier
  throughput        = coalesce(var.benchmark_cosmos_throughput, local.cfg.cosmos_throughput)

  tags = local.common_tags
}

module "container_apps" {
  source = "../../modules/container_apps"

  name_prefix         = "argus-${var.tier}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  tags = local.common_tags
}

module "key_vault" {
  source = "../../modules/key_vault"

  name_prefix         = "kv-argus-${var.tier}"
  name_suffix         = random_string.suffix.result
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  tags = local.common_tags
}

module "budget_alert" {
  source = "../../modules/budget_alert"

  name_prefix       = "argus-${var.tier}"
  resource_group_id = azurerm_resource_group.this.id
  amount            = var.budget_amount
  alert_email       = var.alert_email
  start_date        = var.budget_start_date
}

# Dev-only bridge: lets the current az CLI identity (this developer) send to
# Event Hubs directly for local ingestion testing (Chunk 4). Chunk 10
# replaces this with the Container App's managed identity once the ingestion
# service is actually deployed there -- this is not the production auth
# path, which is why it's kept here (env-specific) rather than folded into
# the reusable event_hubs module.
resource "azurerm_role_assignment" "dev_eventhub_sender" {
  scope                = module.event_hubs.namespace_id
  role_definition_name = "Azure Event Hubs Data Sender"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Chunk 8: Foundry account + gpt-5-mini deployment for the LangGraph
# compliance agents' SAR generation. Pure PAYG token billing, $0 idle.
# Originally targeted Claude Opus 4.8 -- blocked by a subscription-level
# 0-TPM quota on all Claude models; gpt-5-mini fallback user-approved.
# See modules/foundry_llm/main.tf for the full history.
module "foundry_llm" {
  source = "../../modules/foundry_llm"

  account_name      = "argus-${var.tier}-foundry-${random_string.suffix.result}"
  project_name      = "argus-${var.tier}-proj"
  deployment_name   = "gpt-5-mini-argus"
  resource_group_id = azurerm_resource_group.this.id
  location          = azurerm_resource_group.this.location
  principal_id      = data.azurerm_client_config.current.object_id

  tags = local.common_tags
}

# The account/project/RBAC were created under the old module name before the
# Claude->gpt-5-mini switch; keep their state instead of destroy/recreate.
moved {
  from = module.foundry_claude
  to   = module.foundry_llm
}

# Chunk 4 validation needs to read events back to confirm delivery (not just
# send), which the Sender role above doesn't cover ("Listen" claims are a
# separate grant from "Send" claims in Event Hubs' AMQP claim model). Same
# dev-only scope and caveat as the Sender grant above -- not the production
# auth path.
resource "azurerm_role_assignment" "dev_eventhub_receiver" {
  scope                = module.event_hubs.namespace_id
  role_definition_name = "Azure Event Hubs Data Receiver"
  principal_id         = data.azurerm_client_config.current.object_id
}
