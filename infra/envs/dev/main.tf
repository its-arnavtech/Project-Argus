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
  }
}

provider "azurerm" {
  subscription_id = var.subscription_id
  features {}
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
  capacity               = local.cfg.eventhub_capacity
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
  throughput        = local.cfg.cosmos_throughput

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
