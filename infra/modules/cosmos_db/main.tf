resource "azurerm_cosmosdb_account" "this" {
  name                = "${var.name_prefix}-${var.name_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"

  free_tier_enabled = var.free_tier_enabled

  consistency_policy {
    consistency_level = var.consistency_level
  }

  # Single region for the dev tier. PDD_Production_Guide.md section 2 specifies
  # multi-region writes for the enterprise tier -- that's a second geo_location
  # block plus enable_multiple_write_locations, deferred until this actually
  # runs at that tier (see envs/dev/variables.tf's tier-switch comment).
  geo_location {
    location          = var.location
    failover_priority = 0
  }

  capabilities {
    name = "EnableGremlin"
  }

  tags = var.tags
}

# Chunk 3 scope stops at the account/database. Chunk 5 creates the actual
# Gremlin graph (container) with the PDD section 1 vertex/edge schema and
# partition key design.
resource "azurerm_cosmosdb_gremlin_database" "this" {
  name                = var.database_name
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.this.name
  throughput          = var.throughput
}
