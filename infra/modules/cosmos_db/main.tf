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

# Chunk 3 scope stopped at the account/database. Chunk 5 prep creates the
# actual Gremlin graph (container) below.
resource "azurerm_cosmosdb_gremlin_database" "this" {
  name                = var.database_name
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.this.name
  throughput          = var.throughput
}

# Single container for the whole graph -- Cosmos DB Gremlin graphs are one
# container with one partition key, not one container per vertex label
# (despite PDD_Production_Guide.md section 1's table implying otherwise;
# that table's per-label "Partition Key" column is reinterpreted as
# indexing policy guidance instead, applied via index_policy below -- see
# docs/architecture/partition_key_strategy.md for the full reasoning; this
# is a resolved decision, not re-litigated here).
#
# No throughput/autoscale_settings block here: this container shares the
# database's already-provisioned throughput (azurerm_cosmosdb_gremlin_database.this,
# 1000 RU/s). Confirmed against Microsoft Learn docs (not assumed): a
# free-tier account's shared-throughput database covers up to 25 containers
# at $0 -- this is the first of 25, so it stays fully free.
resource "azurerm_cosmosdb_gremlin_graph" "this" {
  name                = var.graph_name
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.this.name
  database_name       = azurerm_cosmosdb_gremlin_database.this.name
  partition_key_path  = var.partition_key_path

  index_policy {
    automatic      = true
    indexing_mode  = "consistent"
    included_paths = ["/*"]
    excluded_paths = ["/\"_etag\"/?"]

    # Composite indexes preserve the PDD table's per-property query-
    # efficiency intent without needing separate containers per vertex
    # label. risk_base/gnn_risk_score: Account risk scoring (gnn_risk_score
    # lands in Chunk 7's inference write-back). mcc_code/hop_distance:
    # merchant-category-scoped multi-hop queries (hop_distance is a Chunk 8
    # agent-computed property, not in ingested data yet -- indexing it now
    # costs nothing, since Cosmos only indexes properties that actually
    # appear on a document).
    composite_index {
      index {
        path  = "/risk_base"
        order = "Ascending"
      }
      index {
        path  = "/gnn_risk_score"
        order = "Descending"
      }
    }

    composite_index {
      index {
        path  = "/mcc_code"
        order = "Ascending"
      }
      index {
        path  = "/hop_distance"
        order = "Ascending"
      }
    }
  }
}
