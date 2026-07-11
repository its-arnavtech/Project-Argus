# Chunk 10: the Rust ingestion engine deployed as a real (scale-to-zero)
# Container App in the EXISTING argus-dev-cae environment, with
# system-assigned managed identity as the production auth path.
#
# The dev-only az-CLI-identity role assignments from Chunks 4/8/9 are NOT
# removed -- they remain the local development auth path; this file is the
# separate production path (see context.md's Environment & Resource
# Reference for the two-identity breakdown).

# --- KEDA checkpoint storage -------------------------------------------------
# The KEDA azure-eventhub scaler REQUIRES a blob checkpoint container in all
# checkpoint strategies except AzureFunction (verified against KEDA docs,
# not assumed) -- even with managed-identity auth. This storage account
# exists solely for that; Standard LRS with kilobytes of blobs rounds to
# ~$0/month. Flagged in the Chunk 10 plan as a new (near-zero-cost)
# billable resource for explicit approval.
resource "azurerm_storage_account" "keda_checkpoints" {
  name                     = "stargusdev${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"

  tags = local.common_tags
}

resource "azurerm_storage_container" "keda_checkpoints" {
  name                  = "keda-checkpoints"
  storage_account_id    = azurerm_storage_account.keda_checkpoints.id
  container_access_type = "private"
}

# --- The ingestion Container App --------------------------------------------
resource "azurerm_container_app" "ingestion" {
  name                         = "argus-ingestion"
  container_app_environment_id = module.container_apps.environment_id
  resource_group_name          = azurerm_resource_group.this.name
  revision_mode                = "Single"

  identity {
    type = "SystemAssigned"
  }

  # Chunk 10 addendum: pulled from ACR via the app's own system identity
  # (AcrPull, granted in container_registry.tf) -- no admin credentials,
  # no public image. NOTE: app_acr_pull's principal_id references this
  # resource's own identity, so a depends_on in the other direction would
  # be a graph cycle -- ordering for the first-time bootstrap is handled
  # via a targeted apply (role assignment first), not an HCL dependency.
  registry {
    server   = azurerm_container_registry.this.login_server
    identity = "System"
  }

  template {
    min_replicas = 0
    max_replicas = coalesce(var.load_test_max_replicas, 1)

    container {
      name   = "ingestion"
      image  = "${azurerm_container_registry.this.login_server}/argus-ingestion:chunk10"
      cpu    = 0.25
      memory = "0.5Gi"

      env {
        name  = "ARGUS_MODE"
        value = "service"
      }
      env {
        name  = "ARGUS_SINK"
        value = "eventhub"
      }
      env {
        name  = "ARGUS_EVENTHUB_NAMESPACE"
        value = "${module.event_hubs.namespace_name}.servicebus.windows.net"
      }
      env {
        name  = "ARGUS_EVENTHUB_NAME"
        value = module.event_hubs.event_hub_name
      }
      env {
        name  = "ARGUS_KEY_VAULT_URL"
        value = module.key_vault.key_vault_uri
      }
    }

    # KEDA azure-eventhub rule, managed-identity-authenticated
    # (identity_id = "System" -- attribute name verified in the provider
    # source, the website doc has a typo). PDD intent ">5,000 undrained
    # items" scaled down 10x to 500 for this project's actual throughput,
    # consistent with every other scaled-down parameter.
    #
    # HONEST BEHAVIORAL NOTE (also in context.md): nothing in this build
    # commits Event Hubs checkpoints (the Python inference consumer reads
    # @latest without a checkpoint store), so KEDA counts ALL events inside
    # the 1-day retention window as "unprocessed". Practical effect: a burst
    # of >500 events activates the replica, and it stays active until those
    # events age out of retention (~24h), then the app returns to zero.
    # That is the correct literal semantics of "undrained items > threshold"
    # for a hub nobody drains; a checkpointing consumer (enterprise path)
    # would make it lag-accurate.
    custom_scale_rule {
      name             = "eventhub-lag"
      custom_rule_type = "azure-eventhub"
      identity_id      = "System"
      metadata = {
        eventHubNamespace                   = module.event_hubs.namespace_name
        eventHubName                        = module.event_hubs.event_hub_name
        consumerGroup                       = "$Default"
        unprocessedEventThreshold           = "500"
        activationUnprocessedEventThreshold = "500"
        storageAccountName                  = azurerm_storage_account.keda_checkpoints.name
        blobContainer                       = azurerm_storage_container.keda_checkpoints.name
      }
    }
  }

  tags = local.common_tags
}

# --- Managed-identity role assignments (the production auth path) -----------
resource "azurerm_role_assignment" "app_eventhub_sender" {
  scope                = module.event_hubs.namespace_id
  role_definition_name = "Azure Event Hubs Data Sender"
  principal_id         = azurerm_container_app.ingestion.identity[0].principal_id
}

# The KEDA scaler executes under the app's identity and needs Listen claims
# to read partition/lag info (same Send/Listen split Chunk 4 hit).
resource "azurerm_role_assignment" "app_eventhub_receiver" {
  scope                = module.event_hubs.namespace_id
  role_definition_name = "Azure Event Hubs Data Receiver"
  principal_id         = azurerm_container_app.ingestion.identity[0].principal_id
}

resource "azurerm_role_assignment" "app_kv_secrets_user" {
  scope                = module.key_vault.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_container_app.ingestion.identity[0].principal_id
}

# KEDA reads checkpoint blobs (read-only is sufficient; it never writes).
resource "azurerm_role_assignment" "app_storage_blob_reader" {
  scope                = azurerm_storage_account.keda_checkpoints.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_container_app.ingestion.identity[0].principal_id
}

# --- Cosmos Gremlin RBAC via azapi (no native azurerm resource yet) ---------
# (a) Formalizes the previously-untracked dev-identity grant created via az
#     CLI in the pre-Chunk-8 session (id fea56381-...) -- brought under
#     Terraform with `terraform import`, not recreated.
# (b) Grants the Container App's managed identity the same Data Contributor
#     role, scoped to the graph container. Honest scope note: today's
#     deployed service has NO Gremlin code path (it only produces to Event
#     Hubs); this grant is anticipatory per the Chunk 10 instruction and is
#     flagged as such in context.md rather than silently over-provisioned.
locals {
  cosmos_account_id                = "/subscriptions/${var.subscription_id}/resourceGroups/${azurerm_resource_group.this.name}/providers/Microsoft.DocumentDB/databaseAccounts/${module.cosmos_db.account_name}"
  gremlin_data_contributor_role_id = "${local.cosmos_account_id}/gremlinRoleDefinitions/00000000-0000-0000-0000-000000000004"
  gremlin_scope                    = "${local.cosmos_account_id}/dbs/argus-graph/colls/argus-graph-container"
}

resource "azapi_resource" "dev_gremlin_data_contributor" {
  type      = "Microsoft.DocumentDB/databaseAccounts/gremlinRoleAssignments@2025-05-01-preview"
  name      = "fea56381-280f-4482-8619-1eb6e0933ed1"
  parent_id = local.cosmos_account_id

  body = {
    properties = {
      principalId      = data.azurerm_client_config.current.object_id
      roleDefinitionId = local.gremlin_data_contributor_role_id
      scope            = local.gremlin_scope
    }
  }
}

resource "azapi_resource" "app_gremlin_data_contributor" {
  type      = "Microsoft.DocumentDB/databaseAccounts/gremlinRoleAssignments@2025-05-01-preview"
  name      = "b7c2e4a1-4f3d-4b8e-9c6a-2d5f8e1a9b30"
  parent_id = local.cosmos_account_id

  body = {
    properties = {
      principalId      = azurerm_container_app.ingestion.identity[0].principal_id
      roleDefinitionId = local.gremlin_data_contributor_role_id
      scope            = local.gremlin_scope
    }
  }
}

output "ingestion_app_name" {
  value = azurerm_container_app.ingestion.name
}

output "ingestion_app_principal_id" {
  value = azurerm_container_app.ingestion.identity[0].principal_id
}