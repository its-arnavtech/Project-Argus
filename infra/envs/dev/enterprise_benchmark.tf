# Post-Chunk-11 session: bounded enterprise-tier benchmark resources.
#
# Why this file exists instead of flipping `tier = "enterprise"` wholesale
# (verified consequences, not assumptions):
#   - The tier variable renames the Event Hubs namespace
#     (evhns-argus-enterprise-*), which is a DESTROY + CREATE of the dev
#     namespace -- Standard->Premium is not an in-place upgrade anyway
#     (Microsoft FAQ: "not possible ... without deploying a new resource").
#   - The enterprise map sets cosmos_free_tier = false, and free_tier_enabled
#     forces REPLACEMENT of the Cosmos account -- destroying the loaded
#     graph, every Gremlin RBAC grant, and the once-per-subscription free
#     tier itself.
# So: the Premium namespace is a PARALLEL, NEW resource gated on one toggle
# (teardown = flip to false = the namespace is DELETED, not scaled down),
# and the Cosmos change is a reversible in-place RU bump via
# benchmark_cosmos_throughput (see module call in main.tf).
#
# Cost at time of writing (live Retail Prices API, eastus2):
#   Premium PU $1.027/hr x 4 PUs = $4.108/hr while enabled.
#   Cosmos 10,000 RU/s = 9,000 billed (free tier covers 1,000) x
#   $0.008/100 RU/hr = $0.72/hr while raised.
# Both are bounded-window costs; nothing in this file is meant to survive
# the benchmark session.

variable "enterprise_benchmark_enabled" {
  type        = bool
  default     = false
  description = "Creates the parallel Premium Event Hubs namespace for the bounded enterprise benchmark. Teardown = set false (DELETES the namespace)."
}

variable "benchmark_cosmos_throughput" {
  type        = number
  default     = null
  description = "Temporary Cosmos shared-database RU/s override for the benchmark (10,000 = the single-logical-partition ceiling; higher is provably wasted). Null = tier default."
}

resource "azurerm_eventhub_namespace" "premium_benchmark" {
  count               = var.enterprise_benchmark_enabled ? 1 : 0
  name                = "evhns-argus-prem-${random_string.suffix.result}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "Premium"
  capacity            = 4 # PUs -- the PDD enterprise map's literal number

  tags = local.common_tags
}

resource "azurerm_eventhub" "transactions_premium" {
  count             = var.enterprise_benchmark_enabled ? 1 : 0
  name              = "transactions"
  namespace_id      = azurerm_eventhub_namespace.premium_benchmark[0].id
  partition_count   = 32 # PDD-literal; Premium allows increases later, never decreases
  message_retention = 7  # days, PDD enterprise map
}

# The benchmark's producer/consumer both run locally under the dev az CLI
# identity -- same Send/Listen split Chunk 4 established on the dev
# namespace.
resource "azurerm_role_assignment" "dev_premium_sender" {
  count                = var.enterprise_benchmark_enabled ? 1 : 0
  scope                = azurerm_eventhub_namespace.premium_benchmark[0].id
  role_definition_name = "Azure Event Hubs Data Sender"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_role_assignment" "dev_premium_receiver" {
  count                = var.enterprise_benchmark_enabled ? 1 : 0
  scope                = azurerm_eventhub_namespace.premium_benchmark[0].id
  role_definition_name = "Azure Event Hubs Data Receiver"
  principal_id         = data.azurerm_client_config.current.object_id
}

output "premium_namespace_hostname" {
  value = var.enterprise_benchmark_enabled ? "${azurerm_eventhub_namespace.premium_benchmark[0].name}.servicebus.windows.net" : null
}
