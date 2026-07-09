resource "azurerm_log_analytics_workspace" "this" {
  name                = "${var.name_prefix}-law"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = var.log_analytics_sku
  retention_in_days   = var.log_retention_days

  tags = var.tags
}

# Consumption-only (scale-to-zero) environment: no workload_profile block.
# Note from the provider docs -- an environment created without an initial
# workload profile cannot have one added later and must be recreated, so
# don't add one here casually in a later chunk without checking that.
# Chunk 4 deploys the ingestion service container into this environment;
# nothing is deployed here yet.
resource "azurerm_container_app_environment" "this" {
  name                       = "${var.name_prefix}-cae"
  location                   = var.location
  resource_group_name        = var.resource_group_name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id
  logs_destination           = "log-analytics"

  tags = var.tags
}
