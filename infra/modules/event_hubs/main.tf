resource "azurerm_eventhub_namespace" "this" {
  name                = "${var.name_prefix}-${var.name_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = var.sku
  capacity            = var.capacity

  tags = var.tags
}

resource "azurerm_eventhub" "transactions" {
  name              = var.event_hub_name
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = var.partition_count
  message_retention = var.message_retention_days
}
