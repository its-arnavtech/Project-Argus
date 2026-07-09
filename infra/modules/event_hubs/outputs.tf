output "namespace_name" {
  value = azurerm_eventhub_namespace.this.name
}

output "namespace_id" {
  value = azurerm_eventhub_namespace.this.id
}

output "namespace_hostname" {
  value = "${azurerm_eventhub_namespace.this.name}.servicebus.windows.net"
}

output "event_hub_name" {
  value = azurerm_eventhub.transactions.name
}

output "primary_connection_string" {
  value     = azurerm_eventhub_namespace.this.default_primary_connection_string
  sensitive = true
}
