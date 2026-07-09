output "account_name" {
  value = azurerm_cosmosdb_account.this.name
}

output "database_name" {
  value = azurerm_cosmosdb_gremlin_database.this.name
}

output "endpoint" {
  value = azurerm_cosmosdb_account.this.endpoint
}

output "primary_key" {
  value     = azurerm_cosmosdb_account.this.primary_key
  sensitive = true
}

output "graph_name" {
  value = azurerm_cosmosdb_gremlin_graph.this.name
}

output "partition_key_path" {
  value = azurerm_cosmosdb_gremlin_graph.this.partition_key_path
}
