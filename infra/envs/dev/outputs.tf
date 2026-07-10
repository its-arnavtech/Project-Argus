output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "location" {
  value = azurerm_resource_group.this.location
}

output "eventhub_namespace_name" {
  value = module.event_hubs.namespace_name
}

output "eventhub_name" {
  value = module.event_hubs.event_hub_name
}

output "cosmos_account_name" {
  value = module.cosmos_db.account_name
}

output "cosmos_database_name" {
  value = module.cosmos_db.database_name
}

output "cosmos_endpoint" {
  value = module.cosmos_db.endpoint
}

output "cosmos_graph_name" {
  value = module.cosmos_db.graph_name
}

output "cosmos_partition_key_path" {
  value = module.cosmos_db.partition_key_path
}

output "key_vault_name" {
  value = module.key_vault.key_vault_name
}

output "key_vault_uri" {
  value = module.key_vault.key_vault_uri
}

output "container_apps_environment_name" {
  value = module.container_apps.environment_name
}

output "budget_name" {
  value = module.budget_alert.budget_name
}

output "foundry_account_name" {
  value = module.foundry_llm.account_name
}

output "llm_openai_endpoint" {
  value = module.foundry_llm.openai_endpoint
}

output "llm_deployment_name" {
  value = module.foundry_llm.deployment_name
}
