output "budget_id" {
  value = azurerm_consumption_budget_resource_group.this.id
}

output "budget_name" {
  value = azurerm_consumption_budget_resource_group.this.name
}
