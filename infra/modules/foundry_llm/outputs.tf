output "account_name" {
  value = azapi_resource.foundry.name
}

output "openai_endpoint" {
  value = "https://${azapi_resource.foundry.name}.services.ai.azure.com/"
}

output "deployment_name" {
  value = azurerm_cognitive_deployment.llm.name
}
