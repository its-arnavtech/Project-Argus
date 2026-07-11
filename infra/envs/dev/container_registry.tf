# Chunk 10 addendum: replaces the public-GHCR pull workaround with a
# private Azure Container Registry, pulled via the ingestion app's
# system-assigned managed identity (AcrPull RBAC, no admin credentials,
# no static secret) -- consistent with every other credential decision
# in this project. The image itself no longer needs to be anonymously
# pullable once this lands.
resource "azurerm_container_registry" "this" {
  name                = "acrargusdev${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "Basic"
  admin_enabled       = false

  tags = local.common_tags
}

# Verified in the azurerm provider SOURCE (not just the website doc,
# which is ambiguous/under-specified here): `registry.identity` accepts
# the literal "System" for the app's own system-assigned identity, same
# pattern as the KEDA custom_scale_rule.identity_id attribute -- no
# ValidateFunc restricts it to a user-assigned resource ID.
resource "azurerm_role_assignment" "app_acr_pull" {
  scope                = azurerm_container_registry.this.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_container_app.ingestion.identity[0].principal_id
}

output "container_registry_login_server" {
  value = azurerm_container_registry.this.login_server
}
