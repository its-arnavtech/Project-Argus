data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "this" {
  name                = "${var.name_prefix}-${var.name_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = var.sku_name

  # RBAC, not access policies -- current best practice.
  rbac_authorization_enabled = true
  soft_delete_retention_days = var.soft_delete_retention_days
  purge_protection_enabled   = var.purge_protection_enabled

  tags = var.tags
}

# RBAC-authorized vaults grant nobody access by default, including the
# deployer -- without this, Chunk 4/10 couldn't write the Event Hubs
# connection string / PII salt secrets this vault exists to hold.
resource "azurerm_role_assignment" "deployer_secrets_officer" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}
