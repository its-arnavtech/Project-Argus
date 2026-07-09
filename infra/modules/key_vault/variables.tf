variable "name_prefix" {
  type = string
}

variable "name_suffix" {
  type        = string
  description = "Short random suffix for global uniqueness (Key Vault names are globally unique across Azure, max 24 chars)."
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "sku_name" {
  type    = string
  default = "standard"
}

variable "soft_delete_retention_days" {
  type        = number
  description = "Minimum allowed by Azure is 7. Kept at the minimum so this dev vault can be fully torn down and recreated without a long wait."
  default     = 7
}

variable "purge_protection_enabled" {
  type        = bool
  description = "Off for dev so the vault can be deleted outright during iteration; turn on before anything production-sensitive lands here (Chunk 10)."
  default     = false
}

variable "tags" {
  type    = map(string)
  default = {}
}
