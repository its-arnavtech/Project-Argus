variable "name_prefix" {
  type        = string
  description = "Namespace name prefix, e.g. \"evhns-argus-dev\"."
}

variable "name_suffix" {
  type        = string
  description = "Short random suffix for global uniqueness (Event Hub namespace names are globally unique across Azure)."
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "sku" {
  type        = string
  description = "Basic, Standard, or Premium. PDD_Production_Guide.md section 2 specifies Premium (dedicated cluster) for the enterprise tier; dev defaults to Standard."
  default     = "Standard"
}

variable "capacity" {
  type        = number
  description = "Throughput units (Standard) / processing units (Premium)."
  default     = 1
}

variable "event_hub_name" {
  type    = string
  default = "transactions"
}

variable "partition_count" {
  type        = number
  description = "PDD_Production_Guide.md section 2 specifies 32 partitions for the enterprise tier; dev defaults to 2."
  default     = 2
}

variable "message_retention_days" {
  type        = number
  description = "PDD_Production_Guide.md section 2 specifies 7-day retention for the enterprise tier; dev defaults to 1 (retention is billed)."
  default     = 1
}

variable "tags" {
  type    = map(string)
  default = {}
}
