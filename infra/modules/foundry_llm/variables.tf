variable "account_name" {
  type        = string
  description = "Foundry (AIServices) account name; also its globally-unique custom subdomain."
}

variable "project_name" {
  type = string
}

variable "deployment_name" {
  type        = string
  description = "Deployment name -- this (not the model id) goes in the SDK's `model` parameter."
}

variable "resource_group_id" {
  type = string
}

variable "location" {
  type = string
}

variable "principal_id" {
  type        = string
  description = "Object ID granted Cognitive Services User for Entra-token inference calls."
}

variable "model_name" {
  type    = string
  default = "gpt-5-mini"
}

variable "model_version" {
  type    = string
  default = "2025-08-07"
}

variable "capacity" {
  type        = number
  description = "GlobalStandard capacity in thousands of tokens/min (50 = 50K TPM; this subscription's gpt-5-mini quota is 500)."
  default     = 50
}

variable "tags" {
  type    = map(string)
  default = {}
}
