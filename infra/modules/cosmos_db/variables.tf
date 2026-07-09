variable "name_prefix" {
  type = string
}

variable "name_suffix" {
  type        = string
  description = "Short random suffix for global uniqueness (Cosmos DB account names are globally unique across Azure)."
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "free_tier_enabled" {
  type        = bool
  description = "Cosmos DB free tier: first 1000 RU/s + 25GB storage at no cost, one free-tier account per subscription."
  default     = true
}

variable "consistency_level" {
  type    = string
  default = "Session"
}

variable "database_name" {
  type    = string
  default = "argus-graph"
}

variable "throughput" {
  type        = number
  description = "Gremlin database (shared) RU/s -- manual, increments of 100, minimum 400. Free tier covers up to 1000 RU/s + 25GB at $0; default is set to that max since no container/graph exists yet to split it with (Chunk 5). PDD_Production_Guide.md section 2 specifies 10,000-100,000 RU/s autopilot for the enterprise tier."
  default     = 1000
}

variable "tags" {
  type    = map(string)
  default = {}
}
