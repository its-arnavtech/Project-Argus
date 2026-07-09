# -----------------------------------------------------------------------------
# TIER SWITCH
#
# `tier` toggles every scale-sensitive setting in this environment between:
#   - "dev"        (default): budget-capped, scaled-down tier for this
#                   portfolio build (~$75 total remaining cloud budget).
#                   Standard Event Hubs, 2 partitions, 1-day retention,
#                   Cosmos free tier at 1000 RU/s (the free tier's max --
#                   still $0), single region, Consumption Container Apps.
#   - "enterprise": the literal bank-scale spec from
#                   docs/specs/PDD_Production_Guide.md section 2 -- Premium
#                   Event Hubs, 32 partitions, 7-day retention, Cosmos
#                   10,000-100,000 RU/s autopilot, multi-region writes.
#
# Switching `tier` to "enterprise" and re-running `terraform apply` covers
# Event Hubs SKU/partitions/retention and the Cosmos throughput number in one
# variable change. Multi-region writes and autopilot throughput mode are
# real module behavior changes (modules/cosmos_db is single-region/manual-
# throughput only as of Chunk 3) -- captured in cosmos_multi_region /
# cosmos_autopilot below for documentation, wired in whichever chunk actually
# needs "enterprise" tier live rather than built speculatively now. This is
# the "I designed it to run at bank-scale, here's the Terraform variable
# that proves it" story referenced in context.md's Architectural Decisions
# Log.
# -----------------------------------------------------------------------------
variable "tier" {
  type        = string
  description = "Deployment tier: \"dev\" (scaled-down, budget-capped) or \"enterprise\" (PDD literal spec)."
  default     = "dev"

  validation {
    condition     = contains(["dev", "enterprise"], var.tier)
    error_message = "tier must be \"dev\" or \"enterprise\"."
  }
}

locals {
  tier_config = {
    dev = {
      eventhub_sku             = "Standard"
      eventhub_capacity        = 1
      eventhub_partition_count = 2
      eventhub_retention_days  = 1
      cosmos_free_tier         = true
      cosmos_throughput        = 1000
      cosmos_multi_region      = false
      cosmos_autopilot         = false
    }
    enterprise = {
      eventhub_sku             = "Premium"
      eventhub_capacity        = 4
      eventhub_partition_count = 32
      eventhub_retention_days  = 7
      cosmos_free_tier         = false
      cosmos_throughput        = 10000
      cosmos_multi_region      = true
      cosmos_autopilot         = true
    }
  }

  cfg = local.tier_config[var.tier]

  common_tags = {
    project     = "project-argus"
    environment = var.tier
    managed_by  = "terraform"
  }
}

variable "subscription_id" {
  type        = string
  description = "Azure subscription to deploy into. Confirmed 2026-07-09 with the user: the ~$75 credit grant subscription ('Azure subscription 1')."
  default     = "REDACTED-SUBSCRIPTION-ID"
}

variable "location" {
  type        = string
  description = "Azure region for all resources. Locked to East US 2: Claude Opus 4.8 on Azure AI Foundry (needed in Chunk 8) is only Hosted-on-Azure in East US 2 / Sweden Central, and co-locating everything now avoids cross-region egress charges later."
  default     = "eastus2"
}

variable "alert_email" {
  type        = string
  description = "Email address for budget alert notifications (50%/75%/90% of budget)."
}

variable "budget_amount" {
  type    = number
  default = 75
}

variable "budget_start_date" {
  type        = string
  description = "ISO8601, must be the first of a month. Hardcoded rather than derived from timestamp() so terraform plan doesn't show perpetual drift on every run."
  default     = "2026-07-01T00:00:00Z"
}
