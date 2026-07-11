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

# -----------------------------------------------------------------------------
# CHUNK 11 LOAD-TEST OVERRIDES
#
# Narrow, single-purpose, fully reversible overrides for the load test --
# deliberately NOT a tier switch to "enterprise" (that would also change
# Cosmos throughput/multi-region/autopilot, none of which this test needs).
# Null = use the tier default. Set via -var or a gitignored .auto.tfvars for
# the test window, then unset back to null to revert (Step 6).
#
# partition_count has NO override here on purpose: Standard tier partitions
# are immutable (Microsoft Learn FAQ, confirmed 2026-07-12) -- there is no
# Terraform variable that could express a reversible change even if one were
# wanted, so none is offered.
# -----------------------------------------------------------------------------
variable "load_test_eventhub_capacity" {
  type        = number
  default     = null
  description = "Temporary Event Hubs namespace TU override for the Chunk 11 load test. Null = tier default (1 TU)."
}

variable "load_test_max_replicas" {
  type        = number
  default     = null
  description = "Temporary argus-ingestion max_replicas override for the Chunk 11 load test. Null = baseline (1)."
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
  description = "Azure subscription to deploy into (the ~$75 credit grant subscription, 'Azure subscription 1'). No default -- set via TF_VAR_subscription_id or the gitignored terraform.tfvars so it never lands in git history."
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
