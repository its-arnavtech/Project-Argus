variable "name_prefix" {
  type = string
}

variable "resource_group_id" {
  type = string
}

variable "amount" {
  type        = number
  description = "Monthly budget amount in the subscription's billing currency."
  default     = 75
}

variable "alert_email" {
  type        = string
  description = "Email address for 50%/75%/90% threshold notifications."
}

variable "start_date" {
  type        = string
  description = "ISO8601, must be the first of a month. Passed in as a variable (not derived from timestamp()) so terraform plan doesn't show perpetual drift on every run -- Azure also rejects changing an existing budget's start_date after creation."
}
