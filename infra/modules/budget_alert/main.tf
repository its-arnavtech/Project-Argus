resource "azurerm_consumption_budget_resource_group" "this" {
  name              = "${var.name_prefix}-budget"
  resource_group_id = var.resource_group_id

  amount     = var.amount
  time_grain = "Monthly"

  time_period {
    start_date = var.start_date
  }

  notification {
    enabled        = true
    threshold      = 50
    operator       = "GreaterThanOrEqualTo"
    threshold_type = "Actual"
    contact_emails = [var.alert_email]
  }

  notification {
    enabled        = true
    threshold      = 75
    operator       = "GreaterThanOrEqualTo"
    threshold_type = "Actual"
    contact_emails = [var.alert_email]
  }

  notification {
    enabled        = true
    threshold      = 90
    operator       = "GreaterThanOrEqualTo"
    threshold_type = "Actual"
    contact_emails = [var.alert_email]
  }
}
