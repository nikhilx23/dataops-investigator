variable "subscription_id" {
  type        = string
  description = "Azure subscription to deploy into (no default on purpose)."
}

variable "location" {
  type    = string
  default = "eastus"
}

variable "prefix" {
  type    = string
  default = "dataopsinv"
}

variable "app_image" {
  type        = string
  description = "Container image for the API and dashboard, e.g. ghcr.io/<owner>/dataops-investigator:0.5.0"
}

variable "allowed_client_ip" {
  type        = string
  description = "Single public IP allowed to reach PostgreSQL (portfolio demo, not production)."
}
