# OPTIONAL sketch for running the lightweight profile on Azure. Never applied by this project.
# Creates paid resources if applied: review sizes and prices first.

resource "azurerm_resource_group" "rg" {
  name     = "${var.prefix}-rg"
  location = var.location
}

resource "random_password" "pg_admin" {
  length  = 32
  special = false
}

resource "azurerm_key_vault" "kv" {
  name                       = "${var.prefix}-kv"
  location                   = azurerm_resource_group.rg.location
  resource_group_name        = azurerm_resource_group.rg.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = true
  soft_delete_retention_days = 7
  rbac_authorization_enabled = true
}

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault_secret" "pg_admin" {
  name         = "pg-admin-password"
  value        = random_password.pg_admin.result
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_postgresql_flexible_server" "pg" {
  name                          = "${var.prefix}-pg"
  resource_group_name           = azurerm_resource_group.rg.name
  location                      = azurerm_resource_group.rg.location
  version                       = "16"
  sku_name                      = "B_Standard_B1ms"
  storage_mb                    = 32768
  administrator_login           = "dataops_admin"
  administrator_password        = random_password.pg_admin.result
  backup_retention_days         = 7
  public_network_access_enabled = true
  zone                          = "1"
}

resource "azurerm_postgresql_flexible_server_configuration" "extensions" {
  name      = "azure.extensions"
  server_id = azurerm_postgresql_flexible_server.pg.id
  value     = "VECTOR"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "client" {
  name             = "demo-client"
  server_id        = azurerm_postgresql_flexible_server.pg.id
  start_ip_address = var.allowed_client_ip
  end_ip_address   = var.allowed_client_ip
}

resource "azurerm_log_analytics_workspace" "logs" {
  name                = "${var.prefix}-logs"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

resource "azurerm_container_app_environment" "env" {
  name                       = "${var.prefix}-env"
  location                   = azurerm_resource_group.rg.location
  resource_group_name        = azurerm_resource_group.rg.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.logs.id
}

resource "azurerm_container_app" "api" {
  name                         = "${var.prefix}-api"
  container_app_environment_id = azurerm_container_app_environment.env.id
  resource_group_name          = azurerm_resource_group.rg.name
  revision_mode                = "Single"

  template {
    min_replicas = 0
    max_replicas = 1
    container {
      name    = "api"
      image   = var.app_image
      cpu     = 0.5
      memory  = "1Gi"
      command = ["uvicorn", "dataops.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
      env {
        name  = "DATAOPS_PG_HOST"
        value = azurerm_postgresql_flexible_server.pg.fqdn
      }
      # Role passwords and the API token are expected as Container App secrets sourced from
      # Key Vault; they are intentionally not defined here.
    }
  }

  ingress {
    external_enabled = true
    target_port      = 8000
    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }
}
