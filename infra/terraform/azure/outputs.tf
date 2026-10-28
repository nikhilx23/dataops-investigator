output "postgres_fqdn" {
  value = azurerm_postgresql_flexible_server.pg.fqdn
}

output "api_url" {
  value = "https://${azurerm_container_app.api.ingress[0].fqdn}"
}
