# Optional Azure sketch (Terraform)

**Status: written, not validated, never applied.** The Terraform CLI was not available in the
build environment, so `terraform validate` has not been run; nothing has been deployed and no
cost has been incurred.

Applying this creates paid resources (PostgreSQL Flexible Server, Container Apps, Log
Analytics, Key Vault). Check prices for your region first. Kafka/Spark/Airflow are not
included: the cloud sketch runs the lightweight batch path.

```bash
terraform init
terraform validate
terraform plan -var subscription_id=... -var app_image=... -var allowed_client_ip=...
```

Gaps before real use: private networking instead of a public PostgreSQL firewall rule, Key
Vault references for every role password, an identity for the Container App, and the dashboard
container.
