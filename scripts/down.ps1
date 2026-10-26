# Stop containers. Data volumes are KEPT unless -DeleteVolumes is given (destructive).
param([switch]$DeleteVolumes)
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    if ($DeleteVolumes) { docker compose --profile full down --volumes }
    else { docker compose --profile full down }
} finally { Pop-Location }
