# Dot-sourced by other scripts: load .env without overriding variables already set in the
# environment (same precedence as dataops.config.load_dotenv), and refuse placeholders.
$ErrorActionPreference = 'Stop'
$script:Root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $script:Root '.env'
if (-not (Test-Path $envFile)) { throw "Missing $envFile. Run: python scripts/init_env.py" }
foreach ($line in Get-Content $envFile) {
    $t = $line.Trim()
    if ($t -eq '' -or $t.StartsWith('#') -or -not $t.Contains('=')) { continue }
    $key, $value = $t.Split('=', 2)
    $value = ($value -split ' #', 2)[0].Trim()
    if ($value -eq 'CHANGE_ME') { throw ".env still contains CHANGE_ME for $key. Run: python scripts/init_env.py --force" }
    $key = $key.Trim()
    if ($null -eq [Environment]::GetEnvironmentVariable($key, 'Process')) {
        [Environment]::SetEnvironmentVariable($key, $value, 'Process')
    }
}
