# shellcheck shell=bash
# Sourced by other scripts: load .env without overriding variables already set in the
# environment (same precedence as dataops.config.load_dotenv), and refuse placeholders.
if [ ! -f "$ROOT/.env" ]; then
  echo "Missing $ROOT/.env. Run: python scripts/init_env.py" >&2
  exit 1
fi
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in ''|'#'*) continue ;; esac
  case "$line" in *=*) ;; *) continue ;; esac
  key="${line%%=*}"; value="${line#*=}"; value="${value%% #*}"
  value="${value%"${value##*[![:space:]]}"}"
  if [ "$value" = "CHANGE_ME" ]; then
    echo ".env still contains CHANGE_ME for $key. Run: python scripts/init_env.py --force" >&2
    exit 1
  fi
  if [ -z "${!key+x}" ]; then export "$key=$value"; fi
done < "$ROOT/.env"
