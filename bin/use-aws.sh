#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT_DIR/infrastructure/.env.aws"

python - <<'PY'
import json
from pathlib import Path

cfg_path = Path("config.json")
if not cfg_path.exists():
    cfg = {"config_version": 2, "connections": [], "settings": {}}
else:
    cfg = json.loads(cfg_path.read_text())

settings = cfg.setdefault("settings", {})
settings["db_mode"] = "aws"
settings.setdefault("db_overrides", {})
settings["db_overrides"]["aws"] = {"host": "localhost", "port": 55432}

cfg_path.write_text(json.dumps(cfg, indent=2))
PY

if [ -f "$ENV_FILE" ]; then
  # Stop ssm-tunnel if running (CLI manages it now)
  docker compose -p datapipelines-aws rm -sf ssm-tunnel || true

  docker compose --env-file "$ENV_FILE" \
    -p datapipelines-aws \
    -f infrastructure/docker-compose.yml \
    -f infrastructure/docker-compose.aws.yml \
    up -d --build postgres adminer datapipelines-api
else
  echo "Missing $ENV_FILE. Create it or copy from infrastructure/.env.aws" >&2
  exit 1
fi

echo "AWS stack ready."
echo "Adminer: http://localhost:8002"
echo "API: http://localhost:8082"
echo "CLI target: localhost:55432 (AWS mode)"
