#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

python - <<'PY'
import json
from pathlib import Path

cfg_path = Path("config.json")
if not cfg_path.exists():
    cfg = {"config_version": 2, "connections": [], "settings": {}}
else:
    cfg = json.loads(cfg_path.read_text())

settings = cfg.setdefault("settings", {})
settings["db_mode"] = "local"

cfg_path.write_text(json.dumps(cfg, indent=2))
PY

docker compose -p datapipelines \
  -f infrastructure/docker-compose.yml \
  up -d

echo "Local stack ready."
echo "Adminer: http://localhost:8000"
echo "API: http://localhost:8081"
