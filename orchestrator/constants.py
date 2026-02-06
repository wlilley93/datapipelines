from __future__ import annotations

import os

CONFIG_FILE = "config.json"
SECRETS_FILE = ".dlt/secrets.toml"
RUNS_DIR = "runs"
CONFIG_VERSION = 2

SCHEMA_SNAPSHOTS_DIR = os.path.join(".dlt", "schema_snapshots")

DOCKER_COMPOSE_TEMPLATE = """\
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "${POSTGRES_PORT}:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 15
    restart: unless-stopped
  adminer:
    image: adminer:latest
    ports:
      - "8000:8080"
    depends_on:
      - postgres
    restart: unless-stopped
  metabase:
    image: metabase/metabase:latest
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: postgres
      MB_DB_DBNAME: ${POSTGRES_DB}
      MB_DB_PORT: 5432
      MB_DB_USER: ${POSTGRES_USER}
      MB_DB_PASS: ${POSTGRES_PASSWORD}
      MB_DB_HOST: postgres
    depends_on:
      - postgres
    restart: unless-stopped

volumes:
  pgdata:
"""
