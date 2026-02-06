from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras


@dataclass(frozen=True)
class RunRow:
    id: int
    connection_name: str
    connection_type: str
    schema: str
    success: bool
    started_at: str
    finished_at: str
    duration_ms: int
    attempts: int
    mode: str
    progress_counts: Dict[str, Any]
    error: Optional[str]
    report: Optional[str]
    log_path: Optional[str]


def _connect(pg_creds: Dict[str, Any]):
    return psycopg2.connect(
        host=pg_creds["host"],
        port=int(pg_creds["port"]),
        dbname=pg_creds["database"],
        user=pg_creds["username"],
        password=pg_creds.get("password", ""),
        connect_timeout=int(pg_creds.get("connect_timeout", 5)),
    )


def ensure_runs_table(pg_creds: Dict[str, Any]) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS orchestrator_runs (
      id BIGSERIAL PRIMARY KEY,
      connection_name TEXT NOT NULL,
      connection_type TEXT NOT NULL,
      schema_name TEXT NOT NULL,

      success BOOLEAN NOT NULL,
      started_at TIMESTAMPTZ NOT NULL,
      finished_at TIMESTAMPTZ NOT NULL,
      duration_ms INTEGER NOT NULL,
      attempts INTEGER NOT NULL,

      mode TEXT NOT NULL, -- e.g. incremental|full|unknown

      progress_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
      state_before JSONB NOT NULL DEFAULT '{}'::jsonb,
      state_updates JSONB NOT NULL DEFAULT '{}'::jsonb,

      error TEXT NULL,
      report TEXT NULL,
      log_path TEXT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_conn_time
      ON orchestrator_runs (connection_name, started_at DESC);

    CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_success_time
      ON orchestrator_runs (success, started_at DESC);
    """
    conn = _connect(pg_creds)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def insert_run(pg_creds: Dict[str, Any], payload: Dict[str, Any]) -> int:
    """
    Insert a run row. Payload expects keys similar to CLI log_payload.
    Returns inserted run id.
    """
    ensure_runs_table(pg_creds)

    conn = _connect(pg_creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orchestrator_runs (
                  connection_name,
                  connection_type,
                  schema_name,
                  success,
                  started_at,
                  finished_at,
                  duration_ms,
                  attempts,
                  mode,
                  progress_counts,
                  state_before,
                  state_updates,
                  error,
                  report,
                  log_path
                )
                VALUES (
                  %(connection_name)s,
                  %(connection_type)s,
                  %(schema_name)s,
                  %(success)s,
                  %(started_at)s,
                  %(finished_at)s,
                  %(duration_ms)s,
                  %(attempts)s,
                  %(mode)s,
                  %(progress_counts)s::jsonb,
                  %(state_before)s::jsonb,
                  %(state_updates)s::jsonb,
                  %(error)s,
                  %(report)s,
                  %(log_path)s
                )
                RETURNING id;
                """,
                {
                    "connection_name": payload.get("connection_name", "unknown"),
                    "connection_type": payload.get("connection_type", "unknown"),
                    "schema_name": payload.get("schema", "unknown"),
                    "success": bool(payload.get("success", False)),
                    "started_at": payload.get("started_at"),
                    "finished_at": payload.get("finished_at"),
                    "duration_ms": int(payload.get("duration_ms") or 0),
                    "attempts": int(payload.get("attempts") or 0),
                    "mode": payload.get("mode") or "unknown",
                    "progress_counts": json.dumps(payload.get("progress_counts") or {}),
                    "state_before": json.dumps(payload.get("state_before") or {}),
                    "state_updates": json.dumps(payload.get("state_updates") or {}),
                    "error": payload.get("error"),
                    "report": payload.get("report"),
                    "log_path": payload.get("log_path"),
                },
            )
            rid = cur.fetchone()[0]
        conn.commit()
        return int(rid)
    finally:
        conn.close()


def fetch_runs(pg_creds: Dict[str, Any], *, limit: int = 25) -> List[RunRow]:
    ensure_runs_table(pg_creds)
    conn = _connect(pg_creds)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                  id,
                  connection_name,
                  connection_type,
                  schema_name,
                  success,
                  started_at,
                  finished_at,
                  duration_ms,
                  attempts,
                  mode,
                  progress_counts,
                  error,
                  report,
                  log_path
                FROM orchestrator_runs
                ORDER BY started_at DESC
                LIMIT %s;
                """,
                (int(limit),),
            )
            rows = cur.fetchall() or []
        out: List[RunRow] = []
        for r in rows:
            out.append(
                RunRow(
                    id=int(r["id"]),
                    connection_name=str(r["connection_name"]),
                    connection_type=str(r["connection_type"]),
                    schema=str(r["schema_name"]),
                    success=bool(r["success"]),
                    started_at=str(r["started_at"]),
                    finished_at=str(r["finished_at"]),
                    duration_ms=int(r["duration_ms"]),
                    attempts=int(r["attempts"]),
                    mode=str(r.get("mode") or "unknown"),
                    progress_counts=dict(r.get("progress_counts") or {}),
                    error=r.get("error"),
                    report=r.get("report"),
                    log_path=r.get("log_path"),
                )
            )
        return out
    finally:
        conn.close()


def fetch_last_failed(pg_creds: Dict[str, Any]) -> Optional[RunRow]:
    ensure_runs_table(pg_creds)
    conn = _connect(pg_creds)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                  id,
                  connection_name,
                  connection_type,
                  schema_name,
                  success,
                  started_at,
                  finished_at,
                  duration_ms,
                  attempts,
                  mode,
                  progress_counts,
                  error,
                  report,
                  log_path
                FROM orchestrator_runs
                WHERE success = FALSE
                ORDER BY started_at DESC
                LIMIT 1;
                """
            )
            r = cur.fetchone()
        if not r:
            return None
        return RunRow(
            id=int(r["id"]),
            connection_name=str(r["connection_name"]),
            connection_type=str(r["connection_type"]),
            schema=str(r["schema_name"]),
            success=bool(r["success"]),
            started_at=str(r["started_at"]),
            finished_at=str(r["finished_at"]),
            duration_ms=int(r["duration_ms"]),
            attempts=int(r["attempts"]),
            mode=str(r.get("mode") or "unknown"),
            progress_counts=dict(r.get("progress_counts") or {}),
            error=r.get("error"),
            report=r.get("report"),
            log_path=r.get("log_path"),
        )
    finally:
        conn.close()


def fetch_run_by_id(pg_creds: Dict[str, Any], run_id: int) -> Optional[Dict[str, Any]]:
    ensure_runs_table(pg_creds)
    conn = _connect(pg_creds)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM orchestrator_runs WHERE id = %s;", (int(run_id),))
            r = cur.fetchone()
        return dict(r) if r else None
    finally:
        conn.close()
