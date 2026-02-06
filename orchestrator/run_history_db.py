from __future__ import annotations

from typing import Any, Dict, List, Optional

from .secrets import get_postgres_credentials


def ensure_run_history_table() -> None:
    """
    Optional: a place to later store runs in Postgres.
    Not wired by default yet (safe no-op if psycopg2 missing or creds absent).
    """
    creds = get_postgres_credentials()
    if not creds:
        return
    try:
        import psycopg2  # type: ignore
    except Exception:
        return

    ddl = """
    CREATE TABLE IF NOT EXISTS orchestrator_run_history (
      id BIGSERIAL PRIMARY KEY,
      connection_name TEXT NOT NULL,
      connection_type TEXT NOT NULL,
      schema_name TEXT NOT NULL,
      success BOOLEAN NOT NULL,
      started_at TIMESTAMPTZ NOT NULL,
      finished_at TIMESTAMPTZ NOT NULL,
      duration_ms BIGINT NOT NULL,
      attempts INT NOT NULL,
      error TEXT NULL,
      progress_counts JSONB NULL,
      state_updates JSONB NULL,
      report TEXT NULL,
      raw JSONB NOT NULL
    );
    """

    conn = psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["database"],
        user=creds["username"],
        password=creds.get("password", ""),
        connect_timeout=int(creds.get("connect_timeout", 5)),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def write_run_history_row(payload: Dict[str, Any]) -> None:
    """
    Optional: call this from run_connection if you want DB persistence.
    """
    creds = get_postgres_credentials()
    if not creds:
        return
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore
    except Exception:
        return

    ensure_run_history_table()

    started_at = payload.get("started_at")
    finished_at = payload.get("finished_at")
    duration_ms = int(payload.get("duration_ms") or 0)

    conn = psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["database"],
        user=creds["username"],
        password=creds.get("password", ""),
        connect_timeout=int(creds.get("connect_timeout", 5)),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orchestrator_run_history
                (connection_name, connection_type, schema_name, success, started_at, finished_at, duration_ms,
                 attempts, error, progress_counts, state_updates, report, raw)
                VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    payload.get("connection_name"),
                    payload.get("connection_type"),
                    payload.get("schema"),
                    bool(payload.get("success")),
                    started_at,
                    finished_at,
                    duration_ms,
                    int(payload.get("attempts") or 0),
                    payload.get("error"),
                    psycopg2.extras.Json(payload.get("progress_counts") or {}),
                    psycopg2.extras.Json(payload.get("state_updates") or {}),
                    payload.get("report"),
                    psycopg2.extras.Json(payload),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def list_recent_runs(limit: int = 25) -> List[Dict[str, Any]]:
    creds = get_postgres_credentials()
    if not creds:
        return []
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore
    except Exception:
        return []

    ensure_run_history_table()

    conn = psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["database"],
        user=creds["username"],
        password=creds.get("password", ""),
        connect_timeout=int(creds.get("connect_timeout", 5)),
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, connection_name, connection_type, schema_name, success, started_at, finished_at,
                       duration_ms, attempts, error, progress_counts
                FROM orchestrator_run_history
                ORDER BY id DESC
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = cur.fetchall() or []
            return [dict(r) for r in rows]
    finally:
        conn.close()


def list_recent_runs_for_connection(connection_name: str, limit: int = 10) -> List[Dict[str, Any]]:
    creds = get_postgres_credentials()
    if not creds:
        return []
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore
    except Exception:
        return []

    ensure_run_history_table()

    conn = psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["database"],
        user=creds["username"],
        password=creds.get("password", ""),
        connect_timeout=int(creds.get("connect_timeout", 5)),
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, connection_name, connection_type, schema_name, success, started_at, finished_at,
                       duration_ms, attempts, error, progress_counts
                FROM orchestrator_run_history
                WHERE connection_name = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (connection_name, int(limit)),
            )
            rows = cur.fetchall() or []
            return [dict(r) for r in rows]
    finally:
        conn.close()
