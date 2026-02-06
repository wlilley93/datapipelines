from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
import time
import random
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import load_config_v2, save_config_v2
from .constants import SECRETS_FILE
from .secrets import get_postgres_credentials

# Set up logging
logger = logging.getLogger(__name__)


def get_schema_hash_from_db(
    host: str, port: str, database: str, user: str, password: str
) -> str:
    """
    Generate a hash of the current database schema to detect changes.
    This includes tables, columns, constraints, etc.
    """
    logger.info(f"Getting schema hash from database {database} at {host}:{port}")
    conn = None
    cur = None
    try:
        # Use connection parameters directly instead of environment variables
        conn = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Query to get comprehensive schema information
        query = """
        SELECT
            table_schema,
            table_name,
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            datetime_precision
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name, ordinal_position;
        """

        cur.execute(query)
        schema_data = cur.fetchall()

        # Also get table constraints
        constraint_query = """
        SELECT
            tc.table_schema,
            tc.table_name,
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
        """

        cur.execute(constraint_query)
        constraint_data = cur.fetchall()

        # Get table definitions (CREATE statements) as well
        table_def_query = """
        SELECT
            schemaname,
            tablename,
            tableowner,
            hasindexes,
            hasrules,
            hastriggers
        FROM pg_tables
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog');
        """

        cur.execute(table_def_query)
        table_defs = cur.fetchall()

        # Combine all schema data
        all_schema_data = {
            "columns": [dict(row) for row in schema_data],
            "constraints": [dict(row) for row in constraint_data],
            "tables": [dict(row) for row in table_defs],
        }

        # Create a hash of the schema data
        schema_str = json.dumps(all_schema_data, sort_keys=True, default=str)
        schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()

        logger.info(f"Successfully generated schema hash for database {database}")
        return schema_hash

    except Exception as e:
        logger.error(
            f"Failed to get schema hash from database {database} at {host}:{port}: {str(e)}"
        )
        raise RuntimeError(f"Failed to get schema hash from database: {str(e)}")
    finally:
        # Ensure cleanup happens even if there's an exception
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_schema_sync_state(sync_name: str = "schema_sync") -> Dict[str, Any]:
    """
    Get the current schema sync state from a dedicated schema sync section in config.
    """
    config = load_config_v2()

    # Get schema sync states from a dedicated section in config
    schema_syncs = config.get("schema_syncs", {})
    state = schema_syncs.get(sync_name, {})

    if not state:
        state = {"version": 1, "global": {}, "streams": {}}

    return state


def update_schema_sync_state(sync_name: str, new_state_updates: Dict[str, Any]) -> None:
    """
    Update the schema sync state in a dedicated schema sync section in config.
    """
    config = load_config_v2()

    # Get existing schema sync states or initialize
    schema_syncs = config.get("schema_syncs", {})
    current_state = schema_syncs.get(
        sync_name, {"version": 1, "global": {}, "streams": {}}
    )

    # Merge the updates
    updated_state = _merge_state(current_state, new_state_updates)

    # Update the global last_run_at and last_success_at
    updated_state["global"]["last_run_at"] = datetime.now().isoformat() + "Z"
    if "error" not in new_state_updates.get("global", {}):
        updated_state["global"]["last_success_at"] = datetime.now().isoformat() + "Z"

    # Update the schema syncs section
    schema_syncs[sync_name] = updated_state
    config["schema_syncs"] = schema_syncs
    save_config_v2(config)


def _merge_state(current: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge state dictionaries.
    """
    result = current.copy()

    for key, value in updates.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_state(result[key], value)
        else:
            result[key] = value

    return result


def has_schema_changed(sync_name: str, current_hash: str) -> Tuple[bool, Optional[str]]:
    """
    Check if the schema has changed since the last sync.
    Returns (changed, previous_hash).
    """
    state = get_schema_sync_state(sync_name)
    streams = state.get("streams", {})

    # Look for schema_sync state in the streams
    schema_sync_state = streams.get("schema_sync", {})
    previous_hash = schema_sync_state.get("last_schema_hash")

    if previous_hash is None:
        # First time sync
        return True, None

    return current_hash != previous_hash, previous_hash


def get_detailed_schema_from_db(
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
    *,
    status_cb: Optional[Callable[[str], None]] = None,
    label: str = "",
) -> Dict[str, Any]:
    """
    Get detailed schema information from a database including tables, columns, constraints, etc.
    """
    # "localhost" may resolve to ::1 first on some machines; SSM port-forward typically binds IPv4 only.
    host = "127.0.0.1" if (host or "").strip().lower() == "localhost" else host
    prefix = f"{label}: " if label else ""
    logger.info(f"Getting detailed schema from database {database} at {host}:{port}")
    if status_cb:
        status_cb(f"{prefix}Connecting…")
    conn = None
    cur = None
    try:
        # Use connection parameters directly instead of environment variables
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Get all tables with their information
        if status_cb:
            status_cb(f"{prefix}Loading tables…")
        tables_query = """
        SELECT
            schemaname,
            tablename,
            tableowner,
            hasindexes,
            hasrules,
            hastriggers
        FROM pg_tables
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, tablename;
        """
        cur.execute(tables_query)
        tables = {
            f"{row['schemaname']}.{row['tablename']}": dict(row)
            for row in cur.fetchall()
        }
        logger.debug(f"Retrieved {len(tables)} tables from database {database}")

        # Get all columns with their details
        if status_cb:
            status_cb(f"{prefix}Loading columns…")
        columns_query = """
        SELECT
            table_schema,
            table_name,
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            datetime_precision,
            udt_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name, ordinal_position;
        """
        cur.execute(columns_query)
        columns = cur.fetchall()
        logger.debug(f"Retrieved {len(columns)} columns from database {database}")

        # Group columns by table
        table_columns = {}
        for col in columns:
            table_key = f"{col['table_schema']}.{col['table_name']}"
            if table_key not in table_columns:
                table_columns[table_key] = []
            table_columns[table_key].append(dict(col))

        # Get constraints
        if status_cb:
            status_cb(f"{prefix}Loading constraints…")
        constraints_query = """
        SELECT
            tc.table_schema,
            tc.table_name,
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
        """
        cur.execute(constraints_query)
        constraints = cur.fetchall()
        logger.debug(
            f"Retrieved {len(constraints)} constraints from database {database}"
        )

        # Group constraints by table
        table_constraints = {}
        for constraint in constraints:
            table_key = f"{constraint['table_schema']}.{constraint['table_name']}"
            if table_key not in table_constraints:
                table_constraints[table_key] = []
            table_constraints[table_key].append(dict(constraint))

        # Get indexes
        if status_cb:
            status_cb(f"{prefix}Loading indexes…")
        indexes_query = """
        SELECT
            schemaname,
            tablename,
            indexname,
            indexdef
        FROM pg_indexes
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, tablename, indexname;
        """
        cur.execute(indexes_query)
        indexes = cur.fetchall()
        logger.debug(f"Retrieved {len(indexes)} indexes from database {database}")

        # Group indexes by table
        table_indexes = {}
        for idx in indexes:
            table_key = f"{idx['schemaname']}.{idx['tablename']}"
            if table_key not in table_indexes:
                table_indexes[table_key] = []
            table_indexes[table_key].append(dict(idx))

        # Get primary keys
        if status_cb:
            status_cb(f"{prefix}Loading primary keys…")
        pks_query = """
        SELECT
            tc.table_schema,
            tc.table_name,
            tc.constraint_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position;
        """
        cur.execute(pks_query)
        primary_keys = cur.fetchall()
        logger.debug(
            f"Retrieved {len(primary_keys)} primary keys from database {database}"
        )

        # Group primary keys by table
        table_pks = {}
        for pk in primary_keys:
            table_key = f"{pk['table_schema']}.{pk['table_name']}"
            if table_key not in table_pks:
                table_pks[table_key] = []
            table_pks[table_key].append(dict(pk))

        # Get views
        if status_cb:
            status_cb(f"{prefix}Loading views…")
        views_query = """
        SELECT
            table_schema,
            table_name
        FROM information_schema.views
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name;
        """
        cur.execute(views_query)
        views = cur.fetchall()
        view_names = [f"{row['table_schema']}.{row['table_name']}" for row in views]
        logger.debug(f"Retrieved {len(views)} views from database {database}")

        # Get functions - use a more compatible query
        functions_query = """
        SELECT
            n.nspname AS schema_name,
            p.proname AS function_name
        FROM pg_proc p
        LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
        AND p.prokind = 'f'  -- Only functions, not procedures
        ORDER BY n.nspname, p.proname;
        """
        try:
            if status_cb:
                status_cb(f"{prefix}Loading functions…")
            cur.execute(functions_query)
            functions = cur.fetchall()
            function_defs = {
                f"{row['schema_name']}.{row['function_name']}": dict(row)
                for row in functions
            }
            logger.debug(
                f"Retrieved {len(functions)} functions from database {database}"
            )
        except Exception as e:
            logger.warning(
                f"Could not retrieve functions from database {database}: {str(e)}"
            )
            function_defs = {}
            functions = []

        # Get triggers
        if status_cb:
            status_cb(f"{prefix}Loading triggers…")
        triggers_query = """
        SELECT
            event_object_schema,
            event_object_table,
            trigger_name,
            action_timing,
            event_manipulation
        FROM information_schema.triggers
        ORDER BY event_object_schema, event_object_table, trigger_name;
        """
        cur.execute(triggers_query)
        triggers = cur.fetchall()
        logger.debug(f"Retrieved {len(triggers)} triggers from database {database}")

        # Group triggers by table
        table_triggers = {}
        for trigger in triggers:
            table_key = (
                f"{trigger['event_object_schema']}.{trigger['event_object_table']}"
            )
            if table_key not in table_triggers:
                table_triggers[table_key] = []
            table_triggers[table_key].append(dict(trigger))

        logger.info(f"Successfully retrieved detailed schema from database {database}")
        if status_cb:
            status_cb(f"{prefix}Schema loaded.")
        return {
            "tables": tables,
            "columns": table_columns,
            "constraints": table_constraints,
            "indexes": table_indexes,
            "primary_keys": table_pks,
            "views": view_names,
            "functions": function_defs,
            "triggers": table_triggers,
        }

    except Exception as e:
        logger.error(
            f"Failed to get detailed schema from database {database} at {host}:{port}: {str(e)}"
        )
        raise RuntimeError(f"Failed to get detailed schema from database: {str(e)}")
    finally:
        # Ensure cleanup happens even if there's an exception
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_schema_diff(
    source_host: str,
    source_port: str,
    source_db: str,
    source_user: str,
    source_password: str,
    dest_host: str,
    dest_port: str,
    dest_db: str,
    dest_user: str,
    dest_password: str,
    *,
    include_schemas: bool = False,
    status_cb: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Compare schemas between source and destination databases and return detailed differences.
    """
    try:
        # Get detailed schema from both databases
        source_schema = get_detailed_schema_from_db(
            source_host,
            source_port,
            source_db,
            source_user,
            source_password,
            status_cb=status_cb,
            label="Source",
        )
        dest_schema = get_detailed_schema_from_db(
            dest_host,
            dest_port,
            dest_db,
            dest_user,
            dest_password,
            status_cb=status_cb,
            label="Dest",
        )

        # Compare schemas and identify differences
        if status_cb:
            status_cb("Computing schema diff…")
        diff = {
            "new_tables": [],
            "dropped_tables": [],
            "modified_tables": [],
            "new_columns": {},
            "dropped_columns": {},
            "modified_columns": {},
            "new_constraints": {},
            "dropped_constraints": {},
            "new_indexes": {},
            "dropped_indexes": {},
            "new_views": [],
            "dropped_views": [],
            "new_functions": {},
            "dropped_functions": {},
            "new_triggers": {},
            "dropped_triggers": {},
        }

        # Find new and dropped tables
        source_tables = set(source_schema["tables"].keys())
        dest_tables = set(dest_schema["tables"].keys())

        diff["new_tables"] = list(source_tables - dest_tables)
        diff["dropped_tables"] = list(dest_tables - source_tables)

        # For tables that exist in both, check for column differences
        common_tables = source_tables & dest_tables
        for table in common_tables:
            # Compare columns
            source_cols = {
                col["column_name"]: col
                for col in source_schema["columns"].get(table, [])
            }
            dest_cols = {
                col["column_name"]: col for col in dest_schema["columns"].get(table, [])
            }

            source_col_names = set(source_cols.keys())
            dest_col_names = set(dest_cols.keys())

            new_cols = source_col_names - dest_col_names
            dropped_cols = dest_col_names - source_col_names
            common_cols = source_col_names & dest_col_names

            if new_cols:
                diff["new_columns"][table] = [source_cols[col] for col in new_cols]
            if dropped_cols:
                diff["dropped_columns"][table] = [
                    dest_cols[col] for col in dropped_cols
                ]

            # Check for modified columns (same name, different properties)
            for col_name in common_cols:
                src_col = source_cols[col_name]
                dest_col = dest_cols[col_name]

                # Compare key properties
                if (
                    src_col["data_type"] != dest_col["data_type"]
                    or src_col["is_nullable"] != dest_col["is_nullable"]
                    or src_col["character_maximum_length"]
                    != dest_col["character_maximum_length"]
                    or src_col["numeric_precision"] != dest_col["numeric_precision"]
                ):
                    if table not in diff["modified_columns"]:
                        diff["modified_columns"][table] = []
                    diff["modified_columns"][table].append(
                        {
                            "column_name": col_name,
                            "source": src_col,
                            "destination": dest_col,
                        }
                    )

        # Check for changes in constraints
        for table in common_tables:
            source_constraints = {
                c["constraint_name"]: c
                for c in source_schema["constraints"].get(table, [])
            }
            dest_constraints = {
                c["constraint_name"]: c
                for c in dest_schema["constraints"].get(table, [])
            }

            source_cons = set(source_constraints.keys())
            dest_cons = set(dest_constraints.keys())

            new_cons = source_cons - dest_cons
            dropped_cons = dest_cons - source_cons

            if new_cons:
                if table not in diff["new_constraints"]:
                    diff["new_constraints"][table] = []
                diff["new_constraints"][table] = [
                    source_constraints[con] for con in new_cons
                ]

            if dropped_cons:
                if table not in diff["dropped_constraints"]:
                    diff["dropped_constraints"][table] = []
                diff["dropped_constraints"][table] = [
                    dest_constraints[con] for con in dropped_cons
                ]

        # Check for changes in indexes
        for table in common_tables:
            source_indexes = {
                idx["indexname"]: idx for idx in source_schema["indexes"].get(table, [])
            }
            dest_indexes = {
                idx["indexname"]: idx for idx in dest_schema["indexes"].get(table, [])
            }

            source_idx_names = set(source_indexes.keys())
            dest_idx_names = set(dest_indexes.keys())

            new_indexes = source_idx_names - dest_idx_names
            dropped_indexes = dest_idx_names - source_idx_names

            if new_indexes:
                if table not in diff["new_indexes"]:
                    diff["new_indexes"][table] = []
                diff["new_indexes"][table] = [
                    source_indexes[idx] for idx in new_indexes
                ]

            if dropped_indexes:
                if table not in diff["dropped_indexes"]:
                    diff["dropped_indexes"][table] = []
                diff["dropped_indexes"][table] = [
                    dest_indexes[idx] for idx in dropped_indexes
                ]

        # Check for changes in views
        source_views = set(source_schema["views"])
        dest_views = set(dest_schema["views"])

        diff["new_views"] = list(source_views - dest_views)
        diff["dropped_views"] = list(dest_views - source_views)

        # Check for changes in functions
        source_functions = set(source_schema["functions"].keys())
        dest_functions = set(dest_schema["functions"].keys())

        diff["new_functions"] = {
            func: source_schema["functions"][func]
            for func in (source_functions - dest_functions)
        }
        diff["dropped_functions"] = {
            func: dest_schema["functions"][func]
            for func in (dest_functions - source_functions)
        }

        # Check for changes in triggers
        for table in common_tables:
            source_triggers = {
                t["trigger_name"]: t for t in source_schema["triggers"].get(table, [])
            }
            dest_triggers = {
                t["trigger_name"]: t for t in dest_schema["triggers"].get(table, [])
            }

            source_trig_names = set(source_triggers.keys())
            dest_trig_names = set(dest_triggers.keys())

            new_triggers = source_trig_names - dest_trig_names
            dropped_triggers = dest_trig_names - source_trig_names

            if new_triggers:
                if table not in diff["new_triggers"]:
                    diff["new_triggers"][table] = []
                diff["new_triggers"][table] = [
                    source_triggers[trig] for trig in new_triggers
                ]

            if dropped_triggers:
                if table not in diff["dropped_triggers"]:
                    diff["dropped_triggers"][table] = []
                diff["dropped_triggers"][table] = [
                    dest_triggers[trig] for trig in dropped_triggers
                ]

        # Determine if there are any changes
        has_changes = (
            diff["new_tables"]
            or diff["dropped_tables"]
            or diff["new_columns"]
            or diff["dropped_columns"]
            or diff["modified_columns"]
            or diff["new_constraints"]
            or diff["dropped_constraints"]
            or diff["new_indexes"]
            or diff["dropped_indexes"]
            or diff["new_views"]
            or diff["dropped_views"]
            or diff["new_functions"]
            or diff["dropped_functions"]
            or diff["new_triggers"]
            or diff["dropped_triggers"]
        )

        result: Dict[str, Any] = {
            "changed": has_changes,
            "diff": diff,
            "source_schema_summary": {
                "tables": len(source_schema["tables"]),
                "columns": sum(len(cols) for cols in source_schema["columns"].values()),
                "views": len(source_schema["views"]),
                "functions": len(source_schema["functions"]),
                "triggers": sum(
                    len(trigs) for trigs in source_schema["triggers"].values()
                ),
            },
            "dest_schema_summary": {
                "tables": len(dest_schema["tables"]),
                "columns": sum(len(cols) for cols in dest_schema["columns"].values()),
                "views": len(dest_schema["views"]),
                "functions": len(dest_schema["functions"]),
                "triggers": sum(
                    len(trigs) for trigs in dest_schema["triggers"].values()
                ),
            },
        }
        if include_schemas:
            result["source_schema"] = source_schema
            result["dest_schema"] = dest_schema
        return result

    except Exception as e:
        raise RuntimeError(f"Failed to compare schemas: {str(e)}")


def apply_schema_changes_incrementally(
    source_host: str,
    source_port: str,
    source_db: str,
    source_user: str,
    source_password: str,
    dest_host: str,
    dest_port: str,
    dest_db: str,
    dest_user: str,
    dest_password: str,
    diff_result: Dict[str, Any],
    status_cb: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Apply specific schema changes incrementally based on the diff result.
    This is a more sophisticated approach than full schema sync.
    """
    import subprocess
    import tempfile
    import os

    results = {
        "applied_changes": [],
        "failed_changes": [],
        "skipped_changes": [],
        "summary": {},
    }

    dest_conn = None
    dest_cur = None
    try:
        # Reuse a single destination connection for all DDL; creating a new connection per change is very slow.
        dest_conn = psycopg2.connect(
            host=dest_host,
            port=dest_port,
            dbname=dest_db,
            user=dest_user,
            password=dest_password,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
        )
        dest_cur = dest_conn.cursor()

        def exec_dest(sql: str, *, kind: str, name: str, action: str) -> None:
            try:
                dest_cur.execute(sql)
                dest_conn.commit()
                results["applied_changes"].append(
                    {"type": kind, "name": name, "action": action}
                )
            except Exception as e:
                dest_conn.rollback()
                results["failed_changes"].append(
                    {"type": kind, "name": name, "action": action, "error": str(e)}
                )

        # Apply new tables first
        new_tables = list(diff_result["diff"]["new_tables"])
        for i, table_name in enumerate(new_tables, start=1):
            if status_cb:
                status_cb(f"Creating tables ({i}/{len(new_tables)})…  {table_name}")
            # For new tables, we can use pg_dump to get the specific table definition
            dump_cmd = [
                "pg_dump",
                "--schema-only",
                "--no-owner",
                "--no-privileges",
                f"--table={table_name}",
                "-h",
                source_host,
                "-p",
                str(source_port),
                "-U",
                source_user,
                "-d",
                source_db,
            ]

            dump_env = os.environ.copy()
            dump_env["PGPASSWORD"] = source_password

            result = subprocess.run(
                dump_cmd,
                capture_output=True,
                text=True,
                env=dump_env,
                timeout=600,
            )

            if result.returncode != 0:
                results["failed_changes"].append(
                    {
                        "type": "table",
                        "name": table_name,
                        "action": "create",
                        "error": result.stderr,
                    }
                )
                continue

            # Apply the table creation to destination
            restore_cmd = [
                "psql",
                "-h",
                dest_host,
                "-p",
                str(dest_port),
                "-U",
                dest_user,
                "-d",
                dest_db,
            ]

            restore_env = os.environ.copy()
            restore_env["PGPASSWORD"] = dest_password

            restore_result = subprocess.run(
                restore_cmd,
                input=result.stdout,
                capture_output=True,
                text=True,
                env=restore_env,
                timeout=600,
            )

            if restore_result.returncode == 0:
                results["applied_changes"].append(
                    {"type": "table", "name": table_name, "action": "create"}
                )
            else:
                results["failed_changes"].append(
                    {
                        "type": "table",
                        "name": table_name,
                        "action": "create",
                        "error": restore_result.stderr,
                    }
                )

        # Apply new columns
        for table_name, columns in diff_result["diff"]["new_columns"].items():
            for col in columns:
                if status_cb:
                    status_cb(f"Adding column…  {table_name}.{col['column_name']}")
                alter_cmd = f'ALTER TABLE {table_name} ADD COLUMN "{col["column_name"]}" {col["data_type"]}'
                if col["is_nullable"] == "NO":
                    alter_cmd += " NOT NULL"

                if col["column_default"]:
                    alter_cmd += f" DEFAULT {col['column_default']}"

                exec_dest(
                    alter_cmd,
                    kind="column",
                    name=f"{table_name}.{col['column_name']}",
                    action="add",
                )

        # Apply modified columns (simplified - in practice, you'd need more complex logic)
        for table_name, columns in diff_result["diff"]["modified_columns"].items():
            for col_info in columns:
                col_name = col_info["column_name"]
                src_col = col_info["source"]
                if status_cb:
                    status_cb(
                        f"Skipping modification for column…  {table_name}.{col_name} (Overwrite disabled)"
                    )

                results["skipped_changes"].append(
                    {
                        "type": "column",
                        "name": f"{table_name}.{col_name}",
                        "action": "modify",
                        "reason": "Column modification (overwrite) is disabled to prevent data loss",
                    }
                )
                # alter_cmd = f"ALTER TABLE {table_name} ALTER COLUMN \"{col_name}\" TYPE {src_col['data_type']}"
                # exec_dest(alter_cmd, kind='column', name=f"{table_name}.{col_name}", action='modify')

        # Apply new constraints
        for table_name, constraints in diff_result["diff"]["new_constraints"].items():
            for constraint in constraints:
                # For simplicity, we'll skip complex constraint creation
                # In practice, you'd need to handle different constraint types
                results["skipped_changes"].append(
                    {
                        "type": "constraint",
                        "name": constraint["constraint_name"],
                        "action": "create",
                        "reason": "Complex constraint creation not implemented in this version",
                    }
                )

        # Apply new indexes
        for table_name, indexes in diff_result["diff"]["new_indexes"].items():
            for idx in indexes:
                if status_cb:
                    status_cb(f"Creating index…  {idx.get('indexname') or '(unknown)'}")
                exec_dest(
                    idx["indexdef"],
                    kind="index",
                    name=str(idx.get("indexname") or ""),
                    action="create",
                )

        # Apply new views
        new_views = list(diff_result["diff"]["new_views"])
        for i, view_name in enumerate(new_views, start=1):
            if status_cb:
                status_cb(f"Creating views ({i}/{len(new_views)})…  {view_name}")
            # Get the view definition from source
            dump_cmd = [
                "pg_dump",
                "--schema-only",
                "--no-owner",
                "--no-privileges",
                f"--table={view_name}",
                "-h",
                source_host,
                "-p",
                str(source_port),
                "-U",
                source_user,
                "-d",
                source_db,
            ]

            dump_env = os.environ.copy()
            dump_env["PGPASSWORD"] = source_password

            result = subprocess.run(
                dump_cmd,
                capture_output=True,
                text=True,
                env=dump_env,
                timeout=600,
            )

            if result.returncode != 0:
                results["failed_changes"].append(
                    {
                        "type": "view",
                        "name": view_name,
                        "action": "create",
                        "error": result.stderr,
                    }
                )
                continue

            # Apply the view creation to destination
            restore_cmd = [
                "psql",
                "-h",
                dest_host,
                "-p",
                str(dest_port),
                "-U",
                dest_user,
                "-d",
                dest_db,
            ]

            restore_env = os.environ.copy()
            restore_env["PGPASSWORD"] = dest_password

            restore_result = subprocess.run(
                restore_cmd,
                input=result.stdout,
                capture_output=True,
                text=True,
                env=restore_env,
                timeout=600,
            )

            if restore_result.returncode == 0:
                results["applied_changes"].append(
                    {"type": "view", "name": view_name, "action": "create"}
                )
            else:
                results["failed_changes"].append(
                    {
                        "type": "view",
                        "name": view_name,
                        "action": "create",
                        "error": restore_result.stderr,
                    }
                )

        # Apply new functions
        for func_name, func_def in diff_result["diff"]["new_functions"].items():
            # Since we changed from getting full definition to just source/args,
            # we'll skip function creation for now as it requires full function definition
            # that we can't easily reconstruct from source and args alone
            results["skipped_changes"].append(
                {
                    "type": "function",
                    "name": func_name,
                    "action": "create",
                    "reason": "Function definition reconstruction not implemented in this version",
                }
            )

        # Apply new triggers
        for table_name, triggers in diff_result["diff"]["new_triggers"].items():
            # For now, we'll skip trigger creation as it can be complex
            # In a real implementation, you'd need to get the trigger definition from the source
            for trigger in triggers:
                results["skipped_changes"].append(
                    {
                        "type": "trigger",
                        "name": trigger["trigger_name"],
                        "action": "create",
                        "reason": "Trigger creation not implemented in this version",
                    }
                )

        # Summarize results
        results["summary"] = {
            "applied": len(results["applied_changes"]),
            "failed": len(results["failed_changes"]),
            "skipped": len(results["skipped_changes"]),
            "total_changes": len(diff_result["diff"]["new_tables"])
            + sum(len(cols) for cols in diff_result["diff"]["new_columns"].values())
            + sum(
                len(cols) for cols in diff_result["diff"]["modified_columns"].values()
            )
            + sum(len(cons) for cons in diff_result["diff"]["new_constraints"].values())
            + sum(len(idxs) for idxs in diff_result["diff"]["new_indexes"].values())
            + len(diff_result["diff"]["new_views"])
            + len(diff_result["diff"]["new_functions"])
            + sum(len(trigs) for trigs in diff_result["diff"]["new_triggers"].values()),
        }

        return {
            "status": "success",
            "message": f"Applied incremental changes: {results['summary']['applied']} applied, {results['summary']['failed']} failed, {results['summary']['skipped']} skipped",
            "results": results,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to apply incremental changes: {str(e)}",
            "error": str(e),
        }
    finally:
        try:
            if dest_cur is not None:
                dest_cur.close()
        finally:
            if dest_conn is not None:
                dest_conn.close()


def validate_schema_changes(
    source_schema: Dict[str, Any],
    dest_schema: Dict[str, Any],
    diff_result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Validate schema changes before applying them to ensure they are safe.
    """
    validation_results = {"valid": True, "warnings": [], "errors": []}

    # Check for potentially dangerous operations
    for table_name in diff_result["diff"]["dropped_tables"]:
        validation_results["warnings"].append(
            f"Dropping table '{table_name}' may cause data loss"
        )

    for table_name, columns in diff_result["diff"]["dropped_columns"].items():
        for col in columns:
            validation_results["warnings"].append(
                f"Dropping column '{col['column_name']}' from table '{table_name}' may cause data loss"
            )

    # Check for column type changes that might cause issues
    for table_name, columns in diff_result["diff"]["modified_columns"].items():
        for col_info in columns:
            col_name = col_info["column_name"]
            src_col = col_info["source"]
            dest_col = col_info["destination"]

            # Check for potentially dangerous type changes
            if src_col["data_type"] != dest_col["data_type"]:
                validation_results["warnings"].append(
                    f"Changing column '{table_name}.{col_name}' from type '{dest_col['data_type']}' to '{src_col['data_type']}' may cause data conversion issues"
                )

    # Check for constraint additions that might fail due to existing data
    for table_name, constraints in diff_result["diff"]["new_constraints"].items():
        for constraint in constraints:
            if constraint["constraint_type"] == "CHECK":
                validation_results["warnings"].append(
                    f"Adding CHECK constraint '{constraint['constraint_name']}' to table '{table_name}' might fail if existing data doesn't satisfy the constraint"
                )
            elif constraint["constraint_type"] == "PRIMARY KEY":
                validation_results["warnings"].append(
                    f"Adding PRIMARY KEY constraint '{constraint['constraint_name']}' to table '{table_name}' might fail if existing data has duplicates or nulls"
                )
            elif constraint["constraint_type"] == "UNIQUE":
                validation_results["warnings"].append(
                    f"Adding UNIQUE constraint '{constraint['constraint_name']}' to table '{table_name}' might fail if existing data has duplicates"
                )

    # Determine if validation passed
    validation_results["valid"] = len(validation_results["errors"]) == 0

    return validation_results


def sync_schema_incremental(
    source_host: str,
    source_port: str,
    source_db: str,
    source_user: str,
    source_password: str,
    dest_host: str,
    dest_port: str,
    dest_db: str,
    dest_user: str,
    dest_password: str,
    sync_name: str = "schema_sync",
    max_retries: int = 3,
    base_backoff: float = 1.0,
    max_backoff: float = 60.0,
    use_incremental: bool = True,
    status_cb: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Perform an incremental schema sync with detailed change detection and application.
    This function can do either full sync or incremental sync based on use_incremental flag.
    """
    start_time = time.time()

    for attempt in range(max_retries + 1):  # +1 to include initial attempt
        try:
            if use_incremental:
                # Get detailed schema diff
                diff_result = get_schema_diff(
                    source_host,
                    source_port,
                    source_db,
                    source_user,
                    source_password,
                    dest_host,
                    dest_port,
                    dest_db,
                    dest_user,
                    dest_password,
                    include_schemas=True,
                    status_cb=status_cb,
                )

                if not diff_result["changed"]:
                    # No changes detected
                    return {
                        "status": "no_changes",
                        "message": "No schema changes detected since last sync",
                        "duration": time.time() - start_time,
                        "diff": diff_result,
                    }

                # Reuse the detailed schemas already fetched during diff to avoid duplicate introspection.
                source_schema = diff_result.get("source_schema")
                dest_schema = diff_result.get("dest_schema")
                if not isinstance(source_schema, dict) or not isinstance(
                    dest_schema, dict
                ):
                    source_schema = get_detailed_schema_from_db(
                        source_host,
                        source_port,
                        source_db,
                        source_user,
                        source_password,
                        status_cb=status_cb,
                        label="Source",
                    )
                    dest_schema = get_detailed_schema_from_db(
                        dest_host,
                        dest_port,
                        dest_db,
                        dest_user,
                        dest_password,
                        status_cb=status_cb,
                        label="Dest",
                    )

                # Validate the schema changes before applying
                if status_cb:
                    status_cb("Validating schema changes…")
                validation = validate_schema_changes(
                    source_schema, dest_schema, diff_result
                )

                if not validation["valid"]:
                    return {
                        "status": "error",
                        "message": f"Schema changes failed validation: {validation['errors']}",
                        "validation": validation,
                        "duration": time.time() - start_time,
                        "diff": diff_result,
                    }

                # Apply incremental changes
                if status_cb:
                    status_cb("Applying schema changes…")
                incremental_result = apply_schema_changes_incrementally(
                    source_host,
                    source_port,
                    source_db,
                    source_user,
                    source_password,
                    dest_host,
                    dest_port,
                    dest_db,
                    dest_user,
                    dest_password,
                    diff_result,
                    status_cb=status_cb,
                )

                if incremental_result["status"] == "success":
                    # Update state to reflect successful sync
                    current_source_hash = get_schema_hash_from_db(
                        source_host,
                        source_port,
                        source_db,
                        source_user,
                        source_password,
                    )

                    state_updates = {
                        "streams": {
                            "schema_sync": {
                                "last_schema_hash": current_source_hash,
                                "last_sync_at": datetime.now().isoformat() + "Z",
                                "sync_count": 1,  # In a real implementation, you'd increment this
                                "last_diff": diff_result,  # Store the last diff for reference
                                "validation_warnings": validation[
                                    "warnings"
                                ],  # Store warnings
                            }
                        },
                        "global": {
                            "last_run_at": datetime.now().isoformat() + "Z",
                            "last_success_at": datetime.now().isoformat() + "Z",
                            "attempts": attempt + 1,
                            "error": None,  # Clear any previous error
                            "incremental_sync": True,
                        },
                    }

                    update_schema_sync_state(sync_name, state_updates)

                    return {
                        "status": "success",
                        "message": f"Schema synced incrementally after {attempt + 1} attempt(s)",
                        "duration": time.time() - start_time,
                        "diff": diff_result,
                        "validation": validation,
                        "incremental_results": incremental_result["results"],
                        "attempts": attempt + 1,
                    }
                else:
                    # Incremental sync failed, fall back to full sync
                    print(
                        f"Warning: Incremental sync failed: {incremental_result['message']}"
                    )
                    print("Falling back to full schema sync...")
            else:
                # Use the old method for full sync
                diff_result = get_schema_diff(
                    source_host,
                    source_port,
                    source_db,
                    source_user,
                    source_password,
                    dest_host,
                    dest_port,
                    dest_db,
                    dest_user,
                    dest_password,
                )

                if not diff_result["changed"]:
                    # No changes detected
                    return {
                        "status": "no_changes",
                        "message": "No schema changes detected since last sync",
                        "duration": time.time() - start_time,
                        "diff": diff_result,
                    }

            # Full sync approach
            if status_cb:
                status_cb("Running pg_dump (schema-only)…")
            dump_cmd = [
                "pg_dump",
                "--schema-only",
                "--no-owner",
                "--no-privileges",
                "-h",
                source_host,
                "-p",
                str(source_port),
                "-U",
                source_user,
                "-d",
                source_db,
            ]

            restore_cmd = [
                "psql",
                "-h",
                dest_host,
                "-p",
                str(dest_port),
                "-U",
                dest_user,
                "-d",
                dest_db,
            ]

            # Run pg_dump to get schema SQL, using environment for password
            dump_env = os.environ.copy()
            dump_env["PGPASSWORD"] = source_password

            result = subprocess.run(
                dump_cmd,
                capture_output=True,
                text=True,
                env=dump_env,
                timeout=600,  # 10 minute timeout for full sync
            )

            if result.returncode != 0:
                raise RuntimeError(f"pg_dump failed: {result.stderr}")

            # Run psql to apply schema to destination, using environment for password
            if status_cb:
                status_cb("Applying schema via psql…")
            restore_env = os.environ.copy()
            restore_env["PGPASSWORD"] = dest_password

            restore_result = subprocess.run(
                restore_cmd,
                input=result.stdout,
                capture_output=True,
                text=True,
                env=restore_env,
                timeout=600,  # 10 minute timeout for full sync
            )

            if restore_result.returncode != 0:
                raise RuntimeError(f"psql failed: {restore_result.stderr}")

            # Update state to reflect successful sync
            current_source_hash = get_schema_hash_from_db(
                source_host, source_port, source_db, source_user, source_password
            )

            state_updates = {
                "streams": {
                    "schema_sync": {
                        "last_schema_hash": current_source_hash,
                        "last_sync_at": datetime.now().isoformat() + "Z",
                        "sync_count": 1,  # In a real implementation, you'd increment this
                        "last_diff": diff_result,  # Store the last diff for reference
                    }
                },
                "global": {
                    "last_run_at": datetime.now().isoformat() + "Z",
                    "last_success_at": datetime.now().isoformat() + "Z",
                    "attempts": attempt + 1,
                    "error": None,  # Clear any previous error
                    "incremental_sync": False,
                },
            }

            update_schema_sync_state(sync_name, state_updates)

            return {
                "status": "success",
                "message": f"Schema synced successfully after {attempt + 1} attempt(s)",
                "duration": time.time() - start_time,
                "diff": diff_result,
                "attempts": attempt + 1,
            }

        except Exception as e:
            if attempt < max_retries:
                # Update state with retry info
                retry_state_updates = {
                    "global": {
                        "error": str(e),
                        "last_error_at": datetime.now().isoformat() + "Z",
                        "last_run_at": datetime.now().isoformat() + "Z",
                        "attempts": attempt + 1,
                        "next_retry_at": datetime.now().isoformat() + "Z",
                    }
                }

                update_schema_sync_state(sync_name, retry_state_updates)

                # Calculate backoff time with jitter
                backoff_time = min(base_backoff * (2**attempt), max_backoff)
                jitter = random.uniform(0, 0.1 * backoff_time)  # 10% jitter
                actual_backoff = backoff_time + jitter

                time.sleep(actual_backoff)
                continue
            else:
                # All retries exhausted
                error_state_updates = {
                    "global": {
                        "error": str(e),
                        "last_error_at": datetime.now().isoformat() + "Z",
                        "last_run_at": datetime.now().isoformat() + "Z",
                        "attempts": attempt + 1,
                        "failed_after_retries": True,
                    }
                }

                update_schema_sync_state(sync_name, error_state_updates)

                return {
                    "status": "error",
                    "message": f"Schema sync failed after {max_retries + 1} attempts: {str(e)}",
                    "duration": time.time() - start_time,
                    "error": str(e),
                    "attempts": max_retries + 1,
                }


def get_schema_sync_report(sync_name: str = "schema_sync") -> Dict[str, Any]:
    """
    Generate a report about the schema sync status.
    """
    state = get_schema_sync_state(sync_name)
    schema_sync_state = state.get("streams", {}).get("schema_sync", {})
    global_state = state.get("global", {})

    report = {
        "sync_name": sync_name,
        "last_sync_at": schema_sync_state.get("last_sync_at"),
        "last_schema_hash": schema_sync_state.get("last_schema_hash"),
        "sync_count": schema_sync_state.get("sync_count", 0),
        "global": global_state,
        "streams": {"schema_sync": schema_sync_state},
        "status": "error" if global_state.get("error") else "success",
        "healthy": global_state.get("last_success_at") is not None
        and global_state.get("error") is None,
    }

    return report


def resume_schema_sync_if_needed(sync_name: str = "schema_sync") -> Dict[str, Any]:
    """
    Check if there's a failed sync that needs to be resumed and handle it.
    """
    state = get_schema_sync_state(sync_name)
    global_state = state.get("global", {})

    # Check if there was a failed sync
    if global_state.get("error") and global_state.get("failed_after_retries"):
        last_error_at = global_state.get("last_error_at")
        attempts = global_state.get("attempts", 0)

        return {
            "needs_resume": True,
            "last_error": global_state.get("error"),
            "attempts": attempts,
            "last_error_at": last_error_at,
            "message": f"Previous sync failed after {attempts} attempts at {last_error_at}",
        }

    return {"needs_resume": False, "message": "No failed sync to resume"}


def reset_schema_sync_state(sync_name: str = "schema_sync") -> Dict[str, Any]:
    """
    Reset the schema sync state to allow starting fresh.
    """
    try:
        # Create a clean state
        clean_state = {
            "version": 1,
            "global": {
                "last_run_at": datetime.now().isoformat() + "Z",
                "last_success_at": datetime.now().isoformat() + "Z",
                "error": None,
                "last_error_at": None,
                "attempts": 0,
                "failed_after_retries": False,
            },
            "streams": {
                "schema_sync": {
                    "last_schema_hash": None,
                    "last_sync_at": None,
                    "sync_count": 0,
                }
            },
        }

        update_schema_sync_state(sync_name, clean_state)

        return {
            "status": "success",
            "message": f"Schema sync state for '{sync_name}' has been reset",
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to reset schema sync state: {str(e)}",
        }
