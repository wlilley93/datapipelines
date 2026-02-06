from __future__ import annotations

import os
import sys
import threading
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path

# Ensure project root is in path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, Depends, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from orchestrator.config import load_config_v2, list_connections, get_connection
from orchestrator.run_connection import run_connection
from orchestrator.run_history_db import list_recent_runs, list_recent_runs_for_connection

# Global tracking of running tasks
running_tasks: Dict[str, threading.Event] = {}
running_task_status: Dict[str, Dict[str, Any]] = {}
tasks_lock = threading.Lock()

API_KEY_ENV = "DATAPIPELINES_API_KEY"

def _require_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")) -> None:
    expected_key = os.getenv(API_KEY_ENV)
    if not expected_key:
        raise HTTPException(status_code=500, detail=f"{API_KEY_ENV} is not set")
    if not x_api_key or x_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid API key")

app = FastAPI(
    title="Data Pipelines Agent Interface",
    description="API for LangGraph Agent Tool Use",
    dependencies=[Depends(_require_api_key)],
)


def _json_response(payload: Any) -> JSONResponse:
    """Return a JSON-safe response for agent tools."""
    return JSONResponse(content=jsonable_encoder(payload))


def _serialize_connection(conn: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize connection info to JSON-safe primitives."""
    return {
        "name": conn["name"],
        "type": conn["type"],
        "enabled": bool(conn.get("enabled", True)),
        "schema_name": conn.get("schema", conn["name"]),
        "last_run_at": conn.get("last_run_at"),
    }


class ConnectionInfo(BaseModel):
    name: str
    type: str
    enabled: bool
    schema_name: str
    last_run_at: Optional[str]

class RunResult(BaseModel):
    success: bool
    duration_ms: int
    rows_inserted: int
    error: Optional[str]
    report: Optional[str]
    attempts: Optional[int]
    records_reported: Optional[int]
    progress_counts: Optional[Dict[str, Any]]
    run_id: Optional[str]

class PipelineStatus(BaseModel):
    name: str
    type: str
    enabled: bool
    schema_name: str
    last_run_at: Optional[str]
    last_run_success: Optional[bool]
    last_run_error: Optional[str]
    last_run_duration_ms: Optional[int]
    last_run_attempts: Optional[int]


class BatchRunResult(BaseModel):
    message: str
    task_id: Optional[str] = None
    connections: List[str]

class SingleRunTaskResult(BaseModel):
    message: str
    task_id: Optional[str] = None
    connection: str

@app.get("/connections", response_model=List[ConnectionInfo])
def get_connections():
    """List all available connections."""
    cfg = load_config_v2()
    conns = list_connections(cfg)
    return _json_response([_serialize_connection(c) for c in conns])

@app.post("/connections/{name}/run", response_model=RunResult)
def run_sync(name: str, include_report: bool = Query(False)):
    """Run a synchronization for a specific connection."""
    cfg = load_config_v2()
    conn = get_connection(cfg, name)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{name}' not found")
    
    # Run synchronously to return result to agent
    # Agent tools usually expect a response
    try:
        # pause_on_exit=False so it doesn't block waiting for input
        result_payload = run_connection(cfg, conn, retries=1, pause_on_exit=False)
        
        if not result_payload:
            raise HTTPException(status_code=500, detail="Run failed to return a result payload")
            
        report_text = result_payload.get("report") if include_report else None
        return _json_response(
            RunResult(
                success=result_payload.get("success", False),
                duration_ms=result_payload.get("duration_ms", 0),
                rows_inserted=result_payload.get("rows_inserted", 0),
                error=result_payload.get("error"),
                report=report_text,
                attempts=result_payload.get("attempts"),
                records_reported=result_payload.get("progress_total_records"),
                progress_counts=result_payload.get("progress_counts"),
                run_id=result_payload.get("run_id"),
            ).dict()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/run_selected", response_model=BatchRunResult)
def run_selected_syncs(background_tasks: BackgroundTasks):
    """
    Trigger a background job to run all enabled connections (Batch Sync).
    Returns immediately with a status message.
    """
    cfg = load_config_v2()
    conns = [c for c in list_connections(cfg) if c.get("enabled", True)]

    if not conns:
        return _json_response({"message": "No enabled connections found to run.", "connections": []})

    # Create a unique task ID for this batch run
    task_id = f"batch_{threading.current_thread().ident}_{id(conns)}_{hash(tuple(c['name'] for c in conns)) % 10000}"

    # Create a cancellation event for this task
    cancel_event = threading.Event()

    with tasks_lock:
        running_tasks[task_id] = cancel_event
        running_task_status[task_id] = {
            "kind": "batch",
            "status": "queued",
            "connections": [c["name"] for c in conns],
            "current": None,
            "completed": [],
            "canceled": False,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
        }

    def _batch_runner():
        for conn in conns:
            try:
                with tasks_lock:
                    status = running_task_status.get(task_id, {})
                    status["status"] = "running"
                    status["current"] = conn["name"]
                    running_task_status[task_id] = status

                # Check if cancellation was requested before running each connection
                if cancel_event.is_set():
                    break

                # pause_on_exit=False, retries=2 (standard batch default)
                # Note: Currently run_connection doesn't support cancellation
                # but will be interrupted at the connector level if properly implemented
                run_connection(cfg, conn, retries=2, pause_on_exit=False)
            except Exception:
                # Check for cancellation after each connection attempt
                if cancel_event.is_set():
                    break
                pass # Continue to next connection
            finally:
                with tasks_lock:
                    status = running_task_status.get(task_id, {})
                    completed = status.get("completed", [])
                    completed.append(conn["name"])
                    status["completed"] = completed
                    status["current"] = None
                    running_task_status[task_id] = status
        # Remove the task from tracking once completed
        with tasks_lock:
            running_tasks.pop(task_id, None)
            status = running_task_status.get(task_id, {})
            status["status"] = "finished"
            status["finished_at"] = datetime.now(timezone.utc).isoformat()
            running_task_status[task_id] = status

    background_tasks.add_task(_batch_runner)
    return _json_response({
        "message": f"Started batch sync for {len(conns)} connections",
        "task_id": task_id,
        "connections": [c["name"] for c in conns]
    })

@app.post("/connections/{name}/run_async", response_model=SingleRunTaskResult)
def run_async(name: str, background_tasks: BackgroundTasks):
    """Run a synchronization for a specific connection in the background."""
    cfg = load_config_v2()
    conn = get_connection(cfg, name)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{name}' not found")

    task_id = f"run_{threading.current_thread().ident}_{id(conn)}_{hash(conn['name']) % 10000}"
    cancel_event = threading.Event()

    with tasks_lock:
        running_tasks[task_id] = cancel_event
        running_task_status[task_id] = {
            "kind": "single",
            "status": "queued",
            "connection": conn["name"],
            "canceled": False,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "result": None,
            "error": None,
        }

    def _run_single():
        try:
            with tasks_lock:
                status = running_task_status.get(task_id, {})
                status["status"] = "running"
                running_task_status[task_id] = status

            if cancel_event.is_set():
                return

            result_payload = run_connection(cfg, conn, retries=1, pause_on_exit=False)
            with tasks_lock:
                status = running_task_status.get(task_id, {})
                status["result"] = result_payload
                status["status"] = "finished"
                status["finished_at"] = datetime.now(timezone.utc).isoformat()
                running_task_status[task_id] = status
        except Exception as e:
            with tasks_lock:
                status = running_task_status.get(task_id, {})
                status["error"] = str(e)
                status["status"] = "finished"
                status["finished_at"] = datetime.now(timezone.utc).isoformat()
                running_task_status[task_id] = status
        finally:
            with tasks_lock:
                running_tasks.pop(task_id, None)

    background_tasks.add_task(_run_single)
    return _json_response({
        "message": f"Started sync for {conn['name']}",
        "task_id": task_id,
        "connection": conn["name"],
    })

@app.post("/cancel_task/{task_id}")
def cancel_task(task_id: str):
    """
    Cancel a running sync task by its task ID.
    """
    with tasks_lock:
        cancel_event = running_tasks.get(task_id)
        if cancel_event:
            cancel_event.set()  # Signal the task to stop
            status = running_task_status.get(task_id, {})
            status["canceled"] = True
            status["status"] = "cancelled"
            running_task_status[task_id] = status
            return _json_response({"message": f"Cancel signal sent to task {task_id}"})
        else:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found or already completed")


@app.get("/history", response_model=List[Dict[str, Any]])
def get_history(limit: int = 10):
    """Get recent run history."""
    try:
        runs = list_recent_runs(limit=limit)
        return _json_response(runs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/task_status/{task_id}", response_model=Dict[str, Any])
def get_task_status(task_id: str):
    """Get the current status of a background batch task."""
    with tasks_lock:
        status = running_task_status.get(task_id)
        if not status:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        # Include whether the task is still actively running
        status = dict(status)
        status["active"] = task_id in running_tasks
    return _json_response(status)


@app.get("/pipelines", response_model=List[Dict[str, Any]])
def get_pipelines(only_enabled: bool = False):
    """Get list of all pipeline connections."""
    try:
        cfg = load_config_v2()
        conns = list_connections(cfg)
        if only_enabled:
            conns = [c for c in conns if c.get("enabled", True)]
        return _json_response([_serialize_connection(c) for c in conns])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/connections/{name}/status", response_model=PipelineStatus)
def get_pipeline_status(name: str):
    """Get the latest status for a specific pipeline connection."""
    cfg = load_config_v2()
    conn = get_connection(cfg, name)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection '{name}' not found")

    last_run = list_recent_runs_for_connection(name, limit=1)
    last_run_payload = last_run[0] if last_run else {}

    return _json_response(
        PipelineStatus(
            name=conn["name"],
            type=conn["type"],
            enabled=bool(conn.get("enabled", True)),
            schema_name=conn.get("schema", conn["name"]),
            last_run_at=last_run_payload.get("finished_at") or conn.get("last_run_at"),
            last_run_success=last_run_payload.get("success"),
            last_run_error=last_run_payload.get("error"),
            last_run_duration_ms=last_run_payload.get("duration_ms"),
            last_run_attempts=last_run_payload.get("attempts"),
        ).dict()
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
