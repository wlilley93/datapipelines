# Data Pipelines Agent API Guide

This API is served by `server/agent_server.py` via FastAPI. All endpoints require an API key.

## Authentication

Set the environment variable on the server:

- `DATAPIPELINES_API_KEY` (required)

Send the key on every request:

- Header: `X-API-Key: <your-key>`

If the key is missing or invalid, the server returns `401 Unauthorized`.
If `DATAPIPELINES_API_KEY` is not set on the server, the server returns `500`.

## Base URL

When running via Docker Compose in this repo:

- `http://127.0.0.1:8081`

## Endpoints

### GET /connections

List all available pipeline connections.

Response: JSON array of connection objects.

Example:

```bash
curl -H "X-API-Key: <your-key>" http://127.0.0.1:8081/connections
```

Fields:
- `name` (string)
- `type` (string)
- `enabled` (boolean)
- `schema_name` (string)
- `last_run_at` (string or null)

---

### POST /connections/{name}/run

Run a synchronization for a specific connection (synchronous; request blocks until finished).

Path params:
- `name` (string): connection name

Query params:
- `include_report` (boolean, default: false)

Response:
- `success` (boolean)
- `duration_ms` (integer)
- `rows_inserted` (integer)
- `error` (string or null)
- `report` (string or null, only when `include_report=true`)
- `attempts` (integer or null)
- `records_reported` (integer or null)
- `progress_counts` (object or null)
- `run_id` (string or null)

Example:

```bash
curl -X POST \
  -H "X-API-Key: <your-key>" \
  "http://127.0.0.1:8081/connections/trello/run?include_report=true"
```

Errors:
- `404` if the connection does not exist
- `500` if the run fails unexpectedly

---

### POST /connections/{name}/run_async

Run a synchronization for a specific connection in the background. Returns immediately with a task id.

Path params:
- `name` (string): connection name

Response:
- `message` (string)
- `task_id` (string)
- `connection` (string)

Example:

```bash
curl -X POST \
  -H "X-API-Key: <your-key>" \
  http://127.0.0.1:8081/connections/trello/run_async
```

Use `/task_status/{task_id}` to check completion. For single runs, the status object includes:
- `kind: "single"`
- `connection`
- `result` (object or null, when finished)
- `error` (string or null, when finished)

---

### POST /run_selected

Start a background batch run for all enabled connections. Returns immediately with a task id.

Response:
- `message` (string)
- `task_id` (string)
- `connections` (array of connection names)

Example:

```bash
curl -X POST \
  -H "X-API-Key: <your-key>" \
  http://127.0.0.1:8081/run_selected
```

---

### POST /cancel_task/{task_id}

Cancel a running batch task.

Path params:
- `task_id` (string)

Response:
- `message` (string)

Example:

```bash
curl -X POST \
  -H "X-API-Key: <your-key>" \
  http://127.0.0.1:8081/cancel_task/<task-id>
```

Errors:
- `404` if the task is not found or already completed

---

### GET /task_status/{task_id}

Fetch the current status for a batch task.

Path params:
- `task_id` (string)

Response fields (example):
- `status` (string: queued|running|finished|cancelled)
- `connections` (array of connection names)
- `current` (string or null)
- `completed` (array of connection names)
- `canceled` (boolean)
- `started_at` (string)
- `finished_at` (string or null)
- `active` (boolean)
- `kind` (string: batch|single)
- `connection` (string, when kind=single)
- `result` (object or null, when kind=single)
- `error` (string or null, when kind=single)

Example:

```bash
curl -H "X-API-Key: <your-key>" http://127.0.0.1:8081/task_status/<task-id>
```

Errors:
- `404` if the task is not found

---

### GET /history

Return recent run history entries (DB-backed). If DB history is not configured, this can return an empty list.

Query params:
- `limit` (integer, default: 10)

Example:

```bash
curl -H "X-API-Key: <your-key>" http://127.0.0.1:8081/history?limit=25
```

---

### GET /pipelines

List all pipelines. Same shape as `/connections`.

Query params:
- `only_enabled` (boolean, default: false)

Example:

```bash
curl -H "X-API-Key: <your-key>" http://127.0.0.1:8081/pipelines?only_enabled=true
```

---

### GET /connections/{name}/status

Return the latest status for a pipeline connection. If DB history exists, it is used to populate the latest run fields.
If no DB history is available, it falls back to `last_run_at` from the connection metadata.

Path params:
- `name` (string)

Response fields:
- `name` (string)
- `type` (string)
- `enabled` (boolean)
- `schema_name` (string)
- `last_run_at` (string or null)
- `last_run_success` (boolean or null)
- `last_run_error` (string or null)
- `last_run_duration_ms` (integer or null)
- `last_run_attempts` (integer or null)

Example:

```bash
curl -H "X-API-Key: <your-key>" http://127.0.0.1:8081/connections/trello/status
```

## Notes

- `/connections/{name}/run` runs in-process and may take time depending on the pipeline.
- `/run_selected` is asynchronous; use `/task_status/{task_id}` for progress.
