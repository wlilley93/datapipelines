# CLI Orchestrator

This document is the operational guide to the manual CLI entrypoint. The FastAPI backend runs the same orchestration code headlessly, so if you understand the CLI you understand how any run starts, where it writes artifacts, and how credentials/state are managed.

## Quick start

1. Install dependencies

   ```bash
   pip install -r dependencies/requirements.txt
   ```

2. Ensure required files/dirs exist

   - `config.json` (versioned config; includes `connections`)
   - `.dlt/secrets.toml` (destination + connector secrets)
   - Writable dirs: `runs/`, `run_logs/`, `backups/`

3. Start the CLI

   ```bash
   python -m cli
   ```

## Mental model

There are three durable surfaces. Everything else is derived.

- `config.json`: connection definitions + per-connection state
- `.dlt/secrets.toml`: destination credentials + connector secrets/tokens
- `run_logs/` and `runs/`: immutable artifacts of a particular run

The CLI is opinionated about keeping those surfaces consistent:

- edit connection settings in the CLI -> `config.json` changes
- enter credentials in the CLI -> `.dlt/secrets.toml` changes
- run a connector -> writes `run_logs/...events.jsonl` + `runs/...summary.json`, then merges updated state into `config.json`

## Entry points

- Interactive CLI: `python -m cli`
- Headless runner (used by backend): `python -m webapp.backend.cli_runner` (invoked by the API layer)

Both paths reuse the same runner + connector runtime plumbing.

## Key modules (where to look)

| Module | What it owns |
| --- | --- |
| `orchestrator/app.py` | The interactive menu flow (Rich UI), calls into config/secrets/runner utilities |
| `orchestrator/run_connection.py` | The core execution pipeline: load connector, resolve secrets, stream events, persist artifacts, merge state |
| `orchestrator/config.py` | Reads/writes `config.json`, schema sanitization, and safe updates |
| `orchestrator/secrets.py` | Reads/writes `.dlt/secrets.toml` with locking to avoid concurrent corruption |
| `orchestrator/postgres_ops.py` | Destination bootstrapping and connectivity tests (and helper flows used by Settings/Database UI) |
| `connectors/*` | Individual connector implementations; expected surface is `check()` and `read()` (legacy adapters live here too) |

## Running a connection (what actually happens)

When you select a connection in the CLI, the orchestrator runs roughly:

1. Load config + the selected connection entry
2. Resolve secrets for the connector and destination
3. Call `run_connection(cfg, conn, retries=..., backoff=...)`
4. Stream runtime events (UI + file) while the connector runs
5. Write artifacts
6. Merge updated connector state back into `config.json`

Artifacts written per run:

- Runtime event stream (JSONL): `run_logs/YYYY-MM-DD/<connection>_<run_id>_events.jsonl`
- Run summary (JSON): `runs/YYYY-MM-DD/<connection>_<timestamp>.json`

The summary typically includes `report`, `state`, `stats`, and `progress_counts`.

## Configuration

### `config.json`

`config.json` is the source of truth for which connections exist and whether they are enabled.

Common fields you will see on a connection entry (exact shape evolves):

- `name`: human-friendly identifier (used in paths and UI)
- `type`: connector type key (used by the loader)
- `enabled`: whether it is eligible to run
- `schema`: connector-specific configuration and resource selection
- `state`: incremental sync state (high watermarks, cursor positions, etc.)

Operational note: connector state is not stored in the run logs; it is merged into `config.json` after a successful run (and sometimes partially on failure depending on connector semantics).

### Environment overrides

Most path-like and destination defaults can be overridden via env vars (shell or `.env`), for example:

```bash
ORCH_CONFIG_PATH=./config.json
RUNS_DIR=./runs
RUN_LOGS_DIR=./run_logs
```

Keep destination values in one place if possible (see next section), and use env overrides primarily for local development and CI.

## Secrets and destinations

### `.dlt/secrets.toml`

The CLI stores destination and source secrets in `.dlt/secrets.toml`.

Example (values are placeholders):

```toml
[destination.postgres.credentials]
host = "localhost"
port = 5434
database = "dlt_db"
username = "dlt_user"
password = "REDACTED"

[sources.trello]
api_key = "REDACTED"
token = "REDACTED"
```

Rules of thumb:

- Do not commit real secrets. Treat any value in `.dlt/secrets.toml` as sensitive.
- Keep Compose/app env (`infrastructure/.env`) consistent with `.dlt/secrets.toml` so the CLI and backend talk to the same Postgres.
- If you reconfigure the destination in the CLI, it should write back into `.dlt/secrets.toml` via `orchestrator/secrets.py`.

## Logs, observability, and debugging

### Where to look first

- `run_logs/..._events.jsonl`: canonical event stream (runtime messages, connector progress, request traces where enabled)
- `runs/...json`: end-of-run summary (what succeeded, what failed, state updates)

### Optional run history table

If destination credentials permit it, the orchestrator can persist run history into Postgres (via `orchestrator/run_history_db.py`). This is useful for historical queries and powering UI timelines.

## Backend alignment (FastAPI)

The backend shells out to the headless runner so it can reuse connector loading, secret resolution, event emission, and state merge behavior.

Typical behavior in the backend runner:

- Load connection config from `config.json`
- Run the same connector runtime stack (without Rich UI)
- Emit a final JSON payload (e.g., `report_text`, `state_updates`, `exit_code`) so the API can update its own tables (e.g. `sync_runs`)

If a run works in the CLI but fails via the API, diff:

- env vars (paths, destination host/port)
- `.dlt/secrets.toml` contents and permissions
- working directory and relative paths

## Common workflows

### Add or edit a connection

Use the CLI flows rather than hand-editing when possible. They are designed to:

- keep `config.json` schema-valid
- avoid clobbering other connections
- prompt for missing secrets and store them safely

If you must hand-edit `config.json`, re-open the CLI afterward to validate that the entry loads and renders.

### Re-run after a failure

- Inspect `run_logs/...events.jsonl` for the first error event and any request traces.
- If the failure is auth-related, fix `.dlt/secrets.toml` and re-run.
- If the failure is schema/state-related, inspect the connection's `state` in `config.json` (and consider backing it up before editing).

## Troubleshooting

### Postgres auth errors

If you see `password authentication failed` or connection refused errors:

- Verify `.dlt/secrets.toml` destination credentials match the running database.
- Verify Docker port bindings match what the CLI is trying to connect to (e.g., host port 5434 -> container 5432).
- Check container health (`pg_isready` inside the container is the quickest signal).

### Connector failures

- Use `run_logs/...events.jsonl` to find the first failing step.
- Look for `http.request.*` and `runtime.event` messages for context.
- Confirm the connection is still `enabled` and that the connector type matches what the loader expects.

## AWS RDS via SSM tunnel

Use the SSM tunnel container when the destination is AWS RDS instead of local Postgres.

Typical setup:

1. Ensure AWS credentials exist under `~/.aws` (profile configured)
2. Set required env vars (often in `infrastructure/.env`), for example:
   - `AWS_REGION=eu-west-1`
   - `SSM_TARGET=i-...`
   - `SSM_HOST=your-rds-hostname`
3. Start Compose with the AWS override:

   ```bash
   docker compose -p datapipelines \
     -f infrastructure/docker-compose.yml \
     -f infrastructure/docker-compose.aws.yml \
     up -d --build
   ```

4. Update `.dlt/secrets.toml` destination credentials for the RDS user/password/db as needed

Operational notes:

- Prefer `127.0.0.1` over `localhost` for tunnel endpoints to avoid IPv6 `::1` surprises.
- Pick a local port that does not clash with your local Postgres (e.g., 55432).

### Security hardening

- Use a dedicated AWS profile with minimal permissions for `ssm:StartSession`.
- Prefer short-lived credentials (SSO / assumed role) over long-lived access keys.
- Keep the tunnel container isolated (no published ports unless required by your workflow).

Example least-privilege policy (adjust for your account/instances):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SSMSession",
      "Effect": "Allow",
      "Action": [
        "ssm:StartSession",
        "ssm:TerminateSession",
        "ssm:DescribeSessions",
        "ssm:GetConnectionStatus"
      ],
      "Resource": [
        "arn:aws:ec2:*:*:instance/*",
        "arn:aws:ssm:*:*:document/AWS-StartPortForwardingSessionToRemoteHost"
      ]
    },
    {
      "Sid": "SSMInstanceInfo",
      "Effect": "Allow",
      "Action": [
        "ssm:DescribeInstanceInformation"
      ],
      "Resource": "*"
    }
  ]
}
```

## Minimal reproducible run checklist

When debugging a run (CLI or API), capture these facts:

- exact connection name + type from `config.json`
- destination host/port/database (from `.dlt/secrets.toml`)
- the run's event log path under `run_logs/`
- the run's summary path under `runs/`

With those, another engineer can reproduce the run and see the same artifacts without guessing.
