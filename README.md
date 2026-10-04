# ExecLedger

ExecLedger is a durable local execution control plane for developer tools and coding agents. It accepts command jobs through an HTTP API, gives every job an isolated working directory, persists state in SQLite, enforces idempotent submission, captures bounded output, records an append-only effect log, and snapshots workspace contents before and after execution.

The project is intended for local automation, agent/tool execution, CI-like workflows and reproducible debugging. **It is not a hostile-code security sandbox**; submitted processes run with the service user's OS permissions.

## Why this project

Agent workflows often fail in less obvious ways than “the command returned non-zero”: duplicate retries can execute twice, a crash can leave work marked `RUNNING`, workspace mutations are hard to audit, and callers cannot tell whether an operation was replayed or actually executed again. ExecLedger v0.1 establishes a small but testable control-plane contract around those problems.

## Implemented in v0.1

- REST API for submit/list/get/cancel.
- Atomic `Idempotency-Key` semantics backed by SQLite transactions.
- Explicit execution state machine: `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `TIMED_OUT`, `CANCELLED`, `INTERRUPTED`.
- Bounded async worker pool using argv execution (no shell interpolation).
- Per-job workspace with path traversal rejection and 0600 seeded files.
- Timeout and cancellation handling.
- Bounded stdout/stderr capture with truncation evidence.
- Append-only effect log for lifecycle events.
- Deterministic SHA-256 workspace snapshots before and after execution.
- Startup recovery: stale `RUNNING` executions become `INTERRUPTED` instead of being silently treated as successful.
- Unit/integration tests covering idempotency, state transitions, recovery, path safety, snapshots, success/failure/timeout/cancel and API behavior.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
execledger serve --root .state --port 8080
```

Submit a job:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/executions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d @examples/job.json
```

Inspect it with `GET /v1/executions/{id}`, and inspect audit evidence with:

```text
GET /v1/executions/{id}/effects
GET /v1/executions/{id}/snapshots
```

## Repository layout

```text
src/execledger/
  api.py        HTTP API
  cli.py        CLI/serve entry point
  models.py     request/state/evidence models
  runner.py     async process execution
  service.py    orchestration lifecycle
  store.py      durable SQLite state + idempotency
  workspace.py  path safety + snapshots

tests/          behavior and integration tests
docs/           architecture and explicit limits
examples/       runnable submission payload
```

## Design boundaries

ExecLedger does not pretend that a directory is a secure sandbox. For hostile/untrusted workloads, place the runner behind a real isolation boundary (container/VM/sandbox) and keep this service as the control plane.

The next milestone is deliberately non-trivial: durable worker leases and multi-process coordination, retry/backoff policy, incremental log streaming, snapshot restore, artifact retention/GC, and crash-consistent recovery. That work builds on this functioning v0.1 instead of being a from-scratch exercise.
