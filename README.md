# ExecLedger

ExecLedger is a durable local execution control plane for developer tools, coding agents, and automation workflows. It accepts command jobs through an HTTP API, gives every job an isolated working directory, persists state in SQLite, enforces idempotent submission, captures bounded output, records an append-only effect log, and snapshots workspace contents before and after execution.

The project is intended for local automation, agent/tool execution, CI-like workflows and reproducible debugging. **It is not a hostile-code security sandbox**; submitted processes run with the service user's OS permissions.

## Why this project

Agent workflows fail in more ways than “the command returned non-zero”: duplicate retries can execute twice, a crash can leave work marked `RUNNING`, descendant processes can leak after a timeout, live output can disappear until a job ends, and workspace mutations are hard to audit.

ExecLedger provides a small execution contract around those failure modes: durable state, explicit lifecycle transitions, idempotency, process-tree ownership, incremental evidence, and reproducible workspace snapshots.

## Implemented in v0.2

- REST API for submit/list/get/cancel.
- Atomic `Idempotency-Key` semantics backed by SQLite transactions.
- Explicit execution state machine: `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `TIMED_OUT`, `CANCELLED`, `INTERRUPTED`.
- Bounded async worker pool using argv execution (no shell interpolation).
- Per-job workspace with path traversal rejection and 0600 seeded files.
- Timeout and cancellation handling.
- POSIX process-group ownership so cancellation, timeout and shutdown terminate descendant processes as well as the root process.
- Graceful shutdown marks interrupted running work as `INTERRUPTED` instead of misreporting it as a normal failure.
- Incremental stdout/stderr persistence through append-only `output_chunk` effects.
- Resumable Server-Sent Events endpoint for following lifecycle and output events.
- Bounded final stdout/stderr capture with truncation evidence.
- Deterministic SHA-256 workspace snapshots before and after execution.
- Startup recovery: stale `RUNNING` executions become `INTERRUPTED` instead of being silently treated as successful.
- Python HTTP client and operator CLI for submit/get/list/wait/cancel/logs/effects/snapshots/follow.
- Unit/integration tests and GitHub Actions on Python 3.11 and 3.12.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
execledger serve --root .state --port 8080
```

Submit a job with the CLI:

```bash
execledger submit examples/job.json --idempotency-key demo-1
```

Or submit over HTTP:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/executions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d @examples/job.json
```

Then inspect or follow it:

```bash
execledger list
execledger get EXECUTION_ID
execledger wait EXECUTION_ID
execledger logs EXECUTION_ID
execledger effects EXECUTION_ID
execledger snapshots EXECUTION_ID
execledger follow EXECUTION_ID --events
execledger cancel EXECUTION_ID
```

Set a non-default server once per shell:

```bash
export EXECLEDGER_URL=http://127.0.0.1:8080
```

The live SSE endpoint is:

```text
GET /v1/executions/{id}/events?after=<effect-seq>
```

Each event uses the durable effect sequence as its SSE `id`, so clients can reconnect from the last committed event they processed.

## Repository layout

```text
src/execledger/
  api.py        HTTP API + SSE stream
  client.py     synchronous Python HTTP client
  cli.py        server and operator CLI
  models.py     request/state/evidence models
  runner.py     async process execution + process-tree lifecycle
  service.py    orchestration lifecycle
  store.py      durable SQLite state + idempotency + effect log
  workspace.py  path safety + snapshots

tests/          behavior and integration tests
docs/           architecture and explicit limits
examples/       runnable submission payload
```

## Execution semantics

ExecLedger owns the process tree of a job. On POSIX systems every job starts in a new session/process group. Cancel, timeout, and service shutdown signal that process group, preventing background children from silently outliving the logical execution.

Output is persisted incrementally as append-only effects while a command runs. Final `stdout` and `stderr` fields remain bounded snapshots for convenient inspection; they are not the only source of output evidence.

## Design boundaries

ExecLedger does not pretend that a directory is a secure sandbox. For hostile or mutually untrusted workloads, place the runner behind a real isolation boundary such as a container, VM or dedicated sandbox and keep ExecLedger as the control plane.

Current workers still coordinate inside one service process. Durable cross-process worker leases, retry/backoff, restorable content-addressed snapshots, artifact retention/GC, and schema migrations are natural next reliability milestones.
