# Architecture

ExecLedger is a local execution control plane for reproducible developer/agent jobs. It is deliberately not a security sandbox; commands run as the service user. Its guarantees focus on execution bookkeeping rather than hostile-code isolation.

## Components

- **HTTP API (`api.py`)**: submission, inspection, cancellation, effect-log and snapshot endpoints.
- **Execution service (`service.py`)**: lifecycle owner and boundary between API callers and the runner.
- **SQLite store (`store.py`)**: durable execution state, idempotency keys, effect logs and snapshot manifests.
- **Runner (`runner.py`)**: bounded worker pool using `asyncio.create_subprocess_exec` with no shell interpolation.
- **Workspace manager (`workspace.py`)**: per-execution workspaces, path validation, 0600 seeded files, deterministic content-hash snapshots.

## State machine

```text
QUEUED -> RUNNING -> SUCCEEDED
                  -> FAILED
                  -> TIMED_OUT
                  -> CANCELLED
                  -> INTERRUPTED
QUEUED ----------> CANCELLED
```

`RUNNING` rows found during service startup are marked `INTERRUPTED`. ExecLedger v0.1 intentionally does not automatically replay them because replay policy is workload-dependent.

## Idempotency

Clients submit an `Idempotency-Key`. The key is atomically bound to a canonical request hash. Repeating the same key with the same request returns the original execution. Reusing the key with different input is a conflict.

## Evidence model

Every execution keeps:

- append-only effect events,
- stdout/stderr (bounded by request policy),
- exit status,
- a `before` workspace snapshot,
- an `after` workspace snapshot.

Snapshot manifests contain relative path, size and SHA-256 for each regular file. The manifest itself also has a SHA-256 digest.

## Current limitations / next milestone

The v0.1 worker pool is single-process and polls SQLite. It does not yet implement durable worker leases, retry/backoff policy, SSE log streaming, snapshot restore, multi-process coordination, or artifact garbage collection. These are intentionally documented as the next engineering milestone rather than silently claiming distributed-execution semantics.
