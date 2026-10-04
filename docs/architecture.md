# Architecture

ExecLedger is a local execution control plane for reproducible developer and agent jobs. It is deliberately not a security sandbox; commands run as the service user. Its guarantees focus on execution bookkeeping, process lifecycle and durable evidence rather than hostile-code isolation.

## Components

- **HTTP API (`api.py`)**: submission, inspection, cancellation, effect-log, snapshot and SSE event endpoints.
- **Python client (`client.py`)**: small synchronous API client used directly by applications and by the CLI.
- **Operator CLI (`cli.py`)**: serve, submit, list, get, wait, cancel, logs, effects, snapshots and live follow.
- **Execution service (`service.py`)**: lifecycle owner and boundary between API callers and the runner.
- **SQLite store (`store.py`)**: durable execution state, idempotency keys, append-only effect log and snapshot manifests.
- **Runner (`runner.py`)**: bounded async worker pool and subprocess/process-group lifecycle.
- **Workspace manager (`workspace.py`)**: per-execution workspaces, path validation, 0600 seeded files and deterministic content-hash snapshots.

## State machine

```text
QUEUED -> RUNNING -> SUCCEEDED
                  -> FAILED
                  -> TIMED_OUT
                  -> CANCELLED
                  -> INTERRUPTED
QUEUED ----------> CANCELLED
```

`RUNNING` rows found during service startup are marked `INTERRUPTED`. ExecLedger does not automatically replay them because replay policy is workload-dependent.

During an orderly service shutdown, already-started jobs are terminated and recorded as `INTERRUPTED`. Jobs that were never started remain `QUEUED`.

## Idempotency

Clients submit an `Idempotency-Key`. The key is atomically bound to a canonical request hash.

- Same key + same request returns the original logical execution.
- Same key + different request is rejected with a conflict.
- Worker retries or API retries must not create a second logical execution.

The mapping is durable because it is committed in SQLite together with the execution row.

## Process lifecycle

Each POSIX job starts with `start_new_session=True`, making the root process the leader of a dedicated process group.

ExecLedger signals the whole group for:

- explicit cancellation,
- timeout,
- service shutdown,
- cleanup of descendants left behind after the root exits.

This avoids the common failure mode where a shell, compiler, test worker or subprocess survives after the execution record has already become terminal.

The root process exit code remains the recorded execution exit code. Descendant cleanup is lifecycle cleanup, not a replacement for the root result.

## Effect log and live output

The `effects` table is append-only and uses a monotonically increasing SQLite sequence. Lifecycle events, output chunks, snapshots and terminal records all use this sequence.

While a process is running, stdout and stderr are drained concurrently in bounded chunks. Accepted chunks are stored as:

```json
{
  "kind": "output_chunk",
  "payload": {
    "stream": "stdout",
    "data": "...",
    "bytes": 123
  }
}
```

The same sequence drives the SSE endpoint:

```text
GET /v1/executions/{id}/events?after=<seq>
```

The server emits the effect sequence as the SSE `id`. Reconnecting with the last processed sequence therefore resumes from the next committed effect.

The runner still stores bounded final `stdout` and `stderr` strings on the execution record for convenient inspection. Once the configured byte bound is reached, later bytes are drained and discarded so a child cannot cause unbounded memory growth.

## Workspace snapshots

Every execution records a deterministic manifest before execution and another after execution. Each regular file contributes:

- relative path,
- byte size,
- SHA-256 content digest.

The complete sorted manifest also receives its own SHA-256 digest. Symlinks are excluded from snapshot traversal.

Current snapshots are evidence manifests, not backups: v0.2 does not yet persist file content in a content-addressed store.

## Failure and recovery semantics

- A missing executable becomes `FAILED`.
- A non-zero root exit becomes `FAILED`.
- Deadline expiration becomes `TIMED_OUT`.
- User-requested termination becomes `CANCELLED`.
- Service/startup interruption becomes `INTERRUPTED`.
- Output truncation is evidence attached to the execution, not a reason to change the terminal status.

On restart, stale persisted `RUNNING` rows are conservatively marked `INTERRUPTED`. Automatic retry is intentionally deferred until retry policy and durable worker ownership exist.

## Concurrency boundary

SQLite transactions serialize durable claim/idempotency mutations, but the current worker pool still lives inside one service process. In-memory task/process maps are used only for local lifecycle actions such as sending cancellation signals; they are not treated as durable ownership.

Cross-process workers therefore remain a future milestone and require persisted leases/fencing rather than a larger in-memory lock.

## Design boundaries

ExecLedger v0.2 does not provide:

- hostile-code sandboxing,
- privilege separation,
- multi-host consensus,
- durable multi-process worker leases,
- automatic retry/backoff,
- restorable snapshot bytes,
- artifact retention/garbage collection,
- versioned schema migrations.

Those are explicit boundaries so the repository does not overstate guarantees it has not implemented.
