# Architecture

ExecLedger is a local execution control plane for reproducible developer and agent jobs. Commands run with the service user's OS permissions; the project focuses on durable execution semantics, worker ownership, process lifecycle and evidence rather than hostile-code isolation.

## Components

- **HTTP API (`api.py`)**: submission, inspection, cancellation, attempt/effect/snapshot reads, and SSE events.
- **Python client (`client.py`)**: small synchronous client used directly and by the CLI.
- **Operator CLI (`cli.py`)**: server and job operations.
- **Execution service (`service.py`)**: lifecycle boundary around store, workspace manager and runner.
- **SQLite store (`store.py`)**: durable executions, idempotency, leases, attempts, effects and snapshots.
- **Runner (`runner.py`)**: async worker slots, lease heartbeats and subprocess/process-group lifecycle.
- **Workspace manager (`workspace.py`)**: per-execution files, path validation and deterministic snapshots.

## State machine

```text
QUEUED -> RUNNING -> SUCCEEDED
                  -> FAILED
                  -> TIMED_OUT
                  -> CANCELLED
                  -> INTERRUPTED
QUEUED ----------> CANCELLED
```

A transition into `RUNNING` is now tied to a durable lease and creates an attempt record.

## Idempotency

Submission binds an `Idempotency-Key` to a canonical request hash in the same SQLite database as the execution row.

- same key + same request -> original logical execution
- same key + different request -> conflict
- worker count/process count does not change logical execution identity

Idempotency prevents duplicate logical submissions. It does **not** by itself guarantee exactly-once external side effects.

## Durable worker leases

A claim stores:

- worker identity,
- opaque lease token,
- lease expiry,
- incremented attempt number.

The claim and attempt creation happen under a SQLite `BEGIN IMMEDIATE` transaction. Independent connections/processes therefore cannot both transition the same queued row to `RUNNING`.

While executing, the owning runner periodically renews the lease. Renewal succeeds only when:

- the execution is still `RUNNING`,
- the lease token still matches,
- the old lease has not already passed its deadline.

A worker that wakes up after its lease deadline cannot revive that lease.

### Fencing

Before publishing a terminal state, the runner revalidates ownership. `finish()` verifies the persisted lease token and current deadline inside the finishing transaction.

A stale worker therefore cannot turn an execution into `SUCCEEDED`, `FAILED`, `TIMED_OUT`, `CANCELLED` or `INTERRUPTED` after ownership has been lost.

Lease expiry is conservative: the current version records the logical execution and attempt as `INTERRUPTED`; it does not automatically replay arbitrary work.

## Attempt history

Every successful claim inserts one immutable attempt identity containing:

- execution id,
- attempt number,
- worker identity,
- start time,
- terminal time,
- terminal status,
- root process exit code.

The opaque lease token is stored internally for fencing but is not exposed by the public attempt model/API.

This separates “one logical execution” from “one concrete worker attempt,” which is required before safe retry policy can be added later.

## Expiry sweeping

Workers periodically run a cheap expiry sweep. A sweep selects `RUNNING` rows whose lease is absent or expired and transitions them to `INTERRUPTED`.

The corresponding running attempt is also closed as `INTERRUPTED`, and a `lease_expired` effect is appended.

Starting a second service does not blindly interrupt all running work. Valid leases owned by another service remain untouched.

## Cross-service cancellation

Cancellation is durable:

1. the API/store sets `cancel_requested`,
2. the local owner terminates immediately when available,
3. otherwise the owning worker observes the flag during its next lease heartbeat,
4. the root process group is terminated,
5. the valid lease holder publishes `CANCELLED`.

This allows a control request received by one service process to cancel work owned by another process that shares the database.

## Process lifecycle

On POSIX, every root command starts in a new session/process group.

ExecLedger owns that group. It targets the group for:

- user cancellation,
- execution timeout,
- service shutdown,
- lease loss,
- descendant cleanup after the root exits.

Termination first sends SIGTERM and escalates to SIGKILL if the root ignores graceful termination.

The root process exit code remains the recorded exit code. Process-group cleanup prevents background children from silently outliving the logical execution.

## Effect log and live output

The `effects` table is append-only and uses a monotonically increasing SQLite sequence. Lifecycle events, output chunks, snapshots and terminal records use that sequence.

While a command runs, stdout and stderr are drained concurrently in bounded chunks and stored as `output_chunk` effects. The final execution row also contains bounded stdout/stderr strings for convenient inspection.

SSE uses the same durable sequence:

```text
GET /v1/executions/{id}/events?after=<seq>
Last-Event-ID: <seq>
```

Reconnection resumes strictly after the last committed sequence.

## Workspace snapshots and blob storage

Before and after execution, ExecLedger records a sorted manifest of regular files containing:

- relative path,
- byte size,
- SHA-256 content digest.

The complete manifest also receives a SHA-256 digest. Symlinks are excluded from snapshot traversal.

v0.4 also stores each regular file's bytes in a local content-addressed blob store. The digest is the object key, so identical contents across snapshots are deduplicated automatically. Blob writes use a temporary file, fsync, and atomic replace; an existing object is re-hashed before it is trusted.

Snapshot metadata remains in SQLite while object bytes live beneath the state root. This separation keeps database rows compact while making snapshots restorable.

### Diff

A workspace diff validates both manifests and compares paths by digest/size. It reports:

- added files,
- modified files,
- deleted files.

The diff is metadata-only and does not mutate either historical snapshot.

### Restore

Restore always targets a newly generated server-controlled directory. It never overwrites the original execution workspace.

Before materializing files, ExecLedger verifies:

- the snapshot manifest digest,
- manifest item structure,
- relative path safety,
- duplicate paths,
- blob existence and content digest,
- stored byte size.

A partially failed restore is removed. Historical snapshots created before blob persistence remain readable, but an attempted restore fails explicitly when their object bytes are unavailable.

## Database migration

The database uses SQLite `PRAGMA user_version`.

v0.3 schema version 2 adds:

- worker ownership fields to `executions`,
- durable `attempts` table and index.

An existing v0.1/v0.2 database with schema version 0 is upgraded in place inside a transaction. Existing execution rows, idempotency mappings, effects, snapshots and captured output remain intact.

A database advertising a future version newer than this binary supports is rejected instead of being silently modified.

## Current guarantee boundary

v0.3 provides:

- durable logical job identity,
- cross-connection/process atomic claim,
- expiring ownership leases,
- stale terminal-write fencing,
- persistent attempts,
- cross-service cancellation observation,
- process-tree cleanup,
- durable incremental output evidence.

It does not yet provide:

- policy-driven automatic retry/backoff,
- exactly-once arbitrary external effects,
- multi-host consensus,
- artifact retention/garbage collection,
- hostile-code sandboxing.

These limits are explicit because reliable execution infrastructure is defined as much by what it refuses to promise as by the features it implements.
