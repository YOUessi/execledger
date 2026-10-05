# Architecture

ExecLedger is a local execution control plane for reproducible developer and agent jobs. Commands run with the service user's OS permissions; the project focuses on durable execution semantics, worker ownership, process lifecycle and evidence rather than hostile-code isolation.

## Components

- **HTTP API (`api.py`)**: submission, inspection, cancellation, attempt/effect/snapshot reads, and SSE events.
- **Python client (`client.py`)**: small synchronous client used directly and by the CLI.
- **Operator CLI (`cli.py`)**: server and job operations.
- **Operator Web console (`web/`)**: bundled same-origin HTML/CSS/JS UI over the public REST/SSE contract.
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


## Operator Web console

The Web console is packaged as static assets under `execledger/web` and mounted by FastAPI at `/ui`. The root path redirects to `/ui/`.

The UI deliberately consumes the same APIs available to external clients:

- list/get/submit/cancel executions;
- attempts and effects;
- SSE live events;
- snapshots, diff and restore.

It does not read SQLite directly and does not maintain a separate backend state model. A browser refresh therefore reconstructs the operator view from durable server state.

The console has no independent authentication layer. Deployments that expose ExecLedger beyond a trusted/local boundary must add access control in front of the service.

The wheel build includes the static assets as package data. CI builds the distribution and verifies that the HTML, CSS and JavaScript are present in the wheel.


## Retry state and backoff

Retry is represented as state on the logical execution rather than as an in-memory timer.

A completed attempt with a retryable outcome is persisted first. If the configured attempt budget remains, the execution row returns to `QUEUED` and receives a durable `next_attempt_at` timestamp. Work claiming filters queued rows by that timestamp.

This gives retry three useful properties:

1. restarting the service does not lose the delay;
2. multiple workers still compete through the same SQLite claim transaction;
3. attempt history stays immutable even when the logical execution continues.

Backoff is exponential:

```text
delay(n) = min(initial * multiplier^(n-1), max_backoff)
```

where `n` is the attempt that just failed.

Retryable outcomes are deliberately limited to `FAILED`, `TIMED_OUT`, and `INTERRUPTED`. `CANCELLED` and `SUCCEEDED` are never retry targets.

The default `max_attempts` is 1. This is a compatibility and safety boundary: upgrading ExecLedger never silently changes a once-only command into a replayed command.

## Retry and idempotency

Adding retry settings changed the serialized execution request schema. Existing databases may contain idempotency hashes computed before the retry policy field existed.

When an old idempotency hash differs, ExecLedger falls back to semantic request comparison after applying current defaults. If the requests are equivalent, the existing logical execution is reused and the stored hash is upgraded. A genuinely different request still conflicts.

This preserves the original idempotency contract across schema evolution.


## Storage maintenance and garbage collection

Snapshot blobs are content-addressed objects, so object lifetime is determined by references from durable snapshot manifests rather than by execution age.

GC performs a mark-and-sweep style pass:

1. read every snapshot manifest and collect referenced SHA-256 digests;
2. inventory canonical objects under the blob store;
3. classify objects absent from the reference set as orphaned;
4. report reclaimable bytes in dry-run mode;
5. delete only those orphaned objects when apply mode is explicitly requested.

Malformed snapshot JSON, malformed manifest items, or malformed digests abort collection before deletion. The system deliberately leaks storage rather than risking evidence loss when metadata is ambiguous.

### Cross-process maintenance lock

Blob creation happens before the snapshot row is committed to SQLite. Without coordination, GC could otherwise observe that newly written object during this short interval and classify it as unreferenced.

Snapshot creation, snapshot restore, and GC therefore acquire the same state-root file lock. The lock is advisory but cross-process, and it covers both object-store operations and the corresponding snapshot metadata update.

This keeps the blob store and snapshot reference graph consistent even when multiple ExecLedger service processes share one state root.

### Restored-copy retention

Restored workspaces are derived copies rather than durable evidence roots. GC can optionally prune restore directories older than a caller-specified age.

Restore cleanup is disabled unless an age threshold is explicitly provided. Dry-run mode reports eligible directory names and byte counts without mutating them.

Execution workspaces themselves are not deleted by v0.7 GC. Their lifecycle remains independent because they may still be useful for debugging or future retry policy.


## Observability model

ExecLedger derives operational state from the same durable SQLite rows that drive scheduling. Observability is therefore not maintained in a separate in-memory counter system.

The structured diagnostics endpoint reports:

- execution counts by durable status;
- immediately claimable queue depth;
- queued executions delayed by retry backoff;
- valid and expired worker leases;
- active child processes owned by the current service;
- aggregate attempt/effect/snapshot/idempotency counts;
- content-addressed blob count and bytes;
- workspace and restored-copy count/bytes;
- SQLite database and WAL sizes.

`/metrics` renders these diagnostics as Prometheus-compatible gauges. The current implementation intentionally derives values on scrape instead of introducing a second metrics persistence dependency.

### Liveness vs readiness

`/healthz` is intentionally shallow: if the HTTP process can answer, it reports liveness.

`/readyz` checks that SQLite answers and that the state, workspace and blob roots are present. A failed readiness check returns HTTP 503 so a supervisor can stop routing new traffic without conflating that condition with process death.

## Execution workspace retention

Content-addressed snapshot objects are the durable historical representation. Execution workspaces are mutable run directories and can eventually be reclaimed.

Workspace retention is integrated into the same dry-run-first maintenance operation as blob/restore GC. An execution workspace is eligible only when:

- the execution is in a terminal status;
- `finished_at` is older than the requested cutoff;
- at least one durable snapshot exists.

The snapshot requirement is deliberate. A terminal workspace with no snapshot may still contain the only surviving copy of its inputs or partial state, so maintenance keeps it.

Queued executions waiting for their first run or for retry backoff are never candidates. Running work is never a candidate.

Workspace deletion does not remove SQLite execution history, attempts, effects, idempotency mappings, snapshot metadata or referenced blob objects.


## Execution backend boundary

The control plane no longer owns subprocess construction directly. `ExecutionRunner` depends on the `ExecutionBackend` protocol:

- `spawn(...)` receives the durable execution identity, attempt number, request, workspace, and environment;
- `collect_result(...)` returns the target exit code and optional resource-usage evidence.

The built-in `SubprocessBackend` is intentionally small. Durable scheduling, leases, retry, cancellation, snapshots, effects, and persistence remain outside the backend. A future container/VM backend can therefore replace process creation without forking the state machine.

## POSIX launcher and resource limits

Resource limits are applied by a dedicated launcher process rather than by `preexec_fn` inside the long-lived async service.

The sequence is:

```text
runner
  -> new-session launcher
      -> fork
          -> child applies setrlimit
          -> child execs target
      -> launcher wait4(target)
      -> launcher writes usage result
  -> runner persists attempt/execution outcome
```

The launcher applies configured limits for:

- CPU seconds (`RLIMIT_CPU`);
- virtual address space (`RLIMIT_AS`);
- file size (`RLIMIT_FSIZE`);
- open descriptors (`RLIMIT_NOFILE`);
- process count where supported (`RLIMIT_NPROC`).

Core dumps are disabled for launched targets.

The launcher strips its internal resource-limit environment variable before the target `exec`. An exec failure is reported separately from a target that intentionally exits with status 127, preserving the previous `exit_code=None` launch-failure contract.

## Resource usage evidence

On POSIX the launcher uses `wait4` to capture the target's resource usage. ExecLedger persists:

- wall-clock duration;
- user CPU time;
- system CPU time;
- peak RSS (normalized to bytes on Linux);
- voluntary and involuntary context-switch counts.

Usage is stored on the concrete attempt row and on the logical execution's latest result. Retry therefore preserves usage for every prior attempt while exposing the most recent attempt on the top-level execution.

Schema version 4 adds nullable `usage_json` columns to executions and attempts. Existing databases migrate in place and old rows remain valid with no usage payload.

## Isolation guarantee boundary

POSIX rlimits reduce accidental resource exhaustion; they are not a hostile-code isolation primitive.

A process can still interact with any filesystem, network, IPC, device, or credential that the service user can access unless a stronger backend restricts it. The backend protocol exists specifically so container/cgroup or VM isolation can be added later without changing durable execution semantics.
