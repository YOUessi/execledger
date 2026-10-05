# ExecLedger

ExecLedger is a durable local execution control plane for developer tools, coding agents, and automation workflows. It accepts command jobs through an HTTP API, gives every job an isolated working directory, persists state in SQLite, enforces idempotent submission, records append-only execution evidence, and coordinates workers through durable leases.

The project is intended for local automation, agent/tool execution, CI-like workflows and reproducible debugging. **It is not a hostile-code security sandbox**; submitted processes run with the service user's OS permissions.

## Why this project

Agent workflows fail in more ways than “the command returned non-zero”:

- API retries can submit the same logical work twice.
- Two workers can race for the same queued job.
- A worker can die after claiming work.
- A stale worker can wake up later and incorrectly publish success.
- Cancellation can originate from a process that does not own the child process.
- Descendant processes can survive after a timeout or service shutdown.
- Live output can disappear until a job completes.
- Workspace mutations are hard to audit.

ExecLedger turns those failure modes into explicit control-plane state: idempotency, leases, attempts, process-tree ownership, durable effect logs, resumable event streams, and workspace snapshots.

## Implemented in v0.6

### Durable execution state

- SQLite-backed execution records.
- Atomic `Idempotency-Key` semantics.
- Explicit state machine:
  `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `TIMED_OUT`,
  `CANCELLED`, `INTERRUPTED`.
- In-place migration of existing v0.1/v0.2 SQLite databases.

### Worker ownership

- Atomic work claiming across independent SQLite connections/processes.
- Persisted worker identity, opaque lease token, heartbeat and expiry.
- Stale lease holders cannot publish terminal state.
- Expired leases are converted to `INTERRUPTED` attempts.
- Every claim creates a durable attempt record.
- Attempt history records worker, attempt number, start/end time, status and exit code.
- Cancellation requested through another service instance is observed by the owning worker heartbeat.

### Retry and backoff

Retries are explicit and opt-in. The default is still exactly one attempt.

Each execution can configure:

- `max_attempts`;
- retryable outcomes from `FAILED`, `TIMED_OUT`, and `INTERRUPTED`;
- initial backoff;
- exponential multiplier;
- maximum backoff cap.

A retryable failed attempt is closed in attempt history, while the logical execution returns to `QUEUED` with a durable `next_attempt_at`. Workers only claim that execution after the scheduled time.

Lease expiry follows the same retry policy, so worker loss can be retried without allowing the stale worker to publish a terminal result later.

### Process lifecycle

- Bounded async worker pool using argv execution; no shell interpolation.
- POSIX jobs run in dedicated process groups.
- Cancel, timeout, shutdown and lease loss terminate the whole process group.
- Termination escalates to a forced kill if graceful termination is ignored.
- Graceful service shutdown records interrupted work as `INTERRUPTED`, never as false success.

### Evidence and observability

- Incremental stdout/stderr persistence as append-only `output_chunk` effects.
- Bounded final stdout/stderr capture with truncation evidence.
- Resumable Server-Sent Events using the durable effect sequence as the SSE event id.
- `Last-Event-ID` and explicit sequence cursors are supported.
- Deterministic before/after workspace manifests with SHA-256 file hashes.
- Snapshot file bytes stored in a local content-addressed SHA-256 blob store.
- Identical snapshot contents deduplicated by digest.
- Workspace diffs report added, modified and deleted files between any two snapshots.
- Snapshots can be restored into a new server-managed directory without mutating execution history.
- REST endpoint and CLI access to attempt history, effect history, snapshots, diffs and restores.

### Operator interfaces

- Built-in same-origin Web console served at `/ui/`.
- Submit jobs, inspect execution status, cancel running work, and watch live output in the browser.
- Inspect attempt history, effect timeline, request spec, snapshots, workspace diffs and restored copies.
- Python HTTP client.
- CLI commands for:
  `submit`, `get`, `list`, `wait`, `cancel`, `logs`, `follow`,
  `attempts`, `effects`, `snapshots`, `diff`, and `restore`.
- Configurable worker count, worker identity, lease TTL and heartbeat interval.
- Dockerfile.
- GitHub Actions on Python 3.11 and 3.12, including wheel build and packaged Web asset verification.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
execledger serve --root .state --port 8080
```

Then open the operator console at:

```text
http://127.0.0.1:8080/ui/
```

Submit a job:

```bash
execledger submit examples/job.json --idempotency-key demo-1
```

Inspect or follow it:

```bash
execledger list
execledger get EXECUTION_ID
execledger wait EXECUTION_ID
execledger logs EXECUTION_ID
execledger follow EXECUTION_ID --events
execledger attempts EXECUTION_ID
execledger effects EXECUTION_ID
execledger snapshots EXECUTION_ID
execledger diff EXECUTION_ID BEFORE_SNAPSHOT_ID AFTER_SNAPSHOT_ID
execledger restore EXECUTION_ID SNAPSHOT_ID
execledger cancel EXECUTION_ID
```

Use a non-default server with:

```bash
export EXECLEDGER_URL=http://127.0.0.1:8080
```

## Multiple service processes

Multiple ExecLedger service processes can point at the same state root/SQLite database. Work ownership is not inferred from an in-memory task map: a claim is represented by a persisted lease.

For explicit identities and shorter development leases:

```bash
execledger serve \
  --root .state \
  --worker-id worker-a \
  --workers 2 \
  --lease-seconds 5 \
  --heartbeat-interval 1
```

A second service using the same state root competes for queued work through SQLite transactions. Only one valid lease may own a given execution at a time.

## Live events

The SSE endpoint is:

```text
GET /v1/executions/{id}/events?after=<effect-seq>
```

The server also accepts:

```text
Last-Event-ID: <effect-seq>
```

Each SSE record contains the full persisted effect record. A reconnect starts strictly after the acknowledged sequence.

## Repository layout

```text
src/execledger/
  api.py        HTTP API + SSE stream
  client.py     synchronous Python HTTP client
  cli.py        server and operator CLI
  models.py     request/state/attempt/evidence models
  runner.py     worker leases + process lifecycle + output capture
  service.py    service orchestration
  store.py      SQLite state, migrations, leases, attempts and evidence
  workspace.py  path safety + snapshot/diff/restore logic
  blobstore.py  content-addressed snapshot bytes
  web/          built-in operator console (HTML/CSS/JS)

tests/          unit, integration and multiprocess regression tests
docs/           architecture and explicit guarantees/limits
examples/       runnable execution payload
```

## Design boundaries

ExecLedger does not pretend that a directory is a secure sandbox. For hostile or mutually untrusted workloads, place the runner behind a real container/VM/sandbox boundary and keep ExecLedger as the control plane.

v0.6 supports policy-driven automatic retry, but retries remain **opt-in** because replaying an arbitrary command can duplicate external side effects. ExecLedger does not claim exactly-once semantics for effects outside its own durable control plane.

The Web console is an operator interface, not an authentication boundary. ExecLedger is still intended for trusted/local control-plane deployments unless an external access-control layer is placed in front of it.

Other planned reliability work includes artifact retention/GC, richer schema migration tooling, metrics/health diagnostics, and policy-driven retry/backoff.


## Restorable snapshots

Every before/after snapshot stores a manifest and also persists each regular file's bytes in the state-root blob store:

```text
.state/
  execledger.sqlite3
  workspaces/
  blobs/
    ab/
      ab...<sha256>
  restores/
    <restore-id>/
```

Blob identity is the SHA-256 of the bytes, so unchanged files across executions and snapshots are stored once.

Compare two snapshots:

```bash
execledger diff EXECUTION_ID BEFORE_SNAPSHOT_ID AFTER_SNAPSHOT_ID
```

Restore a historical snapshot into a new directory:

```bash
execledger restore EXECUTION_ID SNAPSHOT_ID
```

Restore never overwrites the original execution workspace. The snapshot manifest digest, every relative path, every blob digest and every restored byte count are validated before a restore is considered successful.

Snapshots created by older ExecLedger releases remain readable as evidence manifests. If their underlying bytes were never written to the new blob store, restore fails explicitly rather than fabricating content.


## Operator Web console

The Web console is bundled inside the Python package, so no Node.js/npm build step is required at runtime. It uses the same-origin REST and SSE APIs exposed by the service.

The console provides:

- execution list with live status;
- JSON job submission with explicit idempotency keys;
- live combined/stdout/stderr views;
- cancellation for non-terminal work;
- durable attempt history;
- request-spec inspection;
- workspace snapshot list;
- snapshot-to-snapshot diff;
- snapshot restore;
- reverse chronological effect timeline.

The UI is intentionally thin: it does not invent a second persistence model or hide control-plane semantics behind client-only state. Refreshing the page reconstructs the view from the durable HTTP APIs.


## Retry policy

A request can opt into bounded retry behavior:

```json
{
  "argv": ["python", "job.py"],
  "retry_policy": {
    "max_attempts": 3,
    "retry_on": ["FAILED", "TIMED_OUT", "INTERRUPTED"],
    "backoff_initial_seconds": 1,
    "backoff_multiplier": 2,
    "backoff_max_seconds": 30
  }
}
```

The first retry waits 1 second, the second waits 2 seconds, then the delay continues exponentially until the cap is reached.

Important semantics:

- `max_attempts: 1` is the default, so existing requests do not start retrying after an upgrade.
- `CANCELLED` is never automatically retried.
- A queued execution waiting for backoff can still be cancelled.
- Attempt outcomes remain durable even if the overall logical execution later succeeds.
- `next_attempt_at` is persisted, so a process restart does not forget the backoff window.
- The same workspace is reused across attempts. Callers should only enable retry for commands whose replay behavior they understand.
