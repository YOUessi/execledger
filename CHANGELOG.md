# Changelog

## 0.4.0

- Added a local SHA-256 content-addressed blob store for snapshot file bytes.
- Snapshot creation now persists restorable content while deduplicating identical files by digest.
- Added manifest-integrity validation before diff and restore operations.
- Added workspace diff reporting for added, modified and deleted files.
- Added snapshot restore into a new server-managed directory without mutating execution history.
- Added path-safety, duplicate-path, missing-blob, size and corruption checks for restore.
- Added REST, Python client and CLI surfaces for snapshot diff and restore.
- Preserved readability of legacy hash-only snapshots while reporting them as non-restorable when bytes are unavailable.
- Added blob-store, restore round-trip, diff, corruption and path-traversal tests.

## 0.3.0

- Added durable worker leases with persisted owner identity, opaque lease token, heartbeat and expiry.
- Added stale-worker fencing so an expired or replaced lease cannot publish terminal execution state.
- Added durable per-execution attempt history.
- Added cross-connection/process atomic claiming through SQLite transactions.
- Added expiry sweeping that closes lost work as `INTERRUPTED` rather than silently reassigning arbitrary side effects.
- Added cross-service cancellation observation through the lease heartbeat.
- Added transactional migration from the existing v0.1/v0.2 SQLite schema.
- Added worker/lease configuration and attempt-history API/client/CLI surfaces.
- Hardened termination so cancel, shutdown and lease loss escalate if SIGTERM is ignored.
- Added competing-service, fencing, migration, expiry and multiprocess claim tests.

## 0.2.0

- Added a reusable synchronous Python HTTP client.
- Added operator CLI commands for submit, get, list, wait, cancel, logs, effects, snapshots and live follow.
- Persist stdout/stderr incrementally as append-only output events while a job is running.
- Added a resumable Server-Sent Events stream keyed by durable effect sequence.
- Added process-group ownership on POSIX so timeout, cancellation and shutdown clean up descendants.
- Distinguished service shutdown from ordinary process failure by recording interrupted executions as `INTERRUPTED`.
- Hardened shutdown races around job claim and process launch.
- Added tests for live event resume, client decoding, process-tree cancellation, shutdown semantics and effect cursors.
- Updated architecture and operator documentation.

## 0.1.0

- Initial durable execution service.
- SQLite-backed execution state and idempotent submission.
- Async worker pool with timeout/cancel behavior.
- Per-execution workspace, effect log and before/after snapshots.
- Startup recovery of stale `RUNNING` executions as `INTERRUPTED`.
- FastAPI service, Dockerfile, tests and CI.
