# Changelog

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
