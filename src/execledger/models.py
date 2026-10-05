from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field, field_validator


def utc_now() -> datetime:
    return datetime.now(UTC)


class ExecutionStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    INTERRUPTED = "INTERRUPTED"


TERMINAL_STATUSES = {
    ExecutionStatus.SUCCEEDED,
    ExecutionStatus.FAILED,
    ExecutionStatus.TIMED_OUT,
    ExecutionStatus.CANCELLED,
    ExecutionStatus.INTERRUPTED,
}

RETRYABLE_STATUSES = {
    ExecutionStatus.FAILED,
    ExecutionStatus.TIMED_OUT,
    ExecutionStatus.INTERRUPTED,
}


class RetryPolicy(BaseModel):
    max_attempts: Annotated[int, Field(ge=1, le=10)] = 1
    retry_on: tuple[ExecutionStatus, ...] = Field(
        default_factory=lambda: tuple(sorted(RETRYABLE_STATUSES, key=lambda item: item.value))
    )
    backoff_initial_seconds: Annotated[float, Field(ge=0, le=3600)] = 0.5
    backoff_multiplier: Annotated[float, Field(ge=1, le=10)] = 2.0
    backoff_max_seconds: Annotated[float, Field(ge=0, le=3600)] = 60.0

    @field_validator("retry_on")
    @classmethod
    def validate_retry_on(
        cls,
        value: tuple[ExecutionStatus, ...],
    ) -> tuple[ExecutionStatus, ...]:
        unsupported = set(value) - RETRYABLE_STATUSES
        if unsupported:
            names = ", ".join(sorted(item.value for item in unsupported))
            raise ValueError(f"retry_on contains unsupported statuses: {names}")
        return tuple(sorted(set(value), key=lambda item: item.value))

    def delay_after_attempt(self, attempt_number: int) -> float:
        if attempt_number < 1:
            raise ValueError("attempt_number must be positive")
        delay = self.backoff_initial_seconds * (
            self.backoff_multiplier ** (attempt_number - 1)
        )
        return min(delay, self.backoff_max_seconds)


class ExecutionSpec(BaseModel):
    argv: Annotated[list[str], Field(min_length=1, max_length=64)]
    env: dict[str, str] = Field(default_factory=dict)
    files: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: Annotated[float, Field(gt=0, le=3600)] = 60.0
    max_output_bytes: Annotated[int, Field(ge=1024, le=4 * 1024 * 1024)] = 256 * 1024
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)

    @field_validator("argv")
    @classmethod
    def validate_argv(cls, value: list[str]) -> list[str]:
        if any(not item or "\x00" in item for item in value):
            raise ValueError("argv entries must be non-empty and NUL-free")
        return value

    @field_validator("env")
    @classmethod
    def validate_env(cls, value: dict[str, str]) -> dict[str, str]:
        for key, item in value.items():
            if not key or "=" in key or "\x00" in key or "\x00" in item:
                raise ValueError("invalid environment entry")
        return value


class ExecutionRecord(BaseModel):
    id: str
    status: ExecutionStatus
    spec: ExecutionSpec
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    attempt: int = 0
    cancel_requested: bool = False
    worker_id: str | None = None
    lease_expires_at: datetime | None = None
    next_attempt_at: datetime | None = None


class AttemptRecord(BaseModel):
    id: str
    execution_id: str
    number: int
    worker_id: str
    status: ExecutionStatus
    started_at: datetime
    finished_at: datetime | None = None
    exit_code: int | None = None


class EffectRecord(BaseModel):
    seq: int
    execution_id: str
    created_at: datetime
    kind: str
    payload: dict[str, object]


class SnapshotRecord(BaseModel):
    id: str
    execution_id: str
    phase: str
    created_at: datetime
    digest: str
    manifest: list[dict[str, object]]


class FileChange(BaseModel):
    path: str
    before_sha256: str | None = None
    after_sha256: str | None = None
    before_size: int | None = None
    after_size: int | None = None


class WorkspaceDiff(BaseModel):
    before_snapshot_id: str
    after_snapshot_id: str
    added: list[FileChange]
    modified: list[FileChange]
    deleted: list[FileChange]


class RestoreRecord(BaseModel):
    id: str
    execution_id: str
    snapshot_id: str
    directory: str
    file_count: int
    snapshot_digest: str


class GarbageBlob(BaseModel):
    digest: str
    size: int


class GarbageCollectionReport(BaseModel):
    dry_run: bool
    snapshots_scanned: int
    blobs_scanned: int
    referenced_blobs: int
    orphan_blobs: list[GarbageBlob]
    deleted_blobs: list[GarbageBlob]
    bytes_reclaimable: int
    bytes_reclaimed: int
    restores_scanned: int
    restore_dirs_eligible: list[str]
    restore_dirs_deleted: list[str]
    restore_bytes_reclaimable: int
    restore_bytes_reclaimed: int


class SubmitResult(BaseModel):
    execution: ExecutionRecord
    created: bool
