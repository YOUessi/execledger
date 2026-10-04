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


class ExecutionSpec(BaseModel):
    argv: Annotated[list[str], Field(min_length=1, max_length=64)]
    env: dict[str, str] = Field(default_factory=dict)
    files: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: Annotated[float, Field(gt=0, le=3600)] = 60.0
    max_output_bytes: Annotated[int, Field(ge=1024, le=4 * 1024 * 1024)] = 256 * 1024

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


class SubmitResult(BaseModel):
    execution: ExecutionRecord
    created: bool
