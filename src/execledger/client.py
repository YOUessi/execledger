from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from execledger.models import (
    TERMINAL_STATUSES,
    AttemptRecord,
    EffectRecord,
    ExecutionRecord,
    ExecutionSpec,
    GarbageCollectionReport,
    RestoreRecord,
    SnapshotRecord,
    SubmitResult,
    WorkspaceDiff,
)


class ExecLedgerHTTPError(RuntimeError):
    def __init__(self, status: int, detail: str):
        self.status = status
        self.detail = detail
        super().__init__(f"ExecLedger API error {status}: {detail}")


class ExecLedgerClient:
    """Small synchronous client for the ExecLedger HTTP API."""

    def __init__(self, base_url: str = "http://127.0.0.1:8080", *, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        url = self.base_url + path
        body = None
        request_headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            request_headers["Content-Type"] = "application/json"
        if headers:
            request_headers.update(headers)

        request = urllib.request.Request(
            url,
            data=body,
            headers=request_headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            detail = raw.decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and isinstance(parsed.get("detail"), str):
                    detail = parsed["detail"]
            except (ValueError, UnicodeError):
                pass
            raise ExecLedgerHTTPError(exc.code, detail) from exc
        except urllib.error.URLError as exc:
            raise ConnectionError(
                f"cannot reach ExecLedger at {self.base_url}: {exc.reason}"
            ) from exc

        if not raw:
            return None
        try:
            return json.loads(raw)
        except (ValueError, UnicodeError) as exc:
            raise ExecLedgerHTTPError(502, "server returned non-JSON response") from exc

    def health(self) -> dict[str, Any]:
        value = self._request("GET", "/healthz")
        assert isinstance(value, dict)
        return value

    def submit(self, spec: ExecutionSpec, idempotency_key: str) -> SubmitResult:
        value = self._request(
            "POST",
            "/v1/executions",
            payload=spec.model_dump(mode="json"),
            headers={"Idempotency-Key": idempotency_key},
        )
        return SubmitResult.model_validate(value)

    def get(self, execution_id: str) -> ExecutionRecord:
        value = self._request("GET", f"/v1/executions/{urllib.parse.quote(execution_id)}")
        return ExecutionRecord.model_validate(value)

    def list(self, *, limit: int = 100) -> list[ExecutionRecord]:
        value = self._request("GET", f"/v1/executions?limit={limit}")
        return [ExecutionRecord.model_validate(item) for item in value]

    def cancel(self, execution_id: str) -> ExecutionRecord:
        value = self._request(
            "POST",
            f"/v1/executions/{urllib.parse.quote(execution_id)}/cancel",
        )
        return ExecutionRecord.model_validate(value)

    def attempts(self, execution_id: str) -> list[AttemptRecord]:
        value = self._request(
            "GET",
            f"/v1/executions/{urllib.parse.quote(execution_id)}/attempts",
        )
        return [AttemptRecord.model_validate(item) for item in value]

    def effects(
        self,
        execution_id: str,
        *,
        after: int = 0,
        limit: int = 1000,
    ) -> list[EffectRecord]:
        quoted = urllib.parse.quote(execution_id)
        value = self._request(
            "GET",
            f"/v1/executions/{quoted}/effects?after={after}&limit={limit}",
        )
        return [EffectRecord.model_validate(item) for item in value]

    def iter_events(
        self,
        execution_id: str,
        *,
        after: int = 0,
    ):
        quoted = urllib.parse.quote(execution_id)
        request = urllib.request.Request(
            f"{self.base_url}/v1/executions/{quoted}/events?after={after}",
            headers={"Accept": "text/event-stream"},
            method="GET",
        )
        try:
            response = urllib.request.urlopen(request, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            detail = raw.decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and isinstance(parsed.get("detail"), str):
                    detail = parsed["detail"]
            except (ValueError, UnicodeError):
                pass
            raise ExecLedgerHTTPError(exc.code, detail) from exc
        except urllib.error.URLError as exc:
            raise ConnectionError(
                f"cannot reach ExecLedger at {self.base_url}: {exc.reason}"
            ) from exc

        with response:
            data_lines: list[str] = []
            for raw_line in response:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                if line.startswith(":"):
                    continue
                if not line:
                    if data_lines:
                        payload = json.loads("\n".join(data_lines))
                        yield EffectRecord.model_validate(payload)
                        data_lines.clear()
                    continue
                if line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())

    def snapshots(self, execution_id: str) -> list[SnapshotRecord]:
        value = self._request(
            "GET",
            f"/v1/executions/{urllib.parse.quote(execution_id)}/snapshots",
        )
        return [SnapshotRecord.model_validate(item) for item in value]

    def diff(
        self,
        execution_id: str,
        before_snapshot_id: str,
        after_snapshot_id: str,
    ) -> WorkspaceDiff:
        quoted = urllib.parse.quote(execution_id)
        query = urllib.parse.urlencode(
            {
                "before": before_snapshot_id,
                "after": after_snapshot_id,
            }
        )
        value = self._request("GET", f"/v1/executions/{quoted}/diff?{query}")
        return WorkspaceDiff.model_validate(value)

    def restore(self, execution_id: str, snapshot_id: str) -> RestoreRecord:
        execution = urllib.parse.quote(execution_id)
        snapshot = urllib.parse.quote(snapshot_id)
        value = self._request(
            "POST",
            f"/v1/executions/{execution}/snapshots/{snapshot}/restore",
        )
        return RestoreRecord.model_validate(value)

    def gc(
        self,
        *,
        apply: bool = False,
        restore_older_than_seconds: float | None = None,
    ) -> GarbageCollectionReport:
        params = {"apply": "true" if apply else "false"}
        if restore_older_than_seconds is not None:
            params["restore_older_than_seconds"] = str(restore_older_than_seconds)
        query = urllib.parse.urlencode(params)
        value = self._request("POST", f"/v1/maintenance/gc?{query}")
        return GarbageCollectionReport.model_validate(value)

    def wait(
        self,
        execution_id: str,
        *,
        timeout: float = 60.0,
        interval: float = 0.2,
    ) -> ExecutionRecord:
        deadline = time.monotonic() + timeout
        while True:
            record = self.get(execution_id)
            if record.status in TERMINAL_STATUSES:
                return record
            if time.monotonic() >= deadline:
                raise TimeoutError(execution_id)
            time.sleep(interval)
