from __future__ import annotations

import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from execledger.client import ExecLedgerClient, ExecLedgerHTTPError
from execledger.models import ExecutionStatus


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send(self, status: int, payload: object) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/v1/maintenance/gc":
            query = urllib.parse.parse_qs(parsed.query)
            apply = query.get("apply", ["false"])[0] == "true"
            self._send(
                200,
                {
                    "dry_run": not apply,
                    "snapshots_scanned": 2,
                    "blobs_scanned": 3,
                    "referenced_blobs": 2,
                    "orphan_blobs": [
                        {"digest": "a" * 64, "size": 12}
                    ],
                    "deleted_blobs": (
                        [{"digest": "a" * 64, "size": 12}]
                        if apply
                        else []
                    ),
                    "bytes_reclaimable": 12,
                    "bytes_reclaimed": 12 if apply else 0,
                    "restores_scanned": 1,
                    "restore_dirs_eligible": ["restore-1"],
                    "restore_dirs_deleted": ["restore-1"] if apply else [],
                    "restore_bytes_reclaimable": 7,
                    "restore_bytes_reclaimed": 7 if apply else 0,
                },
            )
            return
        self._send(404, {"detail": "not found"})

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._send(200, {"ok": True, "workers": 1})
            return
        if self.path == "/v1/executions/missing":
            self._send(404, {"detail": "execution not found"})
            return
        if self.path == "/v1/executions/ex-1/attempts":
            self._send(
                200,
                [
                    {
                        "id": "attempt-1",
                        "execution_id": "ex-1",
                        "number": 1,
                        "worker_id": "worker-a/0",
                        "status": "SUCCEEDED",
                        "started_at": "2026-01-01T00:00:00Z",
                        "finished_at": "2026-01-01T00:00:01Z",
                        "exit_code": 0,
                    }
                ],
            )
            return
        if self.path == "/v1/executions/ex-1/events?after=0":
            payload = {
                "seq": 7,
                "execution_id": "ex-1",
                "created_at": "2026-01-01T00:00:01Z",
                "kind": "execution_finished",
                "payload": {"status": "SUCCEEDED", "exit_code": 0},
            }
            body = (
                "id: 7\n"
                "event: execution_finished\n"
                f"data: {json.dumps(payload)}\n\n"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/v1/executions/ex-1":
            self._send(
                200,
                {
                    "id": "ex-1",
                    "status": "SUCCEEDED",
                    "spec": {
                        "argv": ["echo", "ok"],
                        "env": {},
                        "files": {},
                        "timeout_seconds": 60.0,
                        "max_output_bytes": 262144,
                    },
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:01Z",
                    "started_at": "2026-01-01T00:00:00Z",
                    "finished_at": "2026-01-01T00:00:01Z",
                    "exit_code": 0,
                    "stdout": "ok\n",
                    "stderr": "",
                    "attempt": 1,
                    "cancel_requested": False,
                },
            )
            return
        self._send(404, {"detail": "not found"})


@pytest.fixture
def api_url() -> str:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_client_health_and_execution_decode(api_url: str):
    client = ExecLedgerClient(api_url)
    assert client.health() == {"ok": True, "workers": 1}
    record = client.get("ex-1")
    assert record.status == ExecutionStatus.SUCCEEDED
    assert record.stdout == "ok\n"


def test_client_surfaces_api_detail(api_url: str):
    client = ExecLedgerClient(api_url)
    with pytest.raises(ExecLedgerHTTPError) as captured:
        client.get("missing")
    assert captured.value.status == 404
    assert captured.value.detail == "execution not found"


def test_client_decodes_sse_effects(api_url: str):
    client = ExecLedgerClient(api_url)
    events = list(client.iter_events("ex-1"))
    assert len(events) == 1
    assert events[0].seq == 7
    assert events[0].kind == "execution_finished"
    assert events[0].payload["status"] == "SUCCEEDED"


def test_client_decodes_attempt_history(api_url: str):
    client = ExecLedgerClient(api_url)
    attempts = client.attempts("ex-1")
    assert len(attempts) == 1
    assert attempts[0].worker_id == "worker-a/0"
    assert attempts[0].status == ExecutionStatus.SUCCEEDED


def test_client_decodes_storage_gc_report(api_url: str):
    client = ExecLedgerClient(api_url)
    dry = client.gc(restore_older_than_seconds=3600)
    assert dry.dry_run is True
    assert dry.bytes_reclaimable == 12
    assert dry.deleted_blobs == []

    applied = client.gc(
        apply=True,
        restore_older_than_seconds=3600,
    )
    assert applied.dry_run is False
    assert applied.bytes_reclaimed == 12
    assert applied.restore_dirs_deleted == ["restore-1"]
