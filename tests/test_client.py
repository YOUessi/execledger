from __future__ import annotations

import json
import threading
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

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._send(200, {"ok": True, "workers": 1})
            return
        if self.path == "/v1/executions/missing":
            self._send(404, {"detail": "execution not found"})
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
