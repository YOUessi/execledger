import sys
import time
from pathlib import Path

from fastapi.testclient import TestClient

from execledger.api import create_app


def _wait_terminal(client: TestClient, execution_id: str) -> dict[str, object]:
    for _ in range(300):
        record = client.get(f"/v1/executions/{execution_id}").json()
        if record["status"] in {"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED", "INTERRUPTED"}:
            return record
        time.sleep(0.01)
    raise AssertionError("execution did not reach terminal state")


def test_api_submit_idempotency_and_readback(tmp_path: Path):
    app = create_app(tmp_path, workers=1)
    with TestClient(app) as client:
        payload = {"argv": [sys.executable, "-c", "print('api-ok')"]}
        first = client.post("/v1/executions", headers={"Idempotency-Key": "api-key"}, json=payload)
        assert first.status_code == 201
        second = client.post("/v1/executions", headers={"Idempotency-Key": "api-key"}, json=payload)
        assert second.status_code == 201
        assert first.json()["execution"]["id"] == second.json()["execution"]["id"]
        conflict = client.post(
            "/v1/executions",
            headers={"Idempotency-Key": "api-key"},
            json={"argv": [sys.executable, "-c", "print('different')"]},
        )
        assert conflict.status_code == 409
        execution_id = first.json()["execution"]["id"]
        readback = client.get(f"/v1/executions/{execution_id}")
        assert readback.status_code == 200
        assert client.get(f"/v1/executions/{execution_id}/effects").status_code == 200


def test_sse_stream_is_resumable_from_effect_sequence(tmp_path: Path):
    app = create_app(tmp_path, workers=1)
    with TestClient(app) as client:
        payload = {
            "argv": [
                sys.executable,
                "-c",
                "print('one'); print('two')",
            ]
        }
        created = client.post(
            "/v1/executions",
            headers={"Idempotency-Key": "stream-key"},
            json=payload,
        )
        execution_id = created.json()["execution"]["id"]
        assert _wait_terminal(client, execution_id)["status"] == "SUCCEEDED"

        effects = client.get(f"/v1/executions/{execution_id}/effects").json()
        output = [item for item in effects if item["kind"] == "output_chunk"]
        assert output
        cursor = output[0]["seq"]

        with client.stream(
            "GET",
            f"/v1/executions/{execution_id}/events",
            headers={"Last-Event-ID": str(cursor)},
        ) as response:
            assert response.status_code == 200
            body = "\n".join(response.iter_lines())

        assert f"id: {cursor}" not in body
        assert "event: execution_finished" in body
        assert "data:" in body


def test_sse_rejects_invalid_last_event_id(tmp_path: Path):
    app = create_app(tmp_path, workers=1)
    with TestClient(app) as client:
        created = client.post(
            "/v1/executions",
            headers={"Idempotency-Key": "invalid-cursor-key"},
            json={"argv": [sys.executable, "-c", "print('ok')"]},
        )
        execution_id = created.json()["execution"]["id"]
        response = client.get(
            f"/v1/executions/{execution_id}/events",
            headers={"Last-Event-ID": "not-an-integer"},
        )
        assert response.status_code == 400
        assert response.json()["detail"] == "invalid Last-Event-ID"


def test_api_exposes_attempt_history(tmp_path: Path):
    app = create_app(
        tmp_path,
        workers=1,
        lease_seconds=0.5,
        heartbeat_interval=0.1,
        worker_id="api-worker",
    )
    with TestClient(app) as client:
        created = client.post(
            "/v1/executions",
            headers={"Idempotency-Key": "attempt-api-key"},
            json={"argv": [sys.executable, "-c", "print('ok')"]},
        )
        execution_id = created.json()["execution"]["id"]
        assert _wait_terminal(client, execution_id)["status"] == "SUCCEEDED"

        attempts = client.get(f"/v1/executions/{execution_id}/attempts")
        assert attempts.status_code == 200
        payload = attempts.json()
        assert len(payload) == 1
        assert payload[0]["number"] == 1
        assert payload[0]["worker_id"] == "api-worker/0"
        assert payload[0]["status"] == "SUCCEEDED"
