import sys
from pathlib import Path

from fastapi.testclient import TestClient

from execledger.api import create_app


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
