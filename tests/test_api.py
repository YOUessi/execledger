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


def test_api_diff_and_restore_snapshot(tmp_path: Path):
    app = create_app(tmp_path, workers=1)
    with TestClient(app) as client:
        created = client.post(
            "/v1/executions",
            headers={"Idempotency-Key": "restore-api-key"},
            json={
                "argv": [
                    sys.executable,
                    "-c",
                    (
                        "from pathlib import Path; "
                        "Path('input.txt').write_text('changed'); "
                        "Path('created.txt').write_text('new')"
                    ),
                ],
                "files": {"input.txt": "original"},
            },
        )
        execution_id = created.json()["execution"]["id"]
        assert _wait_terminal(client, execution_id)["status"] == "SUCCEEDED"

        snapshots = client.get(f"/v1/executions/{execution_id}/snapshots").json()
        assert [item["phase"] for item in snapshots] == ["before", "after"]
        before, after = snapshots

        diff = client.get(
            f"/v1/executions/{execution_id}/diff",
            params={"before": before["id"], "after": after["id"]},
        )
        assert diff.status_code == 200
        changes = diff.json()
        assert [item["path"] for item in changes["added"]] == ["created.txt"]
        assert [item["path"] for item in changes["modified"]] == ["input.txt"]

        restored = client.post(
            f"/v1/executions/{execution_id}/snapshots/{before['id']}/restore"
        )
        assert restored.status_code == 200
        restore_root = Path(restored.json()["directory"])
        assert (restore_root / "input.txt").read_text() == "original"
        assert not (restore_root / "created.txt").exists()


def test_operator_console_is_served_from_same_origin(tmp_path: Path):
    app = create_app(tmp_path, workers=1)
    with TestClient(app) as client:
        root = client.get("/", follow_redirects=False)
        assert root.status_code in {302, 307}
        assert root.headers["location"] == "/ui/"

        page = client.get("/ui/")
        assert page.status_code == 200
        assert "ExecLedger Console" in page.text
        assert 'id="execution-list"' in page.text
        assert 'src="/ui/app.js"' in page.text

        script = client.get("/ui/app.js")
        assert script.status_code == 200
        assert "EventSource" in script.text
        assert "/v1/executions" in script.text

        styles = client.get("/ui/styles.css")
        assert styles.status_code == 200
        assert ".execution-item" in styles.text


def test_storage_gc_api_dry_run_and_apply(tmp_path: Path):
    app = create_app(tmp_path, workers=1)
    with TestClient(app) as client:
        created = client.post(
            "/v1/executions",
            headers={"Idempotency-Key": "gc-api-key"},
            json={
                "argv": [sys.executable, "-c", "print('done')"],
                "files": {"input.txt": "keep-me"},
            },
        )
        execution_id = created.json()["execution"]["id"]
        assert _wait_terminal(client, execution_id)["status"] == "SUCCEEDED"

        snapshots = client.get(
            f"/v1/executions/{execution_id}/snapshots"
        ).json()
        referenced = {
            item["sha256"]
            for snapshot in snapshots
            for item in snapshot["manifest"]
        }
        assert referenced

        service = app.state.service
        orphan = service.workspaces.blobs.put(b"orphan-api-content")
        assert orphan not in referenced

        workspace = service.workspaces.directory(execution_id)
        assert workspace.exists()

        dry = client.post(
            "/v1/maintenance/gc",
            params={"workspace_older_than_seconds": "0"},
        )
        assert dry.status_code == 200
        dry_payload = dry.json()
        assert dry_payload["dry_run"] is True
        assert orphan in {
            item["digest"] for item in dry_payload["orphan_blobs"]
        }
        assert service.workspaces.blobs.contains(orphan)
        assert execution_id in dry_payload["workspace_dirs_eligible"]
        assert workspace.exists()

        applied = client.post(
            "/v1/maintenance/gc",
            params={
                "apply": "true",
                "workspace_older_than_seconds": "0",
            },
        )
        assert applied.status_code == 200
        applied_payload = applied.json()
        assert applied_payload["dry_run"] is False
        assert orphan in {
            item["digest"] for item in applied_payload["deleted_blobs"]
        }
        assert not service.workspaces.blobs.contains(orphan)
        assert execution_id in applied_payload["workspace_dirs_deleted"]
        assert not workspace.exists()
        for digest in referenced:
            assert service.workspaces.blobs.contains(digest)


def test_readiness_diagnostics_and_prometheus_metrics(tmp_path: Path):
    app = create_app(
        tmp_path,
        workers=1,
        worker_id="diagnostic-worker",
    )
    with TestClient(app) as client:
        ready = client.get("/readyz")
        assert ready.status_code == 200
        ready_payload = ready.json()
        assert ready_payload["ready"] is True
        assert ready_payload["checks"]["database"] is True
        assert ready_payload["worker_id"] == "diagnostic-worker"

        created = client.post(
            "/v1/executions",
            headers={"Idempotency-Key": "diagnostics-key"},
            json={"argv": [sys.executable, "-c", "print('diagnostic')"]},
        )
        execution_id = created.json()["execution"]["id"]
        assert _wait_terminal(client, execution_id)["status"] == "SUCCEEDED"

        diagnostics = client.get("/v1/diagnostics")
        assert diagnostics.status_code == 200
        payload = diagnostics.json()
        assert payload["worker_id"] == "diagnostic-worker"
        assert payload["workers_configured"] == 1
        assert payload["status_counts"]["SUCCEEDED"] >= 1
        assert payload["snapshots_total"] >= 2
        assert payload["blobs_total"] >= 0
        assert payload["database_bytes"] > 0

        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        assert "text/plain" in metrics.headers["content-type"]
        assert "execledger_queue_ready " in metrics.text
        assert 'execledger_executions{status="SUCCEEDED"}' in metrics.text
        assert "execledger_database_bytes " in metrics.text
