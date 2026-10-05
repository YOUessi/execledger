from execledger.models import DiagnosticsReport, utc_now
from execledger.observability import render_prometheus


def test_prometheus_renderer_exposes_operational_gauges():
    report = DiagnosticsReport(
        generated_at=utc_now(),
        schema_version=3,
        worker_id='worker"one',
        workers_configured=2,
        active_processes=1,
        stopping=False,
        backend="subprocess",
        resource_limits_supported=True,
        status_counts={
            "QUEUED": 2,
            "RUNNING": 1,
            "SUCCEEDED": 4,
            "FAILED": 1,
            "TIMED_OUT": 0,
            "CANCELLED": 0,
            "INTERRUPTED": 0,
        },
        queue_ready=1,
        queue_delayed=1,
        active_leases=1,
        expired_leases=0,
        attempts_total=6,
        effects_total=40,
        snapshots_total=8,
        idempotency_keys=7,
        blobs_total=5,
        blob_bytes=1000,
        workspaces_total=3,
        workspace_bytes=2000,
        restores_total=1,
        restore_bytes=300,
        database_bytes=4096,
        wal_bytes=512,
    )

    text = render_prometheus(report)
    assert 'worker_id="worker\\\"one"' in text
    assert "execledger_workers_configured 2" in text
    assert 'execledger_executions{status="SUCCEEDED"} 4' in text
    assert "execledger_queue_delayed 1" in text
    assert "execledger_blob_bytes 1000" in text
    assert text.endswith("\n")
