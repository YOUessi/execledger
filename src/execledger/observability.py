from __future__ import annotations

from execledger.models import DiagnosticsReport


def render_prometheus(report: DiagnosticsReport) -> str:
    """Render a compact Prometheus/OpenMetrics-compatible text exposition."""
    lines = [
        "# HELP execledger_info Static build/runtime information.",
        "# TYPE execledger_info gauge",
        (
            'execledger_info{worker_id="'
            + report.worker_id.replace("\\", "\\\\").replace('"', '\\"')
            + f'",schema_version="{report.schema_version}"}} 1'
        ),
        "# HELP execledger_workers_configured Configured worker slots.",
        "# TYPE execledger_workers_configured gauge",
        f"execledger_workers_configured {report.workers_configured}",
        "# HELP execledger_active_processes Child processes currently owned by this service.",
        "# TYPE execledger_active_processes gauge",
        f"execledger_active_processes {report.active_processes}",
        "# HELP execledger_queue_ready Queued executions immediately claimable.",
        "# TYPE execledger_queue_ready gauge",
        f"execledger_queue_ready {report.queue_ready}",
        "# HELP execledger_queue_delayed Queued executions waiting for retry backoff.",
        "# TYPE execledger_queue_delayed gauge",
        f"execledger_queue_delayed {report.queue_delayed}",
        "# HELP execledger_active_leases Running executions with a valid lease.",
        "# TYPE execledger_active_leases gauge",
        f"execledger_active_leases {report.active_leases}",
        "# HELP execledger_expired_leases Running executions whose lease is expired or missing.",
        "# TYPE execledger_expired_leases gauge",
        f"execledger_expired_leases {report.expired_leases}",
    ]

    lines.extend(
        [
            "# HELP execledger_executions Executions by durable status.",
            "# TYPE execledger_executions gauge",
        ]
    )
    for status, count in sorted(report.status_counts.items()):
        lines.append(f'execledger_executions{{status="{status}"}} {count}')

    scalar_metrics = {
        "execledger_attempts_total": report.attempts_total,
        "execledger_effects_total": report.effects_total,
        "execledger_snapshots_total": report.snapshots_total,
        "execledger_idempotency_keys": report.idempotency_keys,
        "execledger_blobs_total": report.blobs_total,
        "execledger_blob_bytes": report.blob_bytes,
        "execledger_workspaces_total": report.workspaces_total,
        "execledger_workspace_bytes": report.workspace_bytes,
        "execledger_restores_total": report.restores_total,
        "execledger_restore_bytes": report.restore_bytes,
        "execledger_database_bytes": report.database_bytes,
        "execledger_wal_bytes": report.wal_bytes,
    }
    for name, value in scalar_metrics.items():
        lines.append(f"# TYPE {name} gauge")
        lines.append(f"{name} {value}")

    return "\n".join(lines) + "\n"
