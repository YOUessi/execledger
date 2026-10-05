from __future__ import annotations

from execledger.cli import _record_logs, build_parser
from execledger.models import ExecutionRecord, ExecutionSpec, ExecutionStatus, utc_now


def test_cli_reads_default_server_from_environment(monkeypatch):
    monkeypatch.setenv("EXECLEDGER_URL", "http://example.test:9999")
    args = build_parser().parse_args(["list"])
    assert args.url == "http://example.test:9999"


def test_cli_follow_parser_accepts_resume_cursor():
    args = build_parser().parse_args(
        ["follow", "execution-1", "--after", "42", "--events"]
    )
    assert args.command == "follow"
    assert args.execution_id == "execution-1"
    assert args.after == 42
    assert args.events is True


def test_record_logs_can_select_stream(capsys):
    now = utc_now()
    record = ExecutionRecord(
        id="ex",
        status=ExecutionStatus.SUCCEEDED,
        spec=ExecutionSpec(argv=["echo", "ok"]),
        created_at=now,
        updated_at=now,
        stdout="out\n",
        stderr="err\n",
    )

    _record_logs(record, "stdout")
    captured = capsys.readouterr()
    assert captured.out == "out\n"
    assert captured.err == ""

    _record_logs(record, "stderr")
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "err\n"


def test_cli_snapshot_diff_and_restore_parsers():
    diff_args = build_parser().parse_args(
        ["diff", "execution-1", "before-1", "after-1"]
    )
    assert diff_args.command == "diff"
    assert diff_args.execution_id == "execution-1"
    assert diff_args.before_snapshot_id == "before-1"
    assert diff_args.after_snapshot_id == "after-1"

    restore_args = build_parser().parse_args(
        ["restore", "execution-1", "snapshot-1"]
    )
    assert restore_args.command == "restore"
    assert restore_args.execution_id == "execution-1"
    assert restore_args.snapshot_id == "snapshot-1"


def test_cli_gc_parser_defaults_to_dry_run():
    args = build_parser().parse_args(["gc"])
    assert args.command == "gc"
    assert args.apply is False
    assert args.restore_older_than_seconds is None
    assert args.workspace_older_than_seconds is None


def test_cli_gc_parser_accepts_apply_and_restore_retention():
    args = build_parser().parse_args(
        [
            "gc",
            "--apply",
            "--restore-older-than-seconds",
            "3600",
            "--workspace-older-than-seconds",
            "7200",
        ]
    )
    assert args.apply is True
    assert args.restore_older_than_seconds == 3600
    assert args.workspace_older_than_seconds == 7200


def test_cli_readiness_and_diagnostics_parsers():
    ready = build_parser().parse_args(["ready"])
    assert ready.command == "ready"

    diagnostics = build_parser().parse_args(["diagnostics"])
    assert diagnostics.command == "diagnostics"
