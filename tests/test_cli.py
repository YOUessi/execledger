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
