from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import uvicorn

from execledger.client import ExecLedgerClient
from execledger.models import ExecutionRecord, ExecutionSpec


def _url_default() -> str:
    return os.environ.get("EXECLEDGER_URL", "http://127.0.0.1:8080")


def _add_url(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--url", default=_url_default())


def _json(value: object) -> None:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    elif isinstance(value, list):
        value = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in value
        ]
    print(json.dumps(value, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="execledger")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Run the HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--root", type=Path, default=Path(".execledger"))
    serve.add_argument("--workers", type=int, default=2)
    serve.add_argument("--worker-id")
    serve.add_argument("--lease-seconds", type=float, default=5.0)
    serve.add_argument("--heartbeat-interval", type=float)

    check = sub.add_parser("check-spec", help="Validate an execution spec JSON file")
    check.add_argument("path", type=Path)

    submit = sub.add_parser("submit", help="Submit an execution spec")
    _add_url(submit)
    submit.add_argument("path", type=Path)
    submit.add_argument("--idempotency-key", required=True)

    get = sub.add_parser("get", help="Get one execution")
    _add_url(get)
    get.add_argument("execution_id")

    list_parser = sub.add_parser("list", help="List executions")
    _add_url(list_parser)
    list_parser.add_argument("--limit", type=int, default=100)

    cancel = sub.add_parser("cancel", help="Cancel an execution")
    _add_url(cancel)
    cancel.add_argument("execution_id")

    wait = sub.add_parser("wait", help="Wait until an execution becomes terminal")
    _add_url(wait)
    wait.add_argument("execution_id")
    wait.add_argument("--timeout", type=float, default=60.0)
    wait.add_argument("--interval", type=float, default=0.2)

    attempts = sub.add_parser("attempts", help="Show persisted execution attempts")
    _add_url(attempts)
    attempts.add_argument("execution_id")

    effects = sub.add_parser("effects", help="Show the append-only effect log")
    _add_url(effects)
    effects.add_argument("execution_id")
    effects.add_argument("--after", type=int, default=0)
    effects.add_argument("--limit", type=int, default=1000)

    follow = sub.add_parser("follow", help="Follow live execution events and output")
    _add_url(follow)
    follow.add_argument("execution_id")
    follow.add_argument("--after", type=int, default=0)
    follow.add_argument(
        "--events",
        action="store_true",
        help="Print non-output lifecycle events to stderr.",
    )

    snapshots = sub.add_parser("snapshots", help="Show workspace snapshots")
    _add_url(snapshots)
    snapshots.add_argument("execution_id")

    diff = sub.add_parser("diff", help="Compare two workspace snapshots")
    _add_url(diff)
    diff.add_argument("execution_id")
    diff.add_argument("before_snapshot_id")
    diff.add_argument("after_snapshot_id")

    restore = sub.add_parser("restore", help="Restore a snapshot into a new read-only-history copy")
    _add_url(restore)
    restore.add_argument("execution_id")
    restore.add_argument("snapshot_id")

    logs = sub.add_parser("logs", help="Print captured stdout/stderr")
    _add_url(logs)
    logs.add_argument("execution_id")
    logs.add_argument(
        "--stream",
        choices=("stdout", "stderr", "both"),
        default="both",
    )

    return parser


def _record_logs(record: ExecutionRecord, stream: str) -> None:
    if stream in {"stdout", "both"}:
        if stream == "both":
            print("== stdout ==")
        print(record.stdout, end="" if record.stdout.endswith("\n") else "\n")
    if stream in {"stderr", "both"}:
        if stream == "both":
            print("== stderr ==")
        print(
            record.stderr,
            end="" if record.stderr.endswith("\n") or not record.stderr else "\n",
            file=sys.stderr if stream == "stderr" else sys.stdout,
        )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "serve":
        from execledger.api import create_app

        uvicorn.run(
            create_app(
                args.root,
                workers=args.workers,
                lease_seconds=args.lease_seconds,
                heartbeat_interval=args.heartbeat_interval,
                worker_id=args.worker_id,
            ),
            host=args.host,
            port=args.port,
        )
        return 0

    if args.command == "check-spec":
        spec = ExecutionSpec.model_validate_json(args.path.read_text())
        _json(spec)
        return 0

    client = ExecLedgerClient(args.url)

    if args.command == "submit":
        spec = ExecutionSpec.model_validate_json(args.path.read_text())
        _json(client.submit(spec, args.idempotency_key))
        return 0

    if args.command == "get":
        _json(client.get(args.execution_id))
        return 0

    if args.command == "list":
        _json(client.list(limit=args.limit))
        return 0

    if args.command == "cancel":
        _json(client.cancel(args.execution_id))
        return 0

    if args.command == "wait":
        _json(
            client.wait(
                args.execution_id,
                timeout=args.timeout,
                interval=args.interval,
            )
        )
        return 0

    if args.command == "attempts":
        _json(client.attempts(args.execution_id))
        return 0

    if args.command == "effects":
        _json(
            client.effects(
                args.execution_id,
                after=args.after,
                limit=args.limit,
            )
        )
        return 0

    if args.command == "follow":
        for effect in client.iter_events(args.execution_id, after=args.after):
            if effect.kind == "output_chunk":
                stream = effect.payload.get("stream")
                data = effect.payload.get("data")
                if isinstance(data, str):
                    target = sys.stderr if stream == "stderr" else sys.stdout
                    print(data, end="", file=target, flush=True)
            elif args.events:
                print(
                    f"[{effect.seq} {effect.kind}] "
                    f"{json.dumps(effect.payload, sort_keys=True)}",
                    file=sys.stderr,
                    flush=True,
                )
        return 0

    if args.command == "snapshots":
        _json(client.snapshots(args.execution_id))
        return 0

    if args.command == "diff":
        _json(
            client.diff(
                args.execution_id,
                args.before_snapshot_id,
                args.after_snapshot_id,
            )
        )
        return 0

    if args.command == "restore":
        _json(client.restore(args.execution_id, args.snapshot_id))
        return 0

    if args.command == "logs":
        _record_logs(client.get(args.execution_id), args.stream)
        return 0

    print("unknown command", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
