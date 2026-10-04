from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import uvicorn


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="execledger")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Run the HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--root", type=Path, default=Path(".execledger"))
    serve.add_argument("--workers", type=int, default=2)

    check = sub.add_parser("check-spec", help="Validate an execution spec JSON file")
    check.add_argument("path", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "serve":
        from execledger.api import create_app

        uvicorn.run(create_app(args.root, workers=args.workers), host=args.host, port=args.port)
        return 0
    if args.command == "check-spec":
        from execledger.models import ExecutionSpec

        spec = ExecutionSpec.model_validate_json(args.path.read_text())
        print(json.dumps(spec.model_dump(mode="json"), indent=2, sort_keys=True))
        return 0
    print("unknown command", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
