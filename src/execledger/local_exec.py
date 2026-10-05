from __future__ import annotations

import json
import os
import sys

_INTERNAL_LIMITS_ENV = "EXECLEDGER_INTERNAL_RESOURCE_LIMITS"


def _bounded(requested: int, hard: int) -> int:
    if hard < 0:
        return requested
    return min(requested, hard)


def _apply_one(resource_module, name: str, requested: int) -> None:
    resource_name = {
        "cpu_seconds": "RLIMIT_CPU",
        "memory_bytes": "RLIMIT_AS",
        "file_size_bytes": "RLIMIT_FSIZE",
        "open_files": "RLIMIT_NOFILE",
    }[name]
    if not hasattr(resource_module, resource_name):
        raise RuntimeError(f"host does not support {resource_name}")

    resource_id = getattr(resource_module, resource_name)
    _, hard = resource_module.getrlimit(resource_id)
    effective = _bounded(requested, hard)
    resource_module.setrlimit(resource_id, (effective, effective))


def apply_limits(limits: dict[str, int]) -> None:
    if os.name != "posix":
        raise RuntimeError("kernel resource limits require POSIX")
    import resource

    for name in (
        "cpu_seconds",
        "memory_bytes",
        "file_size_bytes",
        "open_files",
    ):
        value = limits.get(name)
        if value is not None:
            _apply_one(resource, name, int(value))


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] != "--" or len(args) == 1:
        print("execledger.local_exec requires '-- <command> ...'", file=sys.stderr)
        return 2

    raw = os.environ.pop(_INTERNAL_LIMITS_ENV, "{}")
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("resource limits payload must be an object")
        limits = {str(key): int(value) for key, value in parsed.items()}
        apply_limits(limits)
    except Exception as exc:
        print(f"ExecLedger failed to apply resource limits: {exc}", file=sys.stderr)
        return 126

    command = args[1:]
    try:
        os.execvpe(command[0], command, os.environ)
    except OSError as exc:
        print(f"ExecLedger failed to exec target: {exc}", file=sys.stderr)
        return 127


if __name__ == "__main__":
    raise SystemExit(main())
