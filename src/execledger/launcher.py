from __future__ import annotations

import json
import os
import resource
import signal
import sys
import time
from pathlib import Path


def _bounded_limit(resource_id: int, requested: int) -> tuple[int, int]:
    _, hard = resource.getrlimit(resource_id)
    if hard == resource.RLIM_INFINITY:
        return requested, requested
    value = min(requested, int(hard))
    return value, value


def _apply_limits(limits: dict[str, int | None]) -> None:
    names = {
        "max_cpu_seconds": "RLIMIT_CPU",
        "max_memory_bytes": "RLIMIT_AS",
        "max_file_bytes": "RLIMIT_FSIZE",
        "max_open_files": "RLIMIT_NOFILE",
        "max_processes": "RLIMIT_NPROC",
    }

    if hasattr(resource, "RLIMIT_CORE"):
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

    for key, attribute in names.items():
        value = limits.get(key)
        if value is None:
            continue
        if not hasattr(resource, attribute):
            raise RuntimeError(f"{key} is unsupported on this POSIX platform")
        resource_id = int(getattr(resource, attribute))
        resource.setrlimit(resource_id, _bounded_limit(resource_id, int(value)))


def _write_result(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    if "--" not in argv:
        print("usage: python -m execledger.launcher RESULT.json -- command ...", file=sys.stderr)
        return 2
    separator = argv.index("--")
    if separator != 1 or len(argv) <= 2:
        print("invalid launcher arguments", file=sys.stderr)
        return 2

    result_path = Path(argv[0])
    target = argv[2:]
    limits = json.loads(os.environ.get("EXECLEDGER_RESOURCE_LIMITS", "{}"))
    started = time.monotonic()
    forwarded_signal: int | None = None
    child_pid: int | None = None

    def handle_signal(signum: int, _frame: object) -> None:
        nonlocal forwarded_signal
        forwarded_signal = signum
        if child_pid is not None:
            try:
                os.kill(child_pid, signum)
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    error_read, error_write = os.pipe()
    os.set_inheritable(error_write, False)

    child_pid = os.fork()
    if child_pid == 0:
        os.close(error_read)
        try:
            _apply_limits(limits)
            target_env = dict(os.environ)
            target_env.pop("EXECLEDGER_RESOURCE_LIMITS", None)
            os.execvpe(target[0], target, target_env)
        except BaseException as exc:
            message = f"{type(exc).__name__}: {exc}"
            try:
                os.write(error_write, message.encode(errors="replace"))
            except OSError:
                pass
            os.write(2, f"execledger launcher: {message}\n".encode(errors="replace"))
            os._exit(127)

    os.close(error_write)
    waited_pid, status, usage = os.wait4(child_pid, 0)
    assert waited_pid == child_pid

    try:
        exec_error_raw = os.read(error_read, 65536)
    finally:
        os.close(error_read)
    exec_error = exec_error_raw.decode(errors="replace") or None

    if exec_error is not None:
        target_returncode: int | None = None
    elif os.WIFEXITED(status):
        target_returncode = os.WEXITSTATUS(status)
    elif os.WIFSIGNALED(status):
        target_returncode = -os.WTERMSIG(status)
    else:
        target_returncode = 1

    # Linux reports ru_maxrss in KiB; macOS/BSD report bytes.
    max_rss = int(usage.ru_maxrss)
    if sys.platform.startswith("linux"):
        max_rss *= 1024

    payload = {
        "target_returncode": target_returncode,
        "usage": {
            "wall_time_seconds": max(0.0, time.monotonic() - started),
            "user_cpu_seconds": float(usage.ru_utime),
            "system_cpu_seconds": float(usage.ru_stime),
            "max_rss_bytes": max_rss,
            "voluntary_context_switches": int(usage.ru_nvcsw),
            "involuntary_context_switches": int(usage.ru_nivcsw),
        },
        "forwarded_signal": forwarded_signal,
        "exec_error": exec_error,
    }
    try:
        _write_result(result_path, payload)
    except OSError as exc:
        print(f"execledger launcher: failed to write usage result: {exc}", file=sys.stderr)

    if target_returncode is None:
        return 127
    if target_returncode >= 0:
        return target_returncode
    return 128 + abs(target_returncode)


if __name__ == "__main__":
    raise SystemExit(main())
