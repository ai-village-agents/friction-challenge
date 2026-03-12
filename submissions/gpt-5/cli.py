from __future__ import annotations

import argparse
import importlib.util
import importlib.abc
import json
import sys
from pathlib import Path
from types import ModuleType

BASE = Path(__file__).resolve().parent


def _load_module(name: str, rel: str) -> ModuleType:
    p = BASE / rel
    spec = importlib.util.spec_from_file_location(name, str(p))
    if spec is None or getattr(spec, 'loader', None) is None:
        raise RuntimeError(f"cannot load {name} from {p}")
    m = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    assert isinstance(spec.loader, importlib.abc.Loader)
    spec.loader.exec_module(m)  # type: ignore[attr-defined]
    return m


def cmd_task1_get(args: argparse.Namespace) -> int:
    http = _load_module("gpt5_task1_client", "task1_client.py")
    client = http.RobustHttpClient()
    try:
        data = client.request(
            method="GET",
            url=args.url,
            deadline_s=args.deadline,
            timeout_s=args.timeout,
        )
    except Exception as e:  # RobustHttpError or other
        status = getattr(e, "status", None)
        print(json.dumps({"ok": False, "error": str(e), "status": status}))
        return 2
    print(json.dumps({"ok": True, "data": data}))
    return 0


def cmd_task2_repair(args: argparse.Namespace) -> int:
    m = _load_module("gpt5_task2_repair", "task2_repair.py")
    inp = Path(args.input)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    repaired = m.repair_csv(inp.read_text())
    out.write_text(repaired)
    print(json.dumps({"ok": True, "input": str(inp), "output": str(out)}))
    return 0


def cmd_task3_lock(args: argparse.Namespace) -> int:
    m = _load_module("gpt5_task3_env", "task3_env.py")
    p = Path(args.path)
    if args.action == "acquire":
        ok, why = m.acquire_lock(p, stale_after_s=args.stale_after)
        print(json.dumps({"ok": ok, "why": why}))
        return 0 if ok else 1
    elif args.action == "release":
        ok = m.release_lock(p)
        print(json.dumps({"ok": ok}))
        return 0 if ok else 1
    else:
        print("unknown action", file=sys.stderr)
        return 2


def cmd_task3_tmpdir(args: argparse.Namespace) -> int:
    m = _load_module("gpt5_task3_env", "task3_env.py")
    p = m.ensure_tmpdir(args.path)
    print(json.dumps({"ok": True, "path": str(p)}))
    return 0


def cmd_task3_envget(args: argparse.Namespace) -> int:
    m = _load_module("gpt5_task3_env", "task3_env.py")
    if args.invalidate:
        m.invalidate_env_cache(args.key)
    val = m.getenv_cached(args.key, args.default)
    print(json.dumps({"ok": True, "key": args.key, "value": val}))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="gpt5-cli", description="GPT-5 Friction Challenge CLI demos")
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("task1-get", help="GET a URL with resilient client")
    p1.add_argument("url")
    p1.add_argument("--deadline", type=float, default=None)
    p1.add_argument("--timeout", type=float, default=None)
    p1.set_defaults(func=cmd_task1_get)

    p2 = sub.add_parser("task2-repair", help="Repair a CSV file")
    p2.add_argument("input")
    p2.add_argument("output")
    p2.set_defaults(func=cmd_task2_repair)

    p3 = sub.add_parser("task3-lock", help="Acquire or release a file lock")
    p3.add_argument("action", choices=["acquire", "release"])
    p3.add_argument("--path", required=True)
    p3.add_argument("--stale-after", type=float, default=0.0)
    p3.set_defaults(func=cmd_task3_lock)

    p4 = sub.add_parser("task3-tmpdir", help="Ensure a writable tmpdir")
    p4.add_argument("path")
    p4.set_defaults(func=cmd_task3_tmpdir)

    p5 = sub.add_parser("task3-envget", help="Get an env var via the cache (optional invalidate)")
    p5.add_argument("key")
    p5.add_argument("--default", default=None)
    p5.add_argument("--invalidate", action="store_true")
    p5.set_defaults(func=cmd_task3_envget)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
