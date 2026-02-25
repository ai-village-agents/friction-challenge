#!/usr/bin/env python3
"""Task 3: Ghost machine flaky automation and resilient runner."""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import random
import signal
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, Optional


LOCK_HEARTBEAT_SECS = 2.0
STALE_THRESHOLD = 5.0


def log_event(path: Optional[str], event: str, details: Dict) -> None:
    if not path:
        return
    payload = {
        "timestamp": time.time(),
        "event": event,
        "details": details,
    }
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def atomic_write(path: Path, data: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(data)
    os.replace(tmp, path)


def acquire_lock(lock_path: Path, log_jsonl: Optional[str]) -> bool:
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(json.dumps({"pid": os.getpid(), "heartbeat": time.time()}))
            log_event(log_jsonl, "lock_acquired", {"path": str(lock_path)})
            return True
        except FileExistsError:
            try:
                with open(lock_path, "r", encoding="utf-8") as fh:
                    info = json.load(fh)
                pid = info.get("pid")
                heartbeat = info.get("heartbeat", 0)
            except Exception:  # noqa: BLE001
                pid = None
                heartbeat = 0
            stale = (pid is None) or (not pid_alive(pid)) or (time.time() - heartbeat > STALE_THRESHOLD)
            if stale:
                log_event(log_jsonl, "lock_stale", {"path": str(lock_path), "pid": pid})
                with contextlib.suppress(Exception):
                    os.remove(lock_path)
                continue
            time.sleep(0.1)


def update_heartbeat(lock_path: Path) -> None:
    try:
        data = {"pid": os.getpid(), "heartbeat": time.time()}
        atomic_write(lock_path, json.dumps(data))
    except Exception:
        pass


def sabotager(workdir: Path, lock_path: Path, stop_event: threading.Event, seed: Optional[int]) -> None:
    rng = random.Random(1234 if seed is None else seed + 999)
    while not stop_event.is_set():
        time.sleep(rng.uniform(0.2, 0.5))
        action = rng.choice(["env", "temp", "lock"])
        if action == "env":
            os.environ.pop("REQUIRED_TOKEN", None)
        elif action == "temp":
            if workdir.exists():
                for child in workdir.iterdir():
                    if child.is_file():
                        child.unlink(missing_ok=True)
                workdir.rmdir()
        elif action == "lock":
            lock_path.write_text(json.dumps({"pid": 99999, "heartbeat": time.time() - 100}), encoding="utf-8")


def run_steps(
    workdir: Path,
    lock_path: Path,
    stable: bool,
    log_jsonl: Optional[str],
    deadline: float,
    max_attempts: int,
    seed: Optional[int],
) -> bool:
    required_env = {"REQUIRED_TOKEN": os.environ.get("REQUIRED_TOKEN", "token123")}
    start = time.monotonic()
    base_seed = seed if seed is not None else (7 if stable else 3)
    rng = random.Random(base_seed)

    def ensure_env():
        for k, v in required_env.items():
            os.environ[k] = v

    attempt = 0
    while attempt < max_attempts and time.monotonic() - start < deadline:
        attempt += 1
        ensure_env()
        try:
            ensure_dir(workdir)
            if stable:
                if not acquire_lock(lock_path, log_jsonl):
                    continue
            else:
                if lock_path.exists():
                    return False
            state_file = workdir / "state.txt"
            value = rng.randint(1, 1000)
            if stable:
                atomic_write(state_file, str(value))
                update_heartbeat(lock_path)
            else:
                with open(state_file, "w", encoding="utf-8") as fh:
                    fh.write(str(value))
            log_event(log_jsonl, "step", {"attempt": attempt, "value": value})
            if stable:
                with open(lock_path, "r", encoding="utf-8") as fh:
                    _ = fh.read()
            return True
        except Exception as exc:  # noqa: BLE001
            log_event(log_jsonl, "failure", {"attempt": attempt, "error": str(exc)})
            if stable:
                time.sleep(min(0.5, deadline - (time.monotonic() - start)))
            else:
                return False
        finally:
            if stable:
                with contextlib.suppress(Exception):
                    os.remove(lock_path)
    return False


def run_flaky(args: argparse.Namespace) -> int:
    workdir = Path(args.workdir or tempfile.mkdtemp())
    lock_path = Path(args.lock_path or workdir / "lock.json")
    ok = run_steps(
        workdir,
        lock_path,
        stable=False,
        log_jsonl=args.log_jsonl,
        deadline=3,
        max_attempts=1,
        seed=args.seed,
    )
    print("flaky_success" if ok else "flaky_failed")
    return 0 if ok else 1


def run_stable(args: argparse.Namespace) -> int:
    workdir = Path(args.workdir or tempfile.mkdtemp())
    lock_path = Path(args.lock_path or workdir / "lock.json")
    ok = run_steps(
        workdir,
        lock_path,
        stable=True,
        log_jsonl=args.log_jsonl,
        deadline=args.deadline_secs,
        max_attempts=args.max_attempts,
        seed=args.seed,
    )
    print("stable_success" if ok else "stable_failed")
    return 0 if ok else 1


def run_demo(args: argparse.Namespace) -> int:
    tmpdir = Path(tempfile.mkdtemp())
    lock_path = tmpdir / "lock.json"
    stop = threading.Event()
    saboteur = threading.Thread(
        target=sabotager, args=(tmpdir, lock_path, stop, args.seed), daemon=True
    )
    os.environ["REQUIRED_TOKEN"] = "demo-token"
    saboteur.start()
    try:
        demo_args = argparse.Namespace(**vars(args))
        demo_args.workdir = tmpdir
        demo_args.lock_path = lock_path
        f_status = run_flaky(demo_args)
        s_status = run_stable(demo_args)
    finally:
        stop.set()
        saboteur.join(timeout=1)
    return 0 if s_status == 0 else 1


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["run_flaky", "run_stable", "demo"], required=True)
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--lock-path", type=Path)
    parser.add_argument("--max-attempts", type=int, default=20)
    parser.add_argument("--deadline-secs", type=float, default=20.0)
    parser.add_argument("--log-jsonl")
    parser.add_argument("--seed", type=int)
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.seed is not None:
        random.seed(args.seed)

    if args.mode == "run_flaky":
        return run_flaky(args)
    if args.mode == "run_stable":
        return run_stable(args)
    return run_demo(args)


if __name__ == "__main__":
    raise SystemExit(main())
