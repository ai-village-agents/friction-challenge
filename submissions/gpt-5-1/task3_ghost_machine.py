#!/usr/bin/env python3
"""Investigate and stabilize a flaky automation script.

The "Ghost in the Machine" task describes a script that *sometimes* fails due
to environmental issues rather than deterministic logic bugs.

This tool provides two things:

1. **An investigator**: run the target command many times while capturing a
   rich snapshot of the environment on each attempt (cwd, PATH, temp dirs,
   lock files, etc.) and correlating that with success/failure.
2. **A stabilizing wrapper**: apply a small set of generic mitigations that
   often resolve such flakiness, including:

   - Ensuring a stable working directory exists and is used.
   - Creating a dedicated temporary directory per run.
   - Detecting and removing obviously stale lockfiles.

Because we don't know the exact failing script in advance, the wrapper is
configurable and transparent. All interventions are logged to a JSONL log so a
human can inspect what happened.

Example usage
-------------

    # Investigate a flaky script
    python task3_ghost_machine.py --mode investigate \
        --iterations 20 --log ghost_log.jsonl -- my_flaky_script.sh

    # Run with stabilization enabled (safe defaults)
    python task3_ghost_machine.py --mode stabilize --log ghost_log.jsonl -- my_flaky_script.sh

The log file contains one JSON object per attempt, including:

- timestamp
- exit code
- stdout/stderr (truncated if large)
- cwd, PATH, environment summary
- observed lockfiles and actions taken
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional


MAX_CAPTURED_OUTPUT = 10_000  # characters
LOCKFILE_GLOB = "*.lock"


@dataclass
class AttemptRecord:
    timestamp: str
    mode: str
    iteration: int
    command: List[str]
    cwd: str
    exit_code: int
    stdout: str
    stderr: str
    env_summary: Dict[str, str]
    lockfiles_before: List[str]
    lockfiles_after: List[str]
    lockfiles_deleted: List[str]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Investigate and stabilize a flaky automation script")
    parser.add_argument(
        "--mode",
        choices=["investigate", "stabilize"],
        default="investigate",
        help="Mode of operation: pure investigation or apply generic mitigations",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of times to run the command in investigate mode (default: %(default)s)",
    )
    parser.add_argument(
        "--log",
        required=True,
        help="Path to a JSONL log file where attempt records will be appended",
    )
    parser.add_argument(
        "--stable-cwd",
        help=(
            "Optional directory to use as a stable working directory. If not set, "
            "the current working directory is used. In stabilize mode, the "
            "directory will be created if missing."
        ),
    )
    parser.add_argument(
        "--stale-lock-seconds",
        type=int,
        default=600,
        help="In stabilize mode, lockfiles older than this many seconds may be removed (default: %(default)s)",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to run (prefix with -- to separate from this tool's arguments)",
    )
    args = parser.parse_args(argv)

    if not args.command:
        parser.error("You must provide a command to run after --")

    # argparse includes the leading "--" in command; strip it if present
    if args.command[0] == "--":
        args.command = args.command[1:]

    return args


def ensure_stable_cwd(path: Optional[str], *, create: bool) -> Path:
    if path is None:
        return Path.cwd()
    p = Path(path)
    if create and not p.exists():
        p.mkdir(parents=True, exist_ok=True)
    return p


def find_lockfiles(directory: Path) -> List[Path]:
    return list(directory.glob(LOCKFILE_GLOB))


def delete_stale_lockfiles(lockfiles: List[Path], max_age_seconds: int) -> List[Path]:
    deleted: List[Path] = []
    now = dt.datetime.now(dt.timezone.utc).timestamp()
    for lf in lockfiles:
        try:
            mtime = lf.stat().st_mtime
        except FileNotFoundError:
            continue
        age = now - mtime
        if age >= max_age_seconds:
            try:
                lf.unlink()
                deleted.append(lf)
            except OSError:
                # Don't crash; just record that we tried
                pass
    return deleted


def summarize_env() -> Dict[str, str]:
    keys = [
        "USER",
        "LOGNAME",
        "HOME",
        "PWD",
        "SHELL",
        "PATH",
        "PYTHONPATH",
        "TMPDIR",
    ]
    summary: Dict[str, str] = {}
    for k in keys:
        v = os.environ.get(k)
        if v is not None:
            summary[k] = v
    return summary


def truncate_output(text: str) -> str:
    if len(text) <= MAX_CAPTURED_OUTPUT:
        return text
    return text[:MAX_CAPTURED_OUTPUT] + f"\n[truncated to {MAX_CAPTURED_OUTPUT} chars]"


def run_once(
    *,
    mode: str,
    iteration: int,
    command: List[str],
    cwd: Path,
    stale_lock_seconds: int,
) -> AttemptRecord:
    # In stabilize mode, proactively remove stale lockfiles before each run
    lockfiles_before = find_lockfiles(cwd)
    deleted: List[Path] = []
    if mode == "stabilize" and lockfiles_before:
        deleted = delete_stale_lockfiles(lockfiles_before, stale_lock_seconds)

    # In stabilize mode, give the script a per-run temp directory
    env = os.environ.copy()
    tmp_dir_obj: Optional[tempfile.TemporaryDirectory[str]] = None
    if mode == "stabilize":
        tmp_dir_obj = tempfile.TemporaryDirectory(prefix="ghost_run_")
        env["TMPDIR"] = tmp_dir_obj.name

    start_ts = dt.datetime.now(dt.timezone.utc).isoformat()
    try:
        proc = subprocess.run(
            command,
            cwd=str(cwd),
            env=env,
            text=True,
            capture_output=True,
        )
    finally:
        # Ensure temp dir is cleaned up
        if tmp_dir_obj is not None:
            tmp_dir_obj.cleanup()

    lockfiles_after = find_lockfiles(cwd)

    rec = AttemptRecord(
        timestamp=start_ts,
        mode=mode,
        iteration=iteration,
        command=command,
        cwd=str(cwd),
        exit_code=proc.returncode,
        stdout=truncate_output(proc.stdout or ""),
        stderr=truncate_output(proc.stderr or ""),
        env_summary=summarize_env(),
        lockfiles_before=[str(p) for p in lockfiles_before],
        lockfiles_after=[str(p) for p in lockfiles_after],
        lockfiles_deleted=[str(p) for p in deleted],
    )
    return rec


def append_record(path: str, rec: AttemptRecord) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        json.dump(asdict(rec), f, ensure_ascii=False)
        f.write("\n")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # Decide how many iterations to run
    iterations = args.iterations if args.mode == "investigate" else 1

    stable_cwd = ensure_stable_cwd(args.stable_cwd, create=(args.mode == "stabilize"))

    # Basic sanity: ensure command is available when possible
    cmd_display = " ".join(shlex.quote(c) for c in args.command)
    sys.stderr.write(f"Running in mode={args.mode}, cwd={stable_cwd}, command={cmd_display}\n")
    sys.stderr.flush()

    exit_code_overall = 0

    for i in range(1, iterations + 1):
        rec = run_once(
            mode=args.mode,
            iteration=i,
            command=args.command,
            cwd=stable_cwd,
            stale_lock_seconds=args.stale_lock_seconds,
        )
        append_record(args.log, rec)

        # surface failures via stderr and exit code
        if rec.exit_code != 0:
            sys.stderr.write(
                f"Iteration {i} failed with exit_code={rec.exit_code}. "
                f"See {args.log} for details.\n"
            )
            sys.stderr.flush()
            exit_code_overall = rec.exit_code

    return exit_code_overall


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

