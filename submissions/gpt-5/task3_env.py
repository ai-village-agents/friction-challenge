"""Environment/lock hardening utilities (Task 3).

Functions
- acquire_lock(path, stale_after_s): best-effort atomic lock with stale reclamation
- release_lock(path): remove lock if owned by current PID
- getenv_cached(key, default): process-level env cache resilient to churn
- invalidate_env_cache(key=None): invalidate cached key(s)
- ensure_tmpdir(path): ensure directory exists and is writable; race-safe
"""
from __future__ import annotations

import errno
import json
import os
import socket
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional, Tuple


class LockInfo:
    def __init__(self, pid: int, created_at: float, host: str) -> None:
        self.pid = pid
        self.created_at = created_at
        self.host = host

    @staticmethod
    def now() -> "LockInfo":
        return LockInfo(pid=os.getpid(), created_at=time.time(), host=socket.gethostname())


_ENV_CACHE: Dict[str, str] = {}


def getenv_cached(key: str, default: str | None = None) -> Optional[str]:
    if key in _ENV_CACHE:
        return _ENV_CACHE[key]
    val = os.environ.get(key, default)  # snapshot once
    if val is not None:
        _ENV_CACHE[key] = val
    return val


def invalidate_env_cache(key: Optional[str] = None) -> None:
    if key is None:
        _ENV_CACHE.clear()
    else:
        _ENV_CACHE.pop(key, None)


def _read_lock(path: Path) -> Optional[LockInfo]:
    try:
        data = json.loads(path.read_text())
        return LockInfo(pid=int(data.get("pid", -1)), created_at=float(data.get("created_at", 0.0)), host=str(data.get("host", "")))
    except Exception:
        return None


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Exists but not permitted — assume alive to be conservative
        return True
    else:
        return True


def acquire_lock(path: str | Path, stale_after_s: float = 0.0) -> Tuple[bool, str]:
    """Acquire a file lock by creating it atomically.

    Returns (acquired, reason). If the file exists and is considered stale (by
    age or by dead PID), attempt safe reclamation by renaming the stale file and
    retrying exactly once.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    def write_info(fd: int) -> None:
        info = LockInfo.now()
        os.write(fd, json.dumps({"pid": info.pid, "created_at": info.created_at, "host": info.host}).encode("utf-8"))
        os.fsync(fd)

    # First attempt: atomic create
    try:
        fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except OSError as e:
        if e.errno != errno.EEXIST:
            return (False, f"error:{e.errno}")
    else:
        try:
            write_info(fd)
        finally:
            os.close(fd)
        return (True, "created")

    # Exists: check staleness
    li = _read_lock(p)
    now = time.time()
    is_stale = False
    reasons = []
    if li is None:
        reasons.append("unreadable")
    else:
        age = now - li.created_at
        if stale_after_s > 0.0 and age >= stale_after_s:
            is_stale = True
            reasons.append(f"age>={stale_after_s}")
        if not _pid_alive(li.pid):
            is_stale = True
            reasons.append("pid_dead")
    if not is_stale:
        return (False, "held")

    # Attempt reclamation: rename then create
    stale_note = p.with_suffix(p.suffix + f".stale.{int(now)}")
    try:
        p.replace(stale_note)
    except Exception as e:
        # Could not rename; give up conservatively
        return (False, f"reclaim_failed:{e}")

    try:
        fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except OSError as e:
        if e.errno == errno.EEXIST:
            return (False, "raced")
        return (False, f"error:{e.errno}")
    else:
        try:
            write_info(fd)
        finally:
            os.close(fd)
        # Write provenance note
        try:
            stale_note.write_text(json.dumps({
                "reclaimed_by": os.getpid(),
                "reclaimed_at": now,
                "reason": ",".join(reasons),
            }))
        except Exception:
            pass
        return (True, "reclaimed")


def release_lock(path: str | Path) -> bool:
    p = Path(path)
    li = _read_lock(p)
    if li and li.pid != os.getpid():
        return False
    try:
        p.unlink()
        return True
    except FileNotFoundError:
        return True
    except Exception:
        return False


def ensure_tmpdir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    # Validate writability by a temp file
    try:
        with tempfile.NamedTemporaryFile(prefix=".probe-", dir=str(p), delete=True) as _:
            pass
    except Exception as e:
        raise RuntimeError(f"directory not writable: {p}: {e}")
    return p
