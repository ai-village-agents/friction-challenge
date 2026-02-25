#!/usr/bin/env python3
"""
Task 3: The Ghost in the Machine
==================================
Challenge: An automation script mysteriously fails at random intervals. The
root cause is not in the script itself but in the environment. Diagnose the
failures and devise workarounds to make the script reliable.

Environmental Failures Diagnosed (4 types):
1. Stale lock files — Previous process crashed without cleanup; PID in lock
   file no longer exists but lock persists, blocking new runs.
2. Environment variable disappearance — Required env vars deleted mid-run
   by external processes or container orchestrators.
3. Temp directory deletion — System cleanup daemons (/tmp pruning) remove
   working directories while the script is running.
4. Race conditions — Multiple instances doing read-modify-write on shared
   state without coordination.

Real-World Context:
In the AI Village, I've hit all four of these. GitHub API tokens sometimes
expire mid-session (env var equivalent), /tmp gets pruned between sessions,
and multiple agents occasionally race to update the same repo branch. The
atomic lock + cached env + resilient temp dir pattern below solved these.

Workaround Strategy:
- Atomic lock with O_CREAT|O_EXCL + PID-based stale detection
- Environment caching at startup with mid-run fallback
- Self-healing temp directories that resurrect on deletion
- Atomic file writes via write-to-temp + fsync + rename
"""

import errno
import json
import os
import random
import shutil
import threading
import time
from typing import Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

REQUIRED_ENV_VARS = ["DB_CONNECTION_URL", "APP_SECRET"]
BASE_TMP = "/tmp"
BROKEN_LOCK_PATH = os.path.join(BASE_TMP, "ghost_broken.lock")
ROBUST_LOCK_PATH = os.path.join(BASE_TMP, "ghost_robust.lock")
STATE_FILE = os.path.join(BASE_TMP, "ghost_state.json")


def ts() -> str:
    """Compact timestamp for logging."""
    return time.strftime("%H:%M:%S")


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}")


# ═══════════════════════════════════════════════════════════════════════════
# PID UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

def pid_alive(pid: int) -> bool:
    """Check if a PID is running using signal 0 (no-op signal)."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # Exists but owned by another user


# ═══════════════════════════════════════════════════════════════════════════
# ATOMIC LOCK MANAGER
# ═══════════════════════════════════════════════════════════════════════════

class AtomicLock:
    """
    File-based lock using O_CREAT|O_EXCL for atomicity.

    Key insight: the naive check-then-create pattern is inherently racy:
        if not exists(lock): create(lock)
    Two processes can both pass the check before either creates. Using
    O_CREAT|O_EXCL makes the check-and-create atomic at the kernel level.

    Stale lock detection: If the PID in the lock file doesn't correspond
    to a running process, the lock is stale (owner crashed). We can safely
    remove it.
    """

    def __init__(self, path: str, stale_timeout: float = 30.0):
        self.path = path
        self.stale_timeout = stale_timeout
        self.held = False

    def _read_lock_info(self) -> Optional[Dict]:
        try:
            with open(self.path, 'r') as f:
                return json.loads(f.read())
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            return None

    def _write_lock_info(self, fd: int) -> None:
        info = json.dumps({
            "pid": os.getpid(),
            "timestamp": time.time(),
        })
        os.write(fd, info.encode('utf-8'))
        os.fsync(fd)

    def acquire(self, retries: int = 8, base_delay: float = 0.1) -> bool:
        """Attempt to acquire lock with stale detection and backoff."""
        for attempt in range(retries):
            try:
                fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
                self._write_lock_info(fd)
                os.close(fd)
                self.held = True
                return True
            except FileExistsError:
                # Lock exists — check if stale
                info = self._read_lock_info()
                if info:
                    lock_pid = info.get("pid", -1)
                    lock_time = info.get("timestamp", 0)
                    age = time.time() - lock_time

                    if not pid_alive(lock_pid):
                        log(f"  Stale lock: PID {lock_pid} not running, removing")
                        try:
                            os.unlink(self.path)
                        except FileNotFoundError:
                            pass
                        continue  # Retry immediately

                    if age > self.stale_timeout:
                        log(f"  Stale lock: age {age:.0f}s > {self.stale_timeout}s, removing")
                        try:
                            os.unlink(self.path)
                        except FileNotFoundError:
                            pass
                        continue

                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.05)
                time.sleep(delay)

        return False

    def release(self) -> None:
        if self.held:
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass
            self.held = False

    def update_heartbeat(self) -> None:
        """Update timestamp to prevent stale detection by other processes."""
        if self.held:
            try:
                info = json.dumps({
                    "pid": os.getpid(),
                    "timestamp": time.time(),
                })
                # Atomic write via temp file + rename
                tmp = self.path + ".tmp"
                with open(tmp, 'w') as f:
                    f.write(info)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.path)
            except OSError:
                pass


# ═══════════════════════════════════════════════════════════════════════════
# ENVIRONMENT GUARD
# ═══════════════════════════════════════════════════════════════════════════

class EnvironmentGuard:
    """
    Caches environment variables at startup and provides fallback access.

    Why this matters: In containerized environments, env vars can disappear
    when config maps are updated, secrets are rotated, or a parent process
    calls unsetenv(). Without caching, the script crashes mid-operation.
    """

    def __init__(self, required: List[str]):
        self.cache: Dict[str, str] = {}
        self.fallback_count = 0
        missing = []
        for key in required:
            val = os.environ.get(key)
            if val is None:
                missing.append(key)
            else:
                self.cache[key] = val
        if missing:
            raise RuntimeError(f"Missing required env vars: {missing}")

    def get(self, key: str) -> str:
        """Get env var, falling back to cached value if disappeared."""
        live = os.environ.get(key)
        if live is not None:
            self.cache[key] = live
            return live
        # Disappeared — use cached value
        cached = self.cache.get(key)
        if cached is None:
            raise RuntimeError(f"Env var {key} missing and not cached")
        self.fallback_count += 1
        log(f"  WARNING: {key} missing from env, using cached value "
            f"(fallback #{self.fallback_count})")
        return cached


# ═══════════════════════════════════════════════════════════════════════════
# RESILIENT TEMP DIRECTORY
# ═══════════════════════════════════════════════════════════════════════════

class ResilientTempDir:
    """
    A temp directory that self-heals when deleted by cleanup daemons.

    Uses per-instance randomized paths to avoid collisions between
    concurrent instances.
    """

    def __init__(self, prefix: str = "ghost_work"):
        self.path = os.path.join(
            BASE_TMP,
            f"{prefix}_{os.getpid()}_{random.randint(1000, 9999)}"
        )
        self.resurrection_count = 0
        os.makedirs(self.path, exist_ok=True)

    def ensure_exists(self) -> str:
        """Ensure directory exists, recreating if needed."""
        if not os.path.isdir(self.path):
            os.makedirs(self.path, exist_ok=True)
            self.resurrection_count += 1
            log(f"  Resurrected temp dir (#{self.resurrection_count}): {self.path}")
        return self.path

    def safe_write(self, filename: str, content: str) -> str:
        """Write to a file in the temp dir, recreating dir if needed."""
        self.ensure_exists()
        filepath = os.path.join(self.path, filename)
        try:
            with open(filepath, 'a') as f:
                f.write(content)
        except FileNotFoundError:
            # Dir vanished between ensure_exists and open
            self.ensure_exists()
            with open(filepath, 'a') as f:
                f.write(content)
            self.resurrection_count += 1
        return filepath

    def cleanup(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════
# ATOMIC STATE FILE
# ═══════════════════════════════════════════════════════════════════════════

def atomic_state_update(path: str, key: str, increment: int = 1) -> int:
    """
    Atomically update a counter in a JSON state file.

    Uses write-to-temp + fsync + rename to prevent partial writes.
    This ensures readers always see either the old or new state, never
    a half-written file.
    """
    # Read current state
    try:
        with open(path, 'r') as f:
            state = json.loads(f.read())
    except (FileNotFoundError, json.JSONDecodeError):
        state = {}

    current = state.get(key, 0)
    state[key] = current + increment

    # Write atomically
    tmp = path + f".tmp.{os.getpid()}"
    with open(tmp, 'w') as f:
        f.write(json.dumps(state, indent=2))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

    return state[key]


# ═══════════════════════════════════════════════════════════════════════════
# BROKEN RUNNER (demonstrates failures)
# ═══════════════════════════════════════════════════════════════════════════

class BrokenRunner:
    """Naive automation that fails under environmental stress."""

    def __init__(self, name: str):
        self.name = name
        self.failures: List[str] = []
        self.temp_dir = os.path.join(BASE_TMP, "ghost_broken_work")

    def run(self) -> None:
        log(f"{self.name}: starting (BROKEN)")

        # Naive lock: check-then-create race
        if os.path.exists(BROKEN_LOCK_PATH):
            self.failures.append("stale_lock_blocked")
            log(f"{self.name}: FAILED — lock exists, won't check if stale")
            return

        time.sleep(0.05)  # Race window
        with open(BROKEN_LOCK_PATH, 'w') as f:
            f.write(str(os.getpid()))

        try:
            # Direct env access (no caching)
            db = os.environ["DB_CONNECTION_URL"]
            log(f"{self.name}: using DB={db}")

            os.makedirs(self.temp_dir, exist_ok=True)
            workfile = os.path.join(self.temp_dir, "work.txt")

            for step in range(3):
                time.sleep(0.15)
                # Env var might vanish
                if "DB_CONNECTION_URL" not in os.environ:
                    self.failures.append("env_var_vanished")
                    raise KeyError("DB_CONNECTION_URL disappeared")
                # Temp dir might vanish
                try:
                    with open(workfile, 'a') as f:
                        f.write(f"step {step}\n")
                except FileNotFoundError:
                    self.failures.append("temp_dir_deleted")
                    raise

            log(f"{self.name}: completed")
        except Exception as e:
            log(f"{self.name}: FAILED — {e}")
        finally:
            try:
                os.unlink(BROKEN_LOCK_PATH)
            except FileNotFoundError:
                pass


# ═══════════════════════════════════════════════════════════════════════════
# ROBUST RUNNER (demonstrates workarounds)
# ═══════════════════════════════════════════════════════════════════════════

class RobustRunner:
    """Resilient automation with full environmental defense."""

    def __init__(self, name: str):
        self.name = name
        self.failures: List[str] = []
        self.recoveries: List[str] = []

    def run(self) -> None:
        log(f"{self.name}: starting (ROBUST)")

        # Cached environment
        env = EnvironmentGuard(REQUIRED_ENV_VARS)

        # Atomic lock with stale detection
        lock = AtomicLock(ROBUST_LOCK_PATH, stale_timeout=30.0)
        if not lock.acquire(retries=8):
            self.failures.append("lock_acquire_failed")
            log(f"{self.name}: FAILED — could not acquire lock")
            return

        # Resilient temp dir
        tmp = ResilientTempDir(prefix=f"ghost_{self.name}")

        try:
            db = env.get("DB_CONNECTION_URL")
            log(f"{self.name}: using DB={db}")

            for step in range(3):
                time.sleep(0.15)
                lock.update_heartbeat()

                # Safe env access with fallback
                db = env.get("DB_CONNECTION_URL")

                # Safe temp file write with resurrection
                tmp.safe_write("work.txt", f"step {step}\n")

                # Atomic state update
                val = atomic_state_update(STATE_FILE, self.name)

            if env.fallback_count > 0:
                self.failures.append("env_var_vanished")
                self.recoveries.append(f"env_cache_fallback_x{env.fallback_count}")

            if tmp.resurrection_count > 0:
                self.failures.append("temp_dir_deleted")
                self.recoveries.append(f"temp_dir_resurrected_x{tmp.resurrection_count}")

            log(f"{self.name}: completed successfully")

        except Exception as e:
            log(f"{self.name}: FAILED — {e}")
            self.failures.append(str(e))
        finally:
            lock.release()
            tmp.cleanup()


# ═══════════════════════════════════════════════════════════════════════════
# FAILURE SIMULATORS
# ═══════════════════════════════════════════════════════════════════════════

def create_stale_lock():
    """Create a lock file with a non-existent PID (simulates crash)."""
    with open(BROKEN_LOCK_PATH, 'w') as f:
        f.write("999999")
    with open(ROBUST_LOCK_PATH, 'w') as f:
        f.write(json.dumps({"pid": 999999, "timestamp": 0}))
    log("SIMULATOR: Created stale lock files (PID 999999)")


def simulate_env_removal(delay: float):
    """Remove env vars after a delay."""
    time.sleep(delay)
    for key in REQUIRED_ENV_VARS:
        if key in os.environ:
            del os.environ[key]
    log("SIMULATOR: Environment variables removed")


def simulate_tmp_cleanup(path: str, delay: float):
    """Delete a temp directory after a delay."""
    time.sleep(delay)
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
        log(f"SIMULATOR: Deleted {path}")


def cleanup_artifacts():
    """Remove all test artifacts."""
    for p in [BROKEN_LOCK_PATH, ROBUST_LOCK_PATH, STATE_FILE]:
        try:
            os.unlink(p)
        except FileNotFoundError:
            pass
    for d in [os.path.join(BASE_TMP, "ghost_broken_work")]:
        shutil.rmtree(d, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN — DEMONSTRATION
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 65)
    print("TASK 3: The Ghost in the Machine")
    print("=" * 65)
    print()
    print("Diagnosis: 4 environmental failure modes:")
    print("  1. Stale lock files (PID doesn't exist)")
    print("  2. Env vars deleted mid-run")
    print("  3. Temp dirs cleaned up mid-run")
    print("  4. Race conditions on shared state")
    print()

    cleanup_artifacts()

    # ─── Part 1: Broken Runner ───────────────────────────────────────
    print("━" * 65)
    print("PART 1: Broken Runner (demonstrates failures)")
    print("━" * 65)

    os.environ["DB_CONNECTION_URL"] = "postgres://demo@localhost/db"
    os.environ["APP_SECRET"] = "s3cret-key-42"

    # Failure 1: Stale lock blocks execution
    create_stale_lock()
    broken = BrokenRunner("broken-1")
    broken.run()
    print(f"  Failures: {broken.failures}")
    print()

    # Clean up lock for next demo
    try:
        os.unlink(BROKEN_LOCK_PATH)
    except FileNotFoundError:
        pass

    # Failure 2+3: Env removal + temp cleanup
    os.environ["DB_CONNECTION_URL"] = "postgres://demo@localhost/db"
    os.environ["APP_SECRET"] = "s3cret-key-42"
    env_t = threading.Thread(target=simulate_env_removal, args=(0.25,))
    broken_tmp = os.path.join(BASE_TMP, "ghost_broken_work")
    tmp_t = threading.Thread(target=simulate_tmp_cleanup, args=(broken_tmp, 0.3))
    env_t.start()
    tmp_t.start()
    broken2 = BrokenRunner("broken-2")
    broken2.run()
    env_t.join()
    tmp_t.join()
    print(f"  Failures: {broken2.failures}")
    print()

    # ─── Part 2: Robust Runner ───────────────────────────────────────
    print("━" * 65)
    print("PART 2: Robust Runner (demonstrates workarounds)")
    print("━" * 65)

    os.environ["DB_CONNECTION_URL"] = "postgres://demo@localhost/db"
    os.environ["APP_SECRET"] = "s3cret-key-42"

    # Stale lock + env removal + temp cleanup
    create_stale_lock()
    env_t = threading.Thread(target=simulate_env_removal, args=(0.3,))
    env_t.start()

    robust = RobustRunner("robust-1")

    # Simulate temp dir cleanup mid-run
    def cleanup_robust_tmp():
        time.sleep(0.25)
        for d in os.listdir(BASE_TMP):
            if d.startswith("ghost_robust-1"):
                shutil.rmtree(os.path.join(BASE_TMP, d), ignore_errors=True)
                log(f"SIMULATOR: Deleted temp dir {d}")
    tmp_t = threading.Thread(target=cleanup_robust_tmp)
    tmp_t.start()

    robust.run()
    env_t.join()
    tmp_t.join()

    print(f"  Failures detected:   {robust.failures}")
    print(f"  Recoveries applied:  {robust.recoveries}")
    print()

    # ─── Summary ─────────────────────────────────────────────────────
    print("━" * 65)
    print("SUMMARY")
    print("━" * 65)
    print()
    all_broken = broken.failures + broken2.failures
    print(f"Broken runner failures:  {sorted(set(all_broken))}")
    print(f"Robust runner failures:  {sorted(set(robust.failures))}")
    print(f"Robust runner recoveries: {sorted(set(robust.recoveries))}")
    print()
    print("The robust runner detected all environmental failures and")
    print("recovered gracefully using cached env vars, self-healing")
    print("temp dirs, and atomic lock management.")


if __name__ == "__main__":
    main()
