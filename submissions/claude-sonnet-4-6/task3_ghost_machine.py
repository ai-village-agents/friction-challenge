#!/usr/bin/env python3
"""
Task 3: Ghost in the Machine

This script demonstrates a fragile automation runner and then a robust
workaround implementation that survives common environmental failures.

PART 1: AutomationRunner (broken) intentionally fails when:
  1) A stale PID lock file exists.
  2) Environment variable disappears mid-run.
  3) Temp directory is deleted mid-run.
  4) Two instances race on a shared state file.

PART 2: RobustAutomationRunner (fixed) handles all of the above.
"""

import errno
import os
import random
import shutil
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple


REQUIRED_ENV = ["DB_CONNECTION_URL"]
BASE_TMP = "/tmp"
BROKEN_LOCK = os.path.join(BASE_TMP, "automation.lock")
ROBUST_LOCK = os.path.join(BASE_TMP, "automation_robust.lock")
STATE_FILE = os.path.join(BASE_TMP, "automation_state.txt")


def now() -> str:
    return datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]


def log(msg: str) -> None:
    print(f"[{now()}] {msg}")


def pid_exists(pid: int) -> bool:
    """Check if a PID exists on the system."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # PID exists but we do not have permission to signal it.
        return True
    else:
        return True


def atomic_create_lock(path: str, content: str) -> int:
    """Atomically create a lock file, returning its fd or raising FileExistsError."""
    return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)


def write_lock(fd: int, content: str) -> None:
    os.write(fd, content.encode("utf-8"))
    os.fsync(fd)


def read_lock(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


class AutomationRunner:
    """
    Broken automation runner. It intentionally lacks defensive logic and
    demonstrates failures caused by environmental issues.
    """

    def __init__(self, name: str, failures: List[str]):
        self.name = name
        self.failures = failures
        self.temp_dir = os.path.join(BASE_TMP, "automation_work_shared")

    def acquire_lock(self) -> None:
        """Broken lock handling: refuses to run if lock file exists."""
        if os.path.exists(BROKEN_LOCK):
            self.failures.append("stale_pid_lock")
            raise RuntimeError("Lock exists; assuming another instance is running (stale PID not checked).")
        # Intentionally racy: a small delay between check and create lets two instances slip in.
        time.sleep(0.1)
        with open(BROKEN_LOCK, "w", encoding="utf-8") as f:
            f.write(f"pid={os.getpid()}\n")

    def release_lock(self) -> None:
        try:
            os.remove(BROKEN_LOCK)
        except FileNotFoundError:
            pass

    def run(self) -> None:
        log(f"{self.name}: starting (broken runner)")
        self.acquire_lock()
        try:
            # Part 1: environment variable disappears mid-run.
            db_url = os.environ["DB_CONNECTION_URL"]
            log(f"{self.name}: using DB_CONNECTION_URL={db_url}")

            # Part 2: temp directory cleanup mid-run.
            os.makedirs(self.temp_dir, exist_ok=True)
            log(f"{self.name}: temp dir created {self.temp_dir}")
            temp_file = os.path.join(self.temp_dir, "work.txt")
            with open(temp_file, "w", encoding="utf-8") as f:
                f.write("initial data\n")

            # Race condition: naive read-modify-write of shared state file.
            for i in range(2):
                time.sleep(0.2)
                if "DB_CONNECTION_URL" not in os.environ:
                    self.failures.append("env_var_missing")
                    raise KeyError("DB_CONNECTION_URL disappeared mid-run.")
                # Broken shared state update: read, sleep, write without locks.
                try:
                    with open(STATE_FILE, "r", encoding="utf-8") as f:
                        current = int(f.read().strip() or "0")
                except FileNotFoundError:
                    current = 0
                time.sleep(0.15)  # widen the race window
                with open(STATE_FILE, "w", encoding="utf-8") as f:
                    f.write(str(current + 1))
                # Temp dir might be gone now
                try:
                    with open(temp_file, "a", encoding="utf-8") as f:
                        f.write(f"step {i}\n")
                except FileNotFoundError:
                    self.failures.append("temp_dir_deleted")
                    raise

            log(f"{self.name}: completed")
        finally:
            self.release_lock()


class RobustAutomationRunner:
    """
    Robust automation runner with safety checks and recovery mechanisms.
    """

    def __init__(self, name: str, failures: List[str], recoveries: List[str]):
        self.name = name
        self.failures = failures
        self.recoveries = recoveries
        self.temp_dir = os.path.join(BASE_TMP, f"automation_work_{self.name}_{random.randint(1000,9999)}")
        self.env_cache: Dict[str, str] = {}

    def validate_env(self) -> None:
        """Validate required environment variables and cache their values."""
        missing = []
        for key in REQUIRED_ENV:
            val = os.environ.get(key)
            if val is None:
                missing.append(key)
            else:
                self.env_cache[key] = val
        if missing:
            raise RuntimeError(f"Missing required env vars at startup: {', '.join(missing)}")

    def get_env(self, key: str) -> str:
        """Use cached env var if it disappears mid-run."""
        val = os.environ.get(key)
        if val is None:
            self.failures.append("env_var_missing")
            cached = self.env_cache.get(key)
            if cached is None:
                raise RuntimeError(f"Required env var {key} missing and not cached.")
            log(f"{self.name}: WARNING env var {key} missing; using cached value")
            self.recoveries.append("env_var_cached")
            return cached
        self.env_cache[key] = val
        return val

    def acquire_lock(self, retries: int = 6) -> None:
        """
        Acquire a lock atomically with O_CREAT|O_EXCL.
        If lock exists, check for staleness via PID and heartbeat age.
        """
        backoff = 0.1
        for attempt in range(retries):
            try:
                fd = atomic_create_lock(ROBUST_LOCK, "")
                content = f"pid={os.getpid()}\nheartbeat={time.time()}\n"
                write_lock(fd, content)
                os.close(fd)
                log(f"{self.name}: acquired lock")
                return
            except FileExistsError:
                existing = read_lock(ROBUST_LOCK)
                if existing:
                    pid = None
                    heartbeat = None
                    for line in existing.splitlines():
                        if line.startswith("pid="):
                            try:
                                pid = int(line.split("=", 1)[1])
                            except ValueError:
                                pid = None
                        if line.startswith("heartbeat="):
                            try:
                                heartbeat = float(line.split("=", 1)[1])
                            except ValueError:
                                heartbeat = None
                    stale = False
                    if pid is not None and not pid_exists(pid):
                        stale = True
                        self.failures.append("stale_pid_lock")
                        log(f"{self.name}: found stale lock with non-existent PID {pid}")
                    if heartbeat is not None and time.time() - heartbeat > 30:
                        stale = True
                        self.failures.append("stale_heartbeat")
                        log(f"{self.name}: found stale heartbeat (>{30}s)")
                    if stale:
                        try:
                            os.remove(ROBUST_LOCK)
                            self.recoveries.append("stale_lock_removed")
                            log(f"{self.name}: removed stale lock")
                            continue
                        except FileNotFoundError:
                            pass
                log(f"{self.name}: lock busy, retrying in {backoff:.2f}s (attempt {attempt+1})")
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError("Failed to acquire lock after retries.")

    def heartbeat(self) -> None:
        """Update heartbeat timestamp in the lock file."""
        try:
            with open(ROBUST_LOCK, "w", encoding="utf-8") as f:
                f.write(f"pid={os.getpid()}\nheartbeat={time.time()}\n")
        except FileNotFoundError:
            # Lock disappeared; treat as a recoverable issue and re-acquire.
            self.failures.append("lock_missing")
            self.recoveries.append("lock_reacquired")
            self.acquire_lock()

    def release_lock(self) -> None:
        try:
            os.remove(ROBUST_LOCK)
        except FileNotFoundError:
            pass

    def ensure_temp_dir(self) -> None:
        """Ensure temp dir exists; recreate if missing."""
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir, exist_ok=True)
            self.failures.append("temp_dir_missing")
            self.recoveries.append("temp_dir_recreated")
            log(f"{self.name}: recreated temp dir {self.temp_dir}")

    def safe_append(self, path: str, text: str) -> None:
        """Safe temp file write with recovery if temp dir vanished."""
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(text)
        except FileNotFoundError:
            self.failures.append("temp_file_missing")
            self.ensure_temp_dir()
            with open(path, "a", encoding="utf-8") as f:
                f.write(text)
            self.recoveries.append("temp_file_recreated")

    def run(self) -> None:
        log(f"{self.name}: starting (robust runner)")
        self.validate_env()
        self.acquire_lock()
        try:
            db_url = self.get_env("DB_CONNECTION_URL")
            log(f"{self.name}: using DB_CONNECTION_URL={db_url}")

            os.makedirs(self.temp_dir, exist_ok=True)
            log(f"{self.name}: temp dir {self.temp_dir}")
            temp_file = os.path.join(self.temp_dir, "work.txt")

            # Simulate system cleanup deleting temp dir mid-run.
            cleanup_thread = threading.Thread(
                target=simulate_temp_dir_cleanup, args=(self.temp_dir, 0.6)
            )
            cleanup_thread.start()

            # Simulated work with safe operations and heartbeat updates.
            for i in range(3):
                time.sleep(0.2)
                self.heartbeat()
                # Use cached env if it disappears.
                _ = self.get_env("DB_CONNECTION_URL")
                # Use atomic write to shared state via append only (simpler and safe).
                with open(STATE_FILE, "a", encoding="utf-8") as f:
                    f.write(f"{self.name} step {i}\n")
                self.safe_append(temp_file, f"step {i}\n")

            cleanup_thread.join()
            log(f"{self.name}: completed")
        finally:
            self.release_lock()


# --- Failure simulators ---


def simulate_env_var_disappear(delay: float) -> None:
    """Remove DB_CONNECTION_URL after a delay to simulate mid-run disappearance."""
    time.sleep(delay)
    if "DB_CONNECTION_URL" in os.environ:
        del os.environ["DB_CONNECTION_URL"]
        log("SIMULATOR: DB_CONNECTION_URL removed from environment")


def simulate_temp_dir_cleanup(path: str, delay: float) -> None:
    """Delete temp directory after a delay to simulate system cleanup."""
    time.sleep(delay)
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
        log(f"SIMULATOR: temp dir deleted: {path}")


def simulate_parallel_broken_run(failures: List[str]) -> None:
    """Start two broken runners simultaneously to trigger race conditions."""
    def worker(name: str):
        r = AutomationRunner(name, failures)
        try:
            r.run()
        except Exception as e:
            log(f"{name}: failed with {e}")

    # Start two instances that will fight over the same lock/state.
    t1 = threading.Thread(target=worker, args=("broken-A",))
    t2 = threading.Thread(target=worker, args=("broken-B",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()


def simulate_parallel_robust_run(failures: List[str], recoveries: List[str]) -> None:
    """Start two robust runners simultaneously to show lock contention handling."""
    def worker(name: str):
        r = RobustAutomationRunner(name, failures, recoveries)
        try:
            r.run()
        except Exception as e:
            log(f"{name}: failed with {e}")

    t1 = threading.Thread(target=worker, args=("robust-A",))
    t2 = threading.Thread(target=worker, args=("robust-B",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()


def prepare_stale_lock_file() -> None:
    """Create a stale lock file with a PID that doesn't exist and old heartbeat."""
    with open(BROKEN_LOCK, "w", encoding="utf-8") as f:
        f.write("pid=999999\n")
    with open(ROBUST_LOCK, "w", encoding="utf-8") as f:
        f.write("pid=999999\nheartbeat=0\n")
    log("SIMULATOR: created stale lock files")


def cleanup() -> None:
    for path in [BROKEN_LOCK, ROBUST_LOCK, STATE_FILE]:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def main() -> None:
    failures: List[str] = []
    recoveries: List[str] = []

    cleanup()

    log("=== PART 1: Broken automation runner ===")
    prepare_stale_lock_file()

    # Simulate environment variable for broken run.
    os.environ["DB_CONNECTION_URL"] = "postgres://demo@localhost/db"

    # First broken run shows stale PID lock failure.
    simulate_parallel_broken_run(failures)

    # Remove stale lock so the next broken run can start (and fail later).
    try:
        os.remove(BROKEN_LOCK)
    except FileNotFoundError:
        pass

    # Simulate env var disappearance and temp cleanup.
    env_thread = threading.Thread(target=simulate_env_var_disappear, args=(0.4,))
    temp_cleanup_thread = threading.Thread(
        target=simulate_temp_dir_cleanup,
        args=(os.path.join(BASE_TMP, "automation_work_shared"), 0.6),
    )
    env_thread.start()
    temp_cleanup_thread.start()

    simulate_parallel_broken_run(failures)

    env_thread.join()
    temp_cleanup_thread.join()

    # Detect race condition by checking expected vs actual counter.
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            observed = int(f.read().strip() or "0")
    except FileNotFoundError:
        observed = 0
    expected = 4  # two runners * two steps each
    if observed < expected:
        failures.append("race_condition")
        log(f"Broken race detected: expected {expected} but saw {observed}")

    log("=== PART 2: Robust automation runner ===")

    prepare_stale_lock_file()

    # Restore env var for robust run.
    os.environ["DB_CONNECTION_URL"] = "postgres://demo@localhost/db"

    # Simulate disappearance and temp cleanup for robust run as well.
    env_thread = threading.Thread(target=simulate_env_var_disappear, args=(0.4,))
    env_thread.start()

    simulate_parallel_robust_run(failures, recoveries)

    env_thread.join()

    log("=== SUMMARY ===")
    log(f"Failures observed: {sorted(set(failures)) or 'none'}")
    log(f"Recoveries performed: {sorted(set(recoveries)) or 'none'}")


if __name__ == "__main__":
    main()
