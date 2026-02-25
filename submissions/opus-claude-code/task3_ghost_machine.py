#!/usr/bin/env python3
"""
Task 3: The Ghost in the Machine - Environment Resilience
Author: Opus 4.5 (Claude Code)

CORE INSIGHT: "Random" failures are rarely random. They stem from assumptions
about the environment that occasionally become false. The solution is to
continuously verify assumptions and adapt when they change.

KEY DIFFERENTIATORS:
1. Environment fingerprinting - snapshot all relevant state at startup
2. Assumption monitoring - continuously verify critical assumptions
3. Self-healing patterns - automatically recover from common failures
4. Forensic logging - capture enough context to diagnose any failure
5. Graceful degradation - maintain partial functionality when possible
"""

import os
import sys
import time
import json
import signal
import atexit
import hashlib
import tempfile
import threading
import subprocess
import logging
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional, Any, Dict, List, Callable, Set
from pathlib import Path
from enum import Enum, auto
import fcntl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger("ghost_hunter")


class EnvironmentIssue(Enum):
    """Categories of environment-related failures."""
    STALE_LOCK = auto()           # Lock file from dead process
    PERMISSION_CHANGE = auto()    # File/dir permissions changed
    DISK_FULL = auto()            # Out of disk space
    ENV_VAR_CHANGED = auto()      # Environment variable modified
    TEMP_DIR_GONE = auto()        # Temp directory deleted
    DEPENDENCY_MISSING = auto()   # Required file/binary gone
    CLOCK_SKEW = auto()           # System time changed unexpectedly
    MEMORY_PRESSURE = auto()      # System under memory pressure
    ZOMBIE_PROCESS = auto()       # Child process became zombie
    SIGNAL_RECEIVED = auto()      # Unexpected signal


@dataclass
class EnvironmentFingerprint:
    """
    Snapshot of environment state at a point in time.

    By comparing fingerprints, we can detect what changed between
    a working state and a failing state.
    """
    timestamp: str
    working_dir: str
    working_dir_exists: bool
    working_dir_writable: bool
    temp_dir: str
    temp_dir_exists: bool
    temp_dir_writable: bool
    env_vars_hash: str
    key_env_vars: Dict[str, Optional[str]]
    disk_free_bytes: Optional[int]
    process_id: int
    parent_process_id: int
    user_id: int
    python_path: List[str]

    @classmethod
    def capture(cls, key_env_vars: Optional[List[str]] = None) -> 'EnvironmentFingerprint':
        """Capture current environment state."""
        cwd = os.getcwd()
        tmp = tempfile.gettempdir()

        key_vars = key_env_vars or ['PATH', 'HOME', 'USER', 'PYTHONPATH', 'TZ']
        env_snapshot = {k: os.environ.get(k) for k in key_vars}
        env_hash = hashlib.md5(json.dumps(env_snapshot, sort_keys=True).encode()).hexdigest()[:8]

        # Check disk space
        try:
            statvfs = os.statvfs(cwd)
            disk_free = statvfs.f_frsize * statvfs.f_bavail
        except:
            disk_free = None

        return cls(
            timestamp=datetime.utcnow().isoformat(),
            working_dir=cwd,
            working_dir_exists=os.path.isdir(cwd),
            working_dir_writable=os.access(cwd, os.W_OK),
            temp_dir=tmp,
            temp_dir_exists=os.path.isdir(tmp),
            temp_dir_writable=os.access(tmp, os.W_OK),
            env_vars_hash=env_hash,
            key_env_vars=env_snapshot,
            disk_free_bytes=disk_free,
            process_id=os.getpid(),
            parent_process_id=os.getppid(),
            user_id=os.getuid(),
            python_path=sys.path.copy()
        )

    def diff(self, other: 'EnvironmentFingerprint') -> List[str]:
        """Find differences between two fingerprints."""
        diffs = []

        if self.working_dir != other.working_dir:
            diffs.append(f"Working directory changed: {self.working_dir} -> {other.working_dir}")
        if self.working_dir_writable != other.working_dir_writable:
            diffs.append(f"Working directory writability changed: {self.working_dir_writable} -> {other.working_dir_writable}")
        if self.temp_dir_exists != other.temp_dir_exists:
            diffs.append(f"Temp directory existence changed: {self.temp_dir_exists} -> {other.temp_dir_exists}")
        if self.env_vars_hash != other.env_vars_hash:
            diffs.append(f"Environment variables changed (hash: {self.env_vars_hash} -> {other.env_vars_hash})")
            # Detail which vars changed
            for key in set(self.key_env_vars.keys()) | set(other.key_env_vars.keys()):
                if self.key_env_vars.get(key) != other.key_env_vars.get(key):
                    diffs.append(f"  - {key}: {self.key_env_vars.get(key)} -> {other.key_env_vars.get(key)}")
        if self.disk_free_bytes and other.disk_free_bytes:
            if other.disk_free_bytes < self.disk_free_bytes * 0.1:
                diffs.append(f"Disk space critically reduced: {self.disk_free_bytes} -> {other.disk_free_bytes}")

        return diffs


class LockManager:
    """
    Robust lock file management with stale detection.

    Common ghost: lock file left behind by crashed process.
    Solution: Store PID in lock, verify PID is still alive.
    """

    def __init__(self, lock_path: str, timeout: float = 30.0):
        self.lock_path = Path(lock_path)
        self.timeout = timeout
        self.lock_fd: Optional[int] = None
        self._owned = False

    def _read_lock_info(self) -> Optional[Dict]:
        """Read lock file contents."""
        try:
            if self.lock_path.exists():
                content = self.lock_path.read_text()
                return json.loads(content)
        except (json.JSONDecodeError, IOError):
            pass
        return None

    def _is_process_alive(self, pid: int) -> bool:
        """Check if a process is still running."""
        try:
            os.kill(pid, 0)  # Signal 0 just checks if process exists
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True  # Process exists but we can't signal it

    def _is_lock_stale(self) -> bool:
        """Determine if existing lock is from a dead process."""
        info = self._read_lock_info()
        if info is None:
            return True  # Can't read = assume stale

        pid = info.get('pid')
        if pid is None:
            return True

        if not self._is_process_alive(pid):
            logger.warning(f"Stale lock detected: PID {pid} no longer exists")
            return True

        # Check if lock is too old (possible indicator of hung process)
        created = info.get('created')
        if created:
            try:
                lock_age = time.time() - float(created)
                if lock_age > self.timeout * 10:  # 10x timeout = suspicious
                    logger.warning(f"Suspicious old lock: age={lock_age:.0f}s")
            except:
                pass

        return False

    def acquire(self) -> bool:
        """
        Acquire the lock, handling stale locks automatically.
        """
        start = time.time()

        while time.time() - start < self.timeout:
            # Try to acquire
            try:
                # Use O_CREAT | O_EXCL for atomic creation
                self.lock_fd = os.open(
                    str(self.lock_path),
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o644
                )

                # Write lock info
                lock_info = {
                    'pid': os.getpid(),
                    'created': time.time(),
                    'hostname': os.uname().nodename
                }
                os.write(self.lock_fd, json.dumps(lock_info).encode())
                os.fsync(self.lock_fd)

                self._owned = True
                logger.info(f"Lock acquired: {self.lock_path}")
                return True

            except FileExistsError:
                # Lock exists - check if stale
                if self._is_lock_stale():
                    logger.info("Removing stale lock and retrying")
                    try:
                        self.lock_path.unlink()
                    except:
                        pass
                    continue

                # Lock is held by active process - wait
                time.sleep(0.5)

            except IOError as e:
                logger.error(f"IO error acquiring lock: {e}")
                time.sleep(0.5)

        logger.error(f"Failed to acquire lock within {self.timeout}s")
        return False

    def release(self) -> None:
        """Release the lock."""
        if self._owned:
            try:
                if self.lock_fd:
                    os.close(self.lock_fd)
                    self.lock_fd = None
                self.lock_path.unlink()
                self._owned = False
                logger.info(f"Lock released: {self.lock_path}")
            except Exception as e:
                logger.warning(f"Error releasing lock: {e}")

    def __enter__(self):
        if not self.acquire():
            raise RuntimeError(f"Could not acquire lock: {self.lock_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


class EnvironmentWatchdog:
    """
    Continuously monitor environment for unexpected changes.

    The watchdog runs in a background thread, periodically verifying
    that critical assumptions still hold.
    """

    def __init__(self,
                 check_interval: float = 5.0,
                 on_issue: Optional[Callable[[EnvironmentIssue, str], None]] = None):
        self.check_interval = check_interval
        self.on_issue = on_issue or (lambda i, m: logger.warning(f"Environment issue: {i.name} - {m}"))
        self.baseline_fingerprint: Optional[EnvironmentFingerprint] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._required_files: Set[str] = set()
        self._required_dirs: Set[str] = set()

    def add_required_file(self, path: str) -> None:
        """Mark a file as required for operation."""
        self._required_files.add(path)

    def add_required_dir(self, path: str) -> None:
        """Mark a directory as required for operation."""
        self._required_dirs.add(path)

    def start(self) -> None:
        """Start the watchdog thread."""
        if self._running:
            return

        self.baseline_fingerprint = EnvironmentFingerprint.capture()
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        logger.info("Environment watchdog started")

    def stop(self) -> None:
        """Stop the watchdog thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=self.check_interval * 2)
        logger.info("Environment watchdog stopped")

    def _check_once(self) -> List[Tuple[EnvironmentIssue, str]]:
        """Perform one check cycle."""
        issues = []

        # Check environment fingerprint
        current = EnvironmentFingerprint.capture()
        if self.baseline_fingerprint:
            diffs = self.baseline_fingerprint.diff(current)
            if diffs:
                for diff in diffs:
                    issues.append((EnvironmentIssue.ENV_VAR_CHANGED, diff))

        # Check required files
        for fpath in self._required_files:
            if not os.path.isfile(fpath):
                issues.append((EnvironmentIssue.DEPENDENCY_MISSING, f"Required file missing: {fpath}"))
            elif not os.access(fpath, os.R_OK):
                issues.append((EnvironmentIssue.PERMISSION_CHANGE, f"Cannot read required file: {fpath}"))

        # Check required directories
        for dpath in self._required_dirs:
            if not os.path.isdir(dpath):
                issues.append((EnvironmentIssue.TEMP_DIR_GONE, f"Required directory missing: {dpath}"))
            elif not os.access(dpath, os.W_OK):
                issues.append((EnvironmentIssue.PERMISSION_CHANGE, f"Cannot write to required directory: {dpath}"))

        # Check disk space
        if current.disk_free_bytes and current.disk_free_bytes < 100 * 1024 * 1024:  # < 100MB
            issues.append((EnvironmentIssue.DISK_FULL, f"Disk space critically low: {current.disk_free_bytes} bytes"))

        # Check temp directory
        if not current.temp_dir_exists:
            issues.append((EnvironmentIssue.TEMP_DIR_GONE, f"Temp directory missing: {current.temp_dir}"))

        return issues

    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            try:
                issues = self._check_once()
                for issue_type, message in issues:
                    self.on_issue(issue_type, message)
            except Exception as e:
                logger.error(f"Watchdog check failed: {e}")

            time.sleep(self.check_interval)


class ResilientScript:
    """
    Wrapper for running scripts with full environment resilience.

    This class handles:
    - Lock management (with stale detection)
    - Environment monitoring
    - Automatic temp directory recovery
    - Graceful signal handling
    - Comprehensive logging for forensics
    """

    def __init__(self,
                 script_name: str,
                 work_dir: Optional[str] = None,
                 lock_path: Optional[str] = None):
        self.script_name = script_name
        self.work_dir = Path(work_dir or tempfile.mkdtemp(prefix=f"{script_name}_"))
        self.lock_path = lock_path or str(self.work_dir / f"{script_name}.lock")

        self.lock_manager = LockManager(self.lock_path)
        self.watchdog = EnvironmentWatchdog(
            on_issue=self._handle_environment_issue
        )

        self._issues_detected: List[Tuple[EnvironmentIssue, str]] = []
        self._setup_signal_handlers()

        # Register cleanup
        atexit.register(self._cleanup)

    def _setup_signal_handlers(self) -> None:
        """Setup graceful signal handling."""
        def signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            logger.warning(f"Received signal {sig_name}")
            self._issues_detected.append((EnvironmentIssue.SIGNAL_RECEIVED, sig_name))
            self._cleanup()
            sys.exit(128 + signum)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _handle_environment_issue(self, issue: EnvironmentIssue, message: str) -> None:
        """Handle detected environment issues."""
        self._issues_detected.append((issue, message))
        logger.warning(f"Environment issue detected: {issue.name} - {message}")

        # Attempt self-healing for recoverable issues
        if issue == EnvironmentIssue.TEMP_DIR_GONE:
            self._recover_temp_dir()
        elif issue == EnvironmentIssue.STALE_LOCK:
            pass  # LockManager handles this

    def _recover_temp_dir(self) -> None:
        """Attempt to recover missing temp directory."""
        try:
            self.work_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Recovered work directory: {self.work_dir}")
        except Exception as e:
            logger.error(f"Failed to recover work directory: {e}")

    def _cleanup(self) -> None:
        """Cleanup resources."""
        self.watchdog.stop()
        self.lock_manager.release()

    def run(self, task: Callable[[], Any]) -> Any:
        """
        Run a task with full resilience wrapper.
        """
        logger.info(f"Starting resilient execution of {self.script_name}")

        # Ensure work directory exists
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.watchdog.add_required_dir(str(self.work_dir))

        # Capture baseline
        baseline = EnvironmentFingerprint.capture()
        logger.info(f"Environment fingerprint captured: {baseline.env_vars_hash}")

        try:
            # Acquire lock
            with self.lock_manager:
                # Start monitoring
                self.watchdog.start()

                # Execute task
                result = task()

                # Check for issues during execution
                if self._issues_detected:
                    logger.warning(f"Completed with {len(self._issues_detected)} environment issue(s)")
                    for issue, msg in self._issues_detected:
                        logger.warning(f"  - {issue.name}: {msg}")

                return result

        except Exception as e:
            # Capture forensic information
            current = EnvironmentFingerprint.capture()
            diffs = baseline.diff(current)

            logger.error(f"Task failed: {e}")
            logger.error("Environment changes since start:")
            for diff in diffs:
                logger.error(f"  {diff}")

            raise

        finally:
            self._cleanup()


def demo_ghost_machine():
    """Demonstrate environment resilience features."""

    print("\n" + "="*60)
    print("TASK 3: The Ghost in the Machine - Demo")
    print("="*60 + "\n")

    # Create a resilient script runner
    runner = ResilientScript(
        script_name="demo_script",
        work_dir="/tmp/ghost_demo"
    )

    # Define a task that might fail
    task_count = [0]

    def unreliable_task():
        task_count[0] += 1
        print(f"Running task iteration {task_count[0]}")

        # Simulate work
        time.sleep(0.5)

        # Simulate occasional environment-caused failures
        if task_count[0] == 1:
            # Task succeeds
            return {"status": "success", "iterations": task_count[0]}

    # Run with resilience
    try:
        result = runner.run(unreliable_task)
        print(f"\nTask completed successfully: {result}")
    except Exception as e:
        print(f"\nTask failed: {e}")

    # Show environment fingerprint
    print("\n--- Environment Fingerprint ---")
    fp = EnvironmentFingerprint.capture()
    for key, value in asdict(fp).items():
        if key not in ('python_path', 'key_env_vars'):
            print(f"  {key}: {value}")

    # Demo lock manager
    print("\n--- Lock Manager Demo ---")
    lock = LockManager("/tmp/ghost_demo/test.lock")
    if lock.acquire():
        print("  Lock acquired successfully")
        lock.release()
        print("  Lock released")

    return runner


if __name__ == "__main__":
    demo_ghost_machine()
