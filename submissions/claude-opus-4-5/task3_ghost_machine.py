#!/usr/bin/env python3
"""
Task 3: The Ghost in the Machine - Environmental Resilience
Author: Claude Opus 4.5

PHILOSOPHY: The "ghost" failures happen because scripts assume their environment
is stable. But environments LIE:
- Lock files persist after crashes (stale locks)
- Environment variables vanish mid-execution (threading, subprocess)
- Temp directories get cleaned while you're using them
- Parallel processes fight over shared state

The solution is DEFENSIVE PROGRAMMING: never trust, always verify, and have
fallbacks for everything.

This implementation demonstrates:
1. Stale lock detection via PID liveness check
2. Atomic lock creation with O_CREAT | O_EXCL
3. Environment variable caching and validation
4. Temp directory resurrection
5. Race condition prevention with atomic operations
"""

import os
import sys
import time
import random
import signal
import tempfile
import threading
import logging
import json
import fcntl
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Callable
from contextlib import contextmanager
from enum import Enum, auto

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class EnvironmentFailure(Enum):
    """Categories of environmental failures."""
    STALE_LOCK = auto()           # Lock file from dead process
    HEARTBEAT_TIMEOUT = auto()    # Lock owner stopped heartbeating
    ENV_VAR_MISSING = auto()      # Required env var not set
    ENV_VAR_DISAPPEARED = auto()  # Env var deleted mid-run
    TEMP_DIR_DELETED = auto()     # Temp directory cleaned up
    RACE_CONDITION = auto()       # Concurrent access conflict
    PERMISSION_DENIED = auto()    # File permission issues


@dataclass
class RecoveryAction:
    """Record of a recovery action taken."""
    failure_type: EnvironmentFailure
    timestamp: str
    description: str
    success: bool
    details: Optional[Dict[str, Any]] = None


# =============================================================================
# LOCK MANAGEMENT - Stale lock detection and atomic acquisition
# =============================================================================

@dataclass
class LockInfo:
    """Information stored in a lock file."""
    pid: int
    hostname: str
    created_at: str
    heartbeat_at: str
    
    def to_json(self) -> str:
        return json.dumps({
            'pid': self.pid,
            'hostname': self.hostname,
            'created_at': self.created_at,
            'heartbeat_at': self.heartbeat_at
        })
    
    @classmethod
    def from_json(cls, data: str) -> 'LockInfo':
        d = json.loads(data)
        return cls(
            pid=d['pid'],
            hostname=d['hostname'],
            created_at=d['created_at'],
            heartbeat_at=d['heartbeat_at']
        )


class RobustLockManager:
    """
    Production-grade lock manager with stale detection.
    
    WHY this complexity: Simple lock files break in predictable ways:
    1. Process crashes -> lock file remains (stale lock)
    2. Two processes check "file exists?" simultaneously -> both proceed (race)
    3. NFS/network filesystems -> flock() doesn't work
    
    Our solution:
    - Store PID in lock file, check if process is alive
    - Use O_CREAT | O_EXCL for atomic creation
    - Heartbeat mechanism for long-running processes
    - Configurable staleness threshold
    """
    
    def __init__(
        self,
        lock_path: Path,
        stale_threshold_seconds: float = 30.0,
        heartbeat_interval: float = 10.0,
        max_acquire_attempts: int = 5,
        acquire_retry_delay: float = 1.0
    ):
        self.lock_path = Path(lock_path)
        self.stale_threshold = stale_threshold_seconds
        self.heartbeat_interval = heartbeat_interval
        self.max_attempts = max_acquire_attempts
        self.retry_delay = acquire_retry_delay
        
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_heartbeat = threading.Event()
        self._held = False
        self.recovery_log: List[RecoveryAction] = []
    
    def _is_process_alive(self, pid: int) -> bool:
        """
        Check if a process is still running.
        
        WHY os.kill(pid, 0): Signal 0 doesn't actually send a signal,
        it just checks if the process exists and we have permission to signal it.
        """
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    
    def _is_lock_stale(self) -> tuple[bool, Optional[str]]:
        """
        Check if existing lock is stale.
        Returns (is_stale, reason).
        """
        if not self.lock_path.exists():
            return False, None
        
        try:
            content = self.lock_path.read_text()
            lock_info = LockInfo.from_json(content)
        except (json.JSONDecodeError, KeyError, IOError) as e:
            # Corrupted lock file - treat as stale
            return True, f"corrupted lock file: {e}"
        
        # Check 1: Is the owning process alive?
        if not self._is_process_alive(lock_info.pid):
            return True, f"owning process {lock_info.pid} is dead"
        
        # Check 2: Has the heartbeat timed out?
        try:
            heartbeat_time = datetime.fromisoformat(lock_info.heartbeat_at)
            age = (datetime.now() - heartbeat_time).total_seconds()
            if age > self.stale_threshold:
                return True, f"heartbeat timeout ({age:.1f}s > {self.stale_threshold}s)"
        except ValueError:
            return True, "invalid heartbeat timestamp"
        
        return False, None
    
    def _remove_stale_lock(self, reason: str) -> bool:
        """Remove a stale lock file."""
        try:
            self.lock_path.unlink()
            self.recovery_log.append(RecoveryAction(
                failure_type=EnvironmentFailure.STALE_LOCK,
                timestamp=datetime.now().isoformat(),
                description=f"Removed stale lock: {reason}",
                success=True
            ))
            logger.info(f"Removed stale lock: {reason}")
            return True
        except OSError as e:
            logger.error(f"Failed to remove stale lock: {e}")
            return False
    
    def _create_lock_atomic(self) -> bool:
        """
        Create lock file atomically using O_CREAT | O_EXCL.
        
        WHY atomic: Without O_EXCL, two processes could both:
        1. Check "file exists?" -> False
        2. Create file
        3. Both think they have the lock
        
        O_EXCL makes the check-and-create atomic at the kernel level.
        """
        lock_info = LockInfo(
            pid=os.getpid(),
            hostname=os.uname().nodename,
            created_at=datetime.now().isoformat(),
            heartbeat_at=datetime.now().isoformat()
        )
        
        try:
            # O_CREAT | O_EXCL = create only if doesn't exist (atomic)
            fd = os.open(
                str(self.lock_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644
            )
            os.write(fd, lock_info.to_json().encode())
            os.close(fd)
            return True
        except FileExistsError:
            return False
        except OSError as e:
            logger.error(f"Lock creation failed: {e}")
            return False
    
    def _start_heartbeat(self) -> None:
        """Start background thread to update lock heartbeat."""
        def heartbeat_loop():
            while not self._stop_heartbeat.wait(self.heartbeat_interval):
                try:
                    content = self.lock_path.read_text()
                    lock_info = LockInfo.from_json(content)
                    lock_info.heartbeat_at = datetime.now().isoformat()
                    self.lock_path.write_text(lock_info.to_json())
                except Exception as e:
                    logger.warning(f"Heartbeat update failed: {e}")
        
        self._stop_heartbeat.clear()
        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
    
    def _stop_heartbeat_thread(self) -> None:
        """Stop the heartbeat background thread."""
        self._stop_heartbeat.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=2.0)
            self._heartbeat_thread = None
    
    def acquire(self) -> bool:
        """
        Acquire the lock with retry and stale detection.
        """
        for attempt in range(self.max_attempts):
            # Check for stale lock
            is_stale, reason = self._is_lock_stale()
            if is_stale and reason:
                self._remove_stale_lock(reason)
            
            # Try atomic creation
            if self._create_lock_atomic():
                self._held = True
                self._start_heartbeat()
                logger.info(f"Lock acquired on attempt {attempt + 1}")
                return True
            
            # Lock held by another process - wait and retry
            delay = self.retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
            logger.info(f"Lock busy, waiting {delay:.2f}s before retry {attempt + 2}")
            time.sleep(delay)
        
        logger.error(f"Failed to acquire lock after {self.max_attempts} attempts")
        return False
    
    def release(self) -> None:
        """Release the lock."""
        if not self._held:
            return
        
        self._stop_heartbeat_thread()
        
        try:
            # Verify we own the lock before deleting
            content = self.lock_path.read_text()
            lock_info = LockInfo.from_json(content)
            if lock_info.pid == os.getpid():
                self.lock_path.unlink()
                logger.info("Lock released")
            else:
                logger.warning(f"Lock owned by PID {lock_info.pid}, not releasing")
        except Exception as e:
            logger.error(f"Error releasing lock: {e}")
        
        self._held = False
    
    @contextmanager
    def locked(self):
        """Context manager for lock acquisition."""
        acquired = self.acquire()
        if not acquired:
            raise RuntimeError("Failed to acquire lock")
        try:
            yield
        finally:
            self.release()


# =============================================================================
# ENVIRONMENT CACHING - Protection against disappearing env vars
# =============================================================================

class EnvironmentGuard:
    """
    Caches environment variables and detects mid-run changes.
    
    WHY: In complex systems, env vars can disappear:
    - Subprocess overwrites environment
    - Another thread calls os.environ.clear()
    - Container orchestration modifies environment
    
    We cache required variables at startup and use cached values.
    """
    
    def __init__(self, required_vars: List[str]):
        self.required_vars = required_vars
        self._cache: Dict[str, str] = {}
        self._validated = False
        self.recovery_log: List[RecoveryAction] = []
    
    def validate_and_cache(self) -> tuple[bool, List[str]]:
        """
        Validate all required vars exist and cache them.
        Returns (success, missing_vars).
        """
        missing = []
        for var in self.required_vars:
            value = os.environ.get(var)
            if value is None:
                missing.append(var)
            else:
                self._cache[var] = value
        
        self._validated = len(missing) == 0
        
        if missing:
            self.recovery_log.append(RecoveryAction(
                failure_type=EnvironmentFailure.ENV_VAR_MISSING,
                timestamp=datetime.now().isoformat(),
                description=f"Missing env vars: {missing}",
                success=False
            ))
        
        return self._validated, missing
    
    def get(self, var: str, fallback: Optional[str] = None) -> Optional[str]:
        """
        Get variable from cache, with live check and recovery.
        """
        # First try cache
        if var in self._cache:
            # Verify still in environment
            live_value = os.environ.get(var)
            if live_value != self._cache[var]:
                # Environment changed! Log and use cache
                self.recovery_log.append(RecoveryAction(
                    failure_type=EnvironmentFailure.ENV_VAR_DISAPPEARED,
                    timestamp=datetime.now().isoformat(),
                    description=f"Env var {var} changed/disappeared, using cached value",
                    success=True,
                    details={
                        'var': var,
                        'cached': self._cache[var][:20] + '...',
                        'live': str(live_value)[:20] + '...' if live_value else None
                    }
                ))
                logger.warning(f"Env var {var} disappeared, using cached value")
            return self._cache[var]
        
        # Not in cache - try live
        value = os.environ.get(var, fallback)
        if value:
            self._cache[var] = value
        return value


# =============================================================================
# TEMP DIRECTORY MANAGEMENT - Resurrection after cleanup
# =============================================================================

class ResilientTempDir:
    """
    Temp directory that recreates itself if deleted.
    
    WHY: System cleanup processes (tmpwatch, systemd-tmpfiles) can delete
    temp directories while your script is running. Instead of crashing,
    we detect the deletion and recreate.
    """
    
    def __init__(self, base_dir: Optional[Path] = None, prefix: str = "resilient_"):
        self.base_dir = Path(base_dir) if base_dir else Path(tempfile.gettempdir())
        self.prefix = prefix
        self._path: Optional[Path] = None
        self.recovery_log: List[RecoveryAction] = []
        self._resurrection_count = 0
    
    def _create(self) -> Path:
        """Create the temp directory."""
        self._path = Path(tempfile.mkdtemp(prefix=self.prefix, dir=self.base_dir))
        return self._path
    
    @property
    def path(self) -> Path:
        """Get path, recreating if necessary."""
        if self._path is None:
            return self._create()
        
        if not self._path.exists():
            # Directory was deleted - resurrect it!
            self._resurrection_count += 1
            self.recovery_log.append(RecoveryAction(
                failure_type=EnvironmentFailure.TEMP_DIR_DELETED,
                timestamp=datetime.now().isoformat(),
                description=f"Temp dir deleted, recreating (resurrection #{self._resurrection_count})",
                success=True,
                details={'original_path': str(self._path)}
            ))
            logger.warning(f"Temp directory {self._path} was deleted, recreating")
            self._path.mkdir(parents=True, exist_ok=True)
        
        return self._path
    
    def cleanup(self) -> None:
        """Clean up temp directory."""
        if self._path and self._path.exists():
            import shutil
            shutil.rmtree(self._path, ignore_errors=True)
            self._path = None


# =============================================================================
# ATOMIC FILE OPERATIONS - Race condition prevention
# =============================================================================

class AtomicFileWriter:
    """
    Write files atomically to prevent race conditions.
    
    WHY: If two processes write to the same file, you get corruption.
    Atomic write pattern:
    1. Write to temp file
    2. fsync to ensure data is on disk
    3. Rename temp to target (atomic on POSIX)
    """
    
    @staticmethod
    def write(path: Path, content: str) -> None:
        """Write content atomically."""
        temp_path = path.with_suffix('.tmp.' + str(os.getpid()))
        
        try:
            with open(temp_path, 'w') as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            
            # Atomic rename
            os.rename(temp_path, path)
        except Exception:
            # Clean up temp file on error
            if temp_path.exists():
                temp_path.unlink()
            raise


# =============================================================================
# ENVIRONMENT SIMULATOR - Creates realistic failure scenarios
# =============================================================================

class EnvironmentFailureSimulator:
    """
    Simulates environmental failures for testing.
    """
    
    def __init__(self, work_dir: Path):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.work_dir / "process.lock"
    
    def create_stale_lock(self) -> None:
        """Create a lock file with a dead PID."""
        stale_info = LockInfo(
            pid=99999,  # Almost certainly doesn't exist
            hostname="dead-host",
            created_at="2020-01-01T00:00:00",
            heartbeat_at="2020-01-01T00:00:00"
        )
        self.lock_path.write_text(stale_info.to_json())
        logger.info(f"Created stale lock at {self.lock_path}")
    
    def delete_env_var(self, var: str) -> Optional[str]:
        """Delete an environment variable, return old value."""
        old_value = os.environ.pop(var, None)
        if old_value:
            logger.info(f"Deleted env var {var}")
        return old_value
    
    def delete_temp_dir(self, temp_dir: Path) -> None:
        """Delete a temp directory to simulate cleanup."""
        import shutil
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            logger.info(f"Deleted temp dir {temp_dir}")


# =============================================================================
# MAIN DEMONSTRATION
# =============================================================================

def main():
    """Demonstrate environmental resilience patterns."""
    
    print("=" * 70)
    print("TASK 3: THE GHOST IN THE MACHINE")
    print("Demonstrating resilience against environmental failures")
    print("=" * 70)
    print()
    
    # Setup
    work_dir = Path(tempfile.mkdtemp(prefix="ghost_demo_"))
    simulator = EnvironmentFailureSimulator(work_dir)
    
    # Set up environment
    os.environ['DB_CONNECTION_URL'] = 'postgresql://localhost/test'
    os.environ['API_KEY'] = 'secret-key-12345'
    
    print("SCENARIO 1: Stale Lock Detection")
    print("-" * 70)
    
    # Create stale lock
    simulator.create_stale_lock()
    
    # Try to acquire with our robust manager
    lock_manager = RobustLockManager(
        lock_path=simulator.lock_path,
        stale_threshold_seconds=30.0
    )
    
    with lock_manager.locked():
        print("✓ Successfully acquired lock despite stale lock file!")
        print(f"  Recovery actions: {len(lock_manager.recovery_log)}")
        for action in lock_manager.recovery_log:
            print(f"    - {action.description}")
    
    print()
    print("SCENARIO 2: Environment Variable Disappearance")
    print("-" * 70)
    
    # Set up environment guard
    env_guard = EnvironmentGuard(['DB_CONNECTION_URL', 'API_KEY'])
    success, missing = env_guard.validate_and_cache()
    print(f"Initial validation: {'✓' if success else '✗'}")
    
    # Simulate env var disappearing
    old_value = simulator.delete_env_var('DB_CONNECTION_URL')
    
    # Try to get the value - should use cache
    value = env_guard.get('DB_CONNECTION_URL')
    print(f"✓ Retrieved DB_CONNECTION_URL from cache: {value[:30]}...")
    print(f"  Recovery actions: {len(env_guard.recovery_log)}")
    for action in env_guard.recovery_log:
        print(f"    - {action.description}")
    
    # Restore for later tests
    if old_value:
        os.environ['DB_CONNECTION_URL'] = old_value
    
    print()
    print("SCENARIO 3: Temp Directory Resurrection")
    print("-" * 70)
    
    # Create resilient temp dir
    temp_manager = ResilientTempDir(base_dir=work_dir)
    original_path = temp_manager.path
    print(f"Created temp dir: {original_path}")
    
    # Create a file in it
    test_file = temp_manager.path / "important_data.txt"
    test_file.write_text("critical data")
    print(f"Created file: {test_file}")
    
    # Simulate cleanup daemon deleting it
    simulator.delete_temp_dir(original_path)
    
    # Access path again - should resurrect
    resurrected_path = temp_manager.path
    print(f"✓ Temp dir resurrected: {resurrected_path}")
    print(f"  Resurrection count: {temp_manager._resurrection_count}")
    for action in temp_manager.recovery_log:
        print(f"    - {action.description}")
    
    print()
    print("SCENARIO 4: Atomic File Operations")
    print("-" * 70)
    
    # Demonstrate atomic write
    state_file = work_dir / "state.json"
    state = {"counter": 0, "last_update": datetime.now().isoformat()}
    
    AtomicFileWriter.write(state_file, json.dumps(state))
    print(f"✓ Atomically wrote state file: {state_file}")
    print(f"  Content: {state_file.read_text()}")
    
    print()
    print("=" * 70)
    print("SUMMARY: All environmental failures handled gracefully!")
    print("=" * 70)
    
    # Cleanup
    import shutil
    shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
