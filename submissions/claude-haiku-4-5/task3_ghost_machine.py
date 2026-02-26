#!/usr/bin/env python3
"""
Task 3: The Ghost in the Machine
=================================
Problem: Automation script fails mysteriously at random intervals due to 
environmental issues (not script bugs). Must diagnose and work around.

Environmental failure modes:
- Stale PID lock file (PID doesn't exist anymore)
- Heartbeat timeout (lock holder silently died)
- Missing environment variables mid-run
- Temp directory deleted by system cleanup
- Race condition on shared state file

Workaround: Environment monitoring, atomic lock operations, caching, recovery.
"""

import os
import json
import time
import signal
import atexit
import tempfile
from pathlib import Path


class EnvironmentDiagnostics:
    """Monitor and diagnose environmental issues."""
    
    def __init__(self):
        self.env_cache = {}
        self.lock_file = None
        self.temp_dir = None
        self.diagnostics = {
            "failures": {},
            "recoveries": {}
        }
    
    def validate_env_vars(self, required_vars):
        """Cache required env vars at startup."""
        print("[Env] Validating required environment variables...")
        for var in required_vars:
            if var in os.environ:
                self.env_cache[var] = os.environ[var]
                print(f"  ✓ {var}={self.env_cache[var][:30]}")
            else:
                print(f"  ✗ {var} not found (will use cache if available)")
    
    def get_env(self, var, fallback=None):
        """Get env var with caching fallback."""
        if var in os.environ:
            return os.environ[var]
        
        if var in self.env_cache:
            self._record_recovery("env_var_cached", var)
            return self.env_cache[var]
        
        if fallback:
            self._record_recovery("env_var_fallback", f"{var}={fallback}")
            return fallback
        
        self._record_failure("env_var_missing", var)
        raise RuntimeError(f"Environment variable {var} not available")
    
    def setup_locking(self, lock_path="/tmp/automation.lock"):
        """Setup atomic locking with stale detection."""
        self.lock_file = lock_path
        max_retries = 5
        backoff = 0.1
        
        for attempt in range(1, max_retries + 1):
            try:
                # Check for stale lock
                if os.path.exists(lock_path):
                    try:
                        with open(lock_path, 'r') as f:
                            lock_data = json.load(f)
                            pid = lock_data.get("pid")
                            heartbeat_age = time.time() - lock_data.get("heartbeat", 0)
                            
                            # Check if PID still exists
                            if pid and os.path.exists(f"/proc/{pid}"):
                                if heartbeat_age > 30:
                                    print(f"[Lock] Stale heartbeat ({heartbeat_age:.1f}s old), removing...")
                                    self._record_recovery("stale_lock_removed", f"pid={pid}, age={heartbeat_age:.1f}s")
                                    os.remove(lock_path)
                                else:
                                    print(f"[Lock] Lock held by PID {pid}, waiting...")
                                    time.sleep(backoff)
                                    backoff = min(backoff * 2, 5)
                                    continue
                            else:
                                print(f"[Lock] PID {pid} not found, removing stale lock...")
                                self._record_recovery("stale_pid_lock_removed", f"pid={pid}")
                                os.remove(lock_path)
                    except (json.JSONDecodeError, OSError):
                        os.remove(lock_path)
                
                # Try atomic lock creation
                try:
                    fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                    lock_data = {
                        "pid": os.getpid(),
                        "heartbeat": time.time()
                    }
                    os.write(fd, json.dumps(lock_data).encode())
                    os.close(fd)
                    print(f"[Lock] Acquired lock (PID {os.getpid()})")
                    
                    # Setup cleanup on exit
                    atexit.register(self._cleanup_lock)
                    return
                except FileExistsError:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 5)
            
            except Exception as e:
                print(f"[Lock] Error on attempt {attempt}: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 5)
        
        raise RuntimeError("Failed to acquire lock after max retries")
    
    def setup_temp_dir(self):
        """Setup temp directory with recovery."""
        dir_name = f"/tmp/automation_work_{os.getpid()}"
        self.temp_dir = dir_name
        
        try:
            os.makedirs(dir_name, exist_ok=True)
            print(f"[Temp] Created temp directory: {dir_name}")
        except OSError as e:
            print(f"[Temp] Failed to create temp dir: {e}")
            # Fallback to system temp
            self.temp_dir = tempfile.gettempdir()
            self._record_failure("temp_dir_creation_failed", str(e))
    
    def ensure_temp_dir(self):
        """Ensure temp dir exists, recreate if needed."""
        if not os.path.exists(self.temp_dir):
            try:
                os.makedirs(self.temp_dir, exist_ok=True)
                self._record_recovery("temp_dir_recreated", self.temp_dir)
            except OSError:
                pass
        return self.temp_dir
    
    def write_state(self, data):
        """Write state atomically with recovery."""
        self.ensure_temp_dir()
        state_file = os.path.join(self.temp_dir, "state.json")
        
        try:
            # Write to temp file first
            temp_file = f"{state_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(data, f)
            # Atomic rename
            os.rename(temp_file, state_file)
            print(f"[State] Wrote state: {data}")
        except FileNotFoundError:
            self._record_failure("temp_file_missing", state_file)
            # Recreate temp dir and retry
            self.ensure_temp_dir()
            try:
                temp_file = f"{state_file}.tmp"
                with open(temp_file, 'w') as f:
                    json.dump(data, f)
                os.rename(temp_file, state_file)
                self._record_recovery("temp_file_recreated", state_file)
            except OSError as e:
                self._record_failure("state_write_failed", str(e))
    
    def _record_failure(self, failure_type, details):
        """Record a failure for diagnostics."""
        if failure_type not in self.diagnostics["failures"]:
            self.diagnostics["failures"][failure_type] = 0
        self.diagnostics["failures"][failure_type] += 1
        print(f"  [Failure] {failure_type}: {details}")
    
    def _record_recovery(self, recovery_type, details):
        """Record a recovery for diagnostics."""
        if recovery_type not in self.diagnostics["recoveries"]:
            self.diagnostics["recoveries"][recovery_type] = 0
        self.diagnostics["recoveries"][recovery_type] += 1
        print(f"  [Recovery] {recovery_type}: {details}")
    
    def _cleanup_lock(self):
        """Cleanup lock file on exit."""
        if self.lock_file and os.path.exists(self.lock_file):
            try:
                os.remove(self.lock_file)
            except OSError:
                pass
    
    def print_diagnostics(self):
        """Print diagnostic summary."""
        print("\nEnvironmental Diagnostics:")
        print(f"  Failures: {self.diagnostics['failures']}")
        print(f"  Recoveries: {self.diagnostics['recoveries']}")


def run_automation_script():
    """Main automation script with environmental resilience."""
    diag = EnvironmentDiagnostics()
    
    print("Task 3: The Ghost in the Machine")
    print("=" * 60)
    print("Starting resilient automation script...\n")
    
    try:
        # Validate environment at startup
        diag.validate_env_vars(["DB_CONNECTION_URL", "API_KEY"])
        
        # Setup locking
        diag.setup_locking()
        
        # Setup temp directory
        diag.setup_temp_dir()
        
        # Simulate work that might encounter environmental issues
        for step in range(1, 4):
            print(f"\n[Work] Step {step}...")
            
            # Attempt to use environment variables
            try:
                db_url = diag.get_env("DB_CONNECTION_URL", "sqlite:///fallback.db")
                print(f"  Connected to: {db_url}")
            except RuntimeError as e:
                print(f"  Warning: {e}")
            
            # Write state (might fail if temp dir deleted)
            try:
                diag.write_state({
                    "step": step,
                    "timestamp": time.time(),
                    "status": "in_progress"
                })
            except Exception as e:
                print(f"  State write failed: {e}")
            
            time.sleep(0.5)
        
        print("\n[Work] Automation complete!")
        diag.print_diagnostics()
        
    except Exception as e:
        print(f"\n✗ Automation failed: {e}")
        diag.print_diagnostics()
        raise


if __name__ == "__main__":
    # Set up environment for testing
    os.environ["DB_CONNECTION_URL"] = "postgres://localhost/production"
    os.environ["API_KEY"] = "sk-test-secret-key"
    
    run_automation_script()
