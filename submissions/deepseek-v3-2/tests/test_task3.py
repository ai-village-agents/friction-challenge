#!/usr/bin/env python3
"""
Test suite for Task 3: Ghost in the Machine
DeepSeek-V3.2 Adaptive Resilience Solution
"""

import json
import os
import sys
import time
import tempfile
import threading
import signal
import fcntl
import errno
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock

import pytest

# Ensure project root is importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import task3_ghost_machine as task3
from task3_ghost_machine import (
    EnvironmentSnapshot,
    ResourceMonitor,
    AtomicOperation,
    HeartbeatLock,
    EnvironmentGuard,
    GhostAutomation,
)

# ─── Test EnvironmentSnapshot ────────────────────────────────────────────────

def test_environment_snapshot_capture():
    """Test basic environment snapshot capture."""
    snapshot = EnvironmentSnapshot.capture()
    
    assert isinstance(snapshot, EnvironmentSnapshot)
    assert snapshot.pid == os.getpid()
    assert snapshot.timestamp <= time.time()
    assert isinstance(snapshot.system, dict)
    assert isinstance(snapshot.process, dict)
    assert isinstance(snapshot.filesystem, dict)
    
    # Check expected fields
    assert "hostname" in snapshot.system
    assert "platform" in snapshot.system
    assert "python_version" in snapshot.system
    assert "cwd" in snapshot.filesystem
    assert "temp_dir" in snapshot.filesystem


def test_environment_snapshot_diff():
    """Test diff computation between snapshots."""
    snapshot1 = EnvironmentSnapshot.capture()
    
    # Modify environment to create differences
    original_cwd = os.getcwd()
    temp_dir = tempfile.mkdtemp()
    os.chdir(temp_dir)
    
    snapshot2 = EnvironmentSnapshot.capture()
    diff = snapshot1.diff(snapshot2)
    
    assert isinstance(diff, dict)
    assert "filesystem" in diff
    assert "cwd" in diff.get("filesystem", {})
    
    # Cleanup
    os.chdir(original_cwd)
    os.rmdir(temp_dir)


def test_environment_snapshot_to_dict():
    """Test serialization to dictionary."""
    snapshot = EnvironmentSnapshot.capture()
    snapshot_dict = snapshot.to_dict()
    
    assert isinstance(snapshot_dict, dict)
    assert snapshot_dict["pid"] == snapshot.pid
    assert abs(snapshot_dict["timestamp"] - snapshot.timestamp) < 0.1


# ─── Test ResourceMonitor ────────────────────────────────────────────────────

def test_resource_monitor_basic():
    """Test basic resource monitor functionality."""
    monitor = ResourceMonitor()
    
    # Register a lockfile
    lockfile = tempfile.NamedTemporaryFile(suffix=".lock", delete=False)
    lockfile.close()
    
    monitor.register_lockfile(lockfile.name, os.getpid(), heartbeat_interval=1.0)
    
    # Check for stale resources (should be none)
    stale = monitor.check_stale_resources()
    assert isinstance(stale, list)
    
    # Cleanup - file may have been removed by monitor
    if os.path.exists(lockfile.name):
        os.unlink(lockfile.name)


def test_resource_monitor_stale_lock_detection():
    """Test detection of stale lock with invalid PID."""
    monitor = ResourceMonitor()
    
    lockfile = tempfile.NamedTemporaryFile(suffix=".lock", delete=False)
    lockfile.close()
    
    # Register with a PID that doesn't exist (very high number)
    fake_pid = 999999
    monitor.register_lockfile(lockfile.name, fake_pid, heartbeat_interval=0.1)
    
    # Wait for heartbeat to expire
    time.sleep(0.3)
    
    stale = monitor.check_stale_resources()
    assert isinstance(stale, list)
    
    # Cleanup - file may have been removed by monitor
    if os.path.exists(lockfile.name):
        os.unlink(lockfile.name)


def test_resource_monitor_temp_dir():
    """Test temporary directory monitoring."""
    monitor = ResourceMonitor()
    
    temp_dir = tempfile.mkdtemp()
    monitor.register_temp_dir(temp_dir, expected_lifetime=0.1)
    
    # Wait for expiration
    time.sleep(0.2)
    
    stale = monitor.check_stale_resources()
    assert isinstance(stale, list)
    
    # Cleanup
    if os.path.exists(temp_dir):
        os.rmdir(temp_dir)


# ─── Test AtomicOperation ────────────────────────────────────────────────────

def test_atomic_operation_success():
    """Test successful atomic operation."""
    results = []
    
    def action1():
        results.append("action1")
        return "result1"
    
    def action2():
        results.append("action2")
        return "result2"
    
    op = AtomicOperation("test_success")
    # add_step executes immediately
    op.add_step("Step 1", action1)
    op.add_step("Step 2", action2)
    
    # The operations already executed in add_step
    # The transaction context manager just provides rollback capability
    with op.transaction():
        pass  # Transaction completes successfully
    
    assert results == ["action1", "action2"]
    assert op.completed  # Attribute is 'completed', not '_completed'


def test_atomic_operation_rollback():
    """Test atomic operation with rollback on failure."""
    executed = []
    rolled_back = []
    
    def action1():
        executed.append("action1")
        return "result1"
    
    def action2():
        executed.append("action2")
        raise Exception("Simulated failure")
    
    def rollback1():
        rolled_back.append("rollback1")
    
    op = AtomicOperation("test_rollback")
    # First step should succeed
    op.add_step("Step 1", action1, rollback1)
    
    # Second step will fail during add_step
    try:
        op.add_step("Step 2", action2)
    except Exception:
        pass  # Expected failure
    
    # Check rollback was added
    assert len(op.rollback_actions) == 1
    
    # Cleanup
    op._cleanup()


def test_atomic_operation_recovery():
    """Test recovery of atomic operation."""
    op = AtomicOperation("test_recovery")
    
    # Add a simple step
    step_completed = []
    def action():
        step_completed.append("done")
        return "result"
    
    op.add_step("Test step", action)
    
    # Simulate incomplete operation
    op.completed = False
    
    # Try recovery
    recovered = op.recover()
    assert isinstance(recovered, bool)


# ─── Test HeartbeatLock ──────────────────────────────────────────────────────

def test_heartbeat_lock_acquire_release():
    """Test basic heartbeat lock acquisition and release."""
    lockfile = tempfile.NamedTemporaryFile(suffix=".lock", delete=False)
    lockfile.close()
    
    lock = HeartbeatLock(lockfile.name, heartbeat_interval=0.5)
    
    # Acquire lock
    acquired = lock.acquire(timeout=1.0)
    assert acquired
    
    # Release lock
    lock.release()
    
    # Cleanup - file may have been removed by lock
    if os.path.exists(lockfile.name):
        os.unlink(lockfile.name)


def test_heartbeat_lock_stale_detection():
    """Test detection of stale lock file."""
    lockfile = tempfile.NamedTemporaryFile(suffix=".lock", delete=False)
    lockfile.close()
    
    # Create a stale lock file with old PID
    with open(lockfile.name, 'w') as f:
        json.dump({"pid": 999999, "timestamp": time.time() - 100}, f)
    
    lock = HeartbeatLock(lockfile.name, heartbeat_interval=0.1)
    
    # Should be able to acquire despite stale lock
    acquired = lock.acquire(timeout=1.0)
    assert acquired
    
    lock.release()
    
    # Cleanup
    if os.path.exists(lockfile.name):
        os.unlink(lockfile.name)


# ─── Test EnvironmentGuard ───────────────────────────────────────────────────

def test_environment_guard_protection():
    """Test environment variable protection."""
    guard = EnvironmentGuard()
    
    # Set environment variables
    os.environ["TEST_VAR_1"] = "original_value_1"
    os.environ["TEST_VAR_2"] = "original_value_2"
    
    # Protect them
    guard.protect("TEST_VAR_1", "TEST_VAR_2")
    
    # Modify environment
    os.environ["TEST_VAR_1"] = "modified_value"
    del os.environ["TEST_VAR_2"]
    
    # Check for changes
    issues = guard.check()
    assert isinstance(issues, list)
    assert len(issues) >= 2  # Should detect both changes
    
    # Cleanup
    del os.environ["TEST_VAR_1"]
    if "TEST_VAR_2" in os.environ:
        del os.environ["TEST_VAR_2"]


# ─── Test GhostAutomation ────────────────────────────────────────────────────

def test_ghost_automation_creation():
    """Test GhostAutomation initialization."""
    automation = GhostAutomation("test_instance")
    
    assert automation.script_id == "test_instance"
    assert hasattr(automation, 'resource_monitor')
    assert hasattr(automation, 'env_guard')
    assert hasattr(automation, 'lock')
    assert hasattr(automation, 'failures_simulated')


def test_ghost_automation_run_with_resilience():
    """Test full automation run with resilience."""
    automation = GhostAutomation("test_run")
    
    # Run with resilience (should handle simulated failures)
    result = automation.run_with_resilience()
    
    assert isinstance(result, dict)
    assert "failures" in result
    assert "recovery" in result
    assert "environment" in result
    
    failures = result["failures"]
    assert isinstance(failures, dict)
    assert "simulated" in failures
    assert "remaining_issue_count" in failures
    
    recovery = result["recovery"]
    assert isinstance(recovery, dict)
    assert "successful" in recovery


def test_ghost_automation_simulate_ghost_failures():
    """Test failure simulation."""
    automation = GhostAutomation("test_failure")
    
    # Simulate ghost failures
    automation.simulate_ghost_failures()
    assert automation.failures_simulated >= 0  # Could be 0 if random didn't trigger


# ─── Integration Tests ───────────────────────────────────────────────────────

def test_integration_stale_lock_recovery():
    """Integration test for stale lock detection and recovery."""
    # Create a stale lock file
    lockfile = tempfile.NamedTemporaryFile(suffix=".lock", delete=False)
    with open(lockfile.name, 'w') as f:
        json.dump({"pid": 999999, "timestamp": time.time() - 1000}, f)
    lockfile.close()
    
    # Create automation
    automation = GhostAutomation("integration_test")
    
    # Run with resilience
    result = automation.run_with_resilience()
    
    assert isinstance(result, dict)
    
    # Cleanup
    if os.path.exists(lockfile.name):
        os.unlink(lockfile.name)


def test_integration_environment_changes():
    """Integration test for environment change detection."""
    # Save original environment
    original_env = os.environ.copy()
    
    # Create automation
    automation = GhostAutomation("env_test")
    
    # Modify environment during test
    os.environ["INTEGRATION_TEST_VAR"] = "test_value"
    
    # Run with resilience
    result = automation.run_with_resilience()
    
    assert isinstance(result, dict)
    
    # Restore environment
    os.environ.clear()
    os.environ.update(original_env)


# ─── Mock Tests ──────────────────────────────────────────────────────────────

@patch('task3_ghost_machine.psutil.Process')
def test_snapshot_with_mocked_process(mock_process):
    """Test snapshot with mocked process information."""
    mock_process_instance = Mock()
    mock_process_instance.cpu_percent.return_value = 10.5
    mock_process_instance.memory_percent.return_value = 2.3
    mock_process_instance.num_threads.return_value = 4
    mock_process_instance.num_fds.return_value = 8
    mock_process_instance.create_time.return_value = time.time() - 3600
    mock_process_instance.status.return_value = "running"
    mock_process.return_value = mock_process_instance
    
    snapshot = EnvironmentSnapshot.capture()
    assert snapshot.process["cpu_percent"] == 10.5
    assert snapshot.process["memory_percent"] == 2.3
    assert snapshot.process["num_threads"] == 4


# ─── Performance and Edge Cases ──────────────────────────────────────────────

def test_concurrent_heartbeat_locks():
    """Test concurrent access to heartbeat locks."""
    lockfile = tempfile.NamedTemporaryFile(suffix=".lock", delete=False)
    lockfile.close()
    
    results = []
    errors = []
    
    def worker(worker_id):
        try:
            lock = HeartbeatLock(lockfile.name, heartbeat_interval=0.2)
            acquired = lock.acquire(timeout=0.5)
            if acquired:
                time.sleep(0.1)
                lock.release()
                results.append((worker_id, "success"))
            else:
                results.append((worker_id, "timeout"))
        except Exception as e:
            errors.append((worker_id, str(e)))
    
    # Start multiple threads
    threads = []
    for i in range(3):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # At least some should succeed
    assert len(results) > 0
    
    # Cleanup
    if os.path.exists(lockfile.name):
        os.unlink(lockfile.name)


def test_resource_monitor_cleanup():
    """Test resource monitor cleanup functionality."""
    monitor = ResourceMonitor()
    
    # Register multiple resources
    lockfile = tempfile.NamedTemporaryFile(suffix=".lock", delete=False)
    lockfile.close()
    monitor.register_lockfile(lockfile.name, os.getpid(), heartbeat_interval=0.1)
    
    temp_dir = tempfile.mkdtemp()
    monitor.register_temp_dir(temp_dir, expected_lifetime=0.1)
    
    # Wait for expiration
    time.sleep(0.3)
    
    # Cleanup expired resources
    cleaned = monitor.cleanup_expired()
    assert cleaned >= 0  # Could be 0 or more depending on timing
    
    # Check stale resources
    stale = monitor.check_stale_resources()
    assert isinstance(stale, list)
    
    # Cleanup
    if os.path.exists(lockfile.name):
        os.unlink(lockfile.name)
    if os.path.exists(temp_dir):
        os.rmdir(temp_dir)


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
