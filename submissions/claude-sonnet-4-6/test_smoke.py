"""
Smoke tests for Friction Challenge solutions — Claude Sonnet 4.6.

These tests verify that all three task scripts run end-to-end without crashing,
and that the core workaround patterns produce observable results.

Run with: python3 test_smoke.py -v
"""

import io
import sys
import os
import time
import json
import tempfile
import unittest
import importlib.util
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr

SUBMISSION_DIR = Path(__file__).resolve().parent


def import_module(name, path):
    """Dynamically import a module from path without executing __main__ block."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestTask1CircuitBreaker(unittest.TestCase):
    """Unit tests for Task 1: CircuitBreaker class."""

    def setUp(self):
        self.mod = import_module("task1", SUBMISSION_DIR / "task1_unreliable_api.py")

    def test_starts_closed(self):
        """Circuit breaker should start in CLOSED state."""
        cb = self.mod.CircuitBreaker(failure_threshold=3)
        self.assertTrue(cb.can_attempt(), "Brand new circuit breaker should allow attempts")

    def test_opens_at_threshold(self):
        """Circuit breaker should open after hitting failure threshold."""
        cb = self.mod.CircuitBreaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        self.assertTrue(cb.can_attempt(), "Should still be closed at 2 failures")
        buf = io.StringIO()
        with redirect_stdout(buf):
            cb.record_failure()  # 3rd failure = trips
        self.assertFalse(cb.can_attempt(), "Should be OPEN after 3 failures")

    def test_success_resets_counter(self):
        """A success should reset the consecutive failure counter."""
        cb = self.mod.CircuitBreaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        self.assertEqual(cb.failure_count, 0)
        self.assertTrue(cb.can_attempt(), "Should be CLOSED after success reset")

    def test_half_open_after_recovery_timeout(self):
        """After recovery_timeout elapses, OPEN breaker should allow a probe attempt."""
        cb = self.mod.CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        buf = io.StringIO()
        with redirect_stdout(buf):
            cb.record_failure()
            cb.record_failure()
        # Should be OPEN now — no attempts allowed immediately
        self.assertFalse(cb.can_attempt(), "Should be OPEN and block attempts right away")
        # Manipulate last_failure_time to simulate recovery_timeout elapsed
        import time
        cb.last_failure_time = time.time() - 2  # 2s ago > 1s recovery_timeout
        buf2 = io.StringIO()
        with redirect_stdout(buf2):
            result = cb.can_attempt()
        self.assertTrue(result, "Should go HALF_OPEN and allow probe after recovery_timeout")


class TestTask1FullDemo(unittest.TestCase):
    """Integration test for Task 1 full demo run."""

    def test_demo_succeeds_and_retrieves_value(self):
        """The full mock-server demo should succeed and retrieve valid data."""
        mod = import_module("task1", SUBMISSION_DIR / "task1_unreliable_api.py")
        buf = io.StringIO()
        try:
            with redirect_stdout(buf):
                mod.main()
        except SystemExit:
            pass
        except Exception as e:
            self.fail(f"task1 main() raised {type(e).__name__}: {e}")
        output = buf.getvalue()
        self.assertIn("✅", output, "Demo should report a successful retrieval")
        self.assertIn("value", output, "Result should contain 'value' field")


class TestTask2CorruptionDetection(unittest.TestCase):
    """Unit tests for Task 2: corruption scanning functions."""

    def setUp(self):
        self.mod = import_module("task2", SUBMISSION_DIR / "task2_file_corruption.py")

    def test_detects_null_byte_in_line(self):
        """scan_for_corruption should catch NULL bytes embedded in CSV lines."""
        # Craft a minimal CSV with a NULL byte in a value field
        test_csv = "id,date,sensor,value,unit\n1,2026-02-15,temp_A,21\x00.8,C\n"
        findings = self.mod.scan_for_corruption(test_csv)
        self.assertTrue(len(findings) > 0, "Should detect NULL byte corruption")
        types = [f[2] for f in findings]
        self.assertTrue(any("NULL" in t for t in types), f"Should label it as NULL byte: {types}")

    def test_detects_zero_width_space(self):
        """detect_invisible_unicode should catch zero-width space characters."""
        result = self.mod.detect_invisible_unicode("\u200b22.3")
        self.assertTrue(len(result) > 0, "Zero-width space (U+200B) should be detected")

    def test_detects_fullwidth_homoglyph(self):
        """detect_invisible_unicode should catch fullwidth Unicode digits (e.g., '２' for '2')."""
        # FULLWIDTH DIGIT TWO (U+FF12) looks like ASCII '2' but is a different codepoint
        result = self.mod.detect_invisible_unicode("2\uff122.1")
        self.assertTrue(len(result) > 0, "Fullwidth digit '２' should be detected as a homoglyph")

    def test_detects_impossible_date(self):
        """scan_for_corruption should catch calendar dates that don't exist."""
        test_csv = "id,date,sensor,value,unit\n1,2026-02-30,temp_A,22.3,C\n"
        findings = self.mod.scan_for_corruption(test_csv)
        date_issues = [f for f in findings if f[1] == 'date']
        self.assertTrue(len(date_issues) > 0, "Feb 30 should be flagged as invalid date")

    def test_detects_out_of_range_value(self):
        """scan_for_corruption should flag values outside physical bounds."""
        test_csv = "id,date,sensor,value,unit\n1,2026-02-15,temp_A,9999.9,C\n"
        findings = self.mod.scan_for_corruption(test_csv)
        range_issues = [f for f in findings if 'range' in f[2].lower()]
        self.assertTrue(len(range_issues) > 0, "9999.9°C should be out of physical range")

    def test_clean_data_passes_scan(self):
        """Valid CSV with no corruption should produce zero findings."""
        test_csv = "id,date,sensor,value,unit\n1,2026-02-15,temp_A,22.3,C\n"
        findings = self.mod.scan_for_corruption(test_csv)
        self.assertEqual(len(findings), 0, f"Clean data should have no findings, got: {findings}")

    def test_repair_removes_null_bytes(self):
        """repair_field should strip NULL bytes from field values."""
        result = self.mod.repair_field("21\x00.8")
        self.assertEqual(result, "21.8", "NULL bytes should be stripped")

    def test_repair_removes_zero_width_space(self):
        """repair_field should remove invisible Unicode control chars."""
        result = self.mod.repair_field("\u200b22.3")
        self.assertEqual(result, "22.3", "Zero-width space should be removed")

    def test_full_pipeline_runs(self):
        """Full demo should run and produce expected summary output."""
        mod = import_module("task2", SUBMISSION_DIR / "task2_file_corruption.py")
        buf = io.StringIO()
        try:
            with redirect_stdout(buf):
                mod.main()
        except SystemExit:
            pass
        except Exception as e:
            self.fail(f"task2 main() raised {type(e).__name__}: {e}")
        output = buf.getvalue()
        self.assertIn("Processing complete", output)
        self.assertIn("PHASE", output, "Should show multi-phase pipeline")


class TestTask3LockMechanics(unittest.TestCase):
    """Tests for Task 3: stale lock detection and atomic lock creation."""

    def setUp(self):
        self.mod = import_module("task3", SUBMISSION_DIR / "task3_ghost_machine.py")

    def test_atomic_lock_creation_exclusive(self):
        """Two threads racing to create the same lock — only one should succeed."""
        import threading
        results = []
        lock_path = tempfile.mktemp(suffix=".lock")

        def try_acquire():
            try:
                fd = self.mod.atomic_create_lock(lock_path, "placeholder")
                results.append(("success", fd))
                # Don't close the fd here - let cleanup handle it
            except FileExistsError:
                results.append(("failed",))

        try:
            t1 = threading.Thread(target=try_acquire)
            t2 = threading.Thread(target=try_acquire)
            t1.start(); t2.start()
            t1.join(); t2.join()

            successes = [r for r in results if r[0] == "success"]
            failures = [r for r in results if r[0] == "failed"]
            self.assertEqual(len(successes), 1, "Exactly one thread should win the lock race")
            self.assertEqual(len(failures), 1, "Exactly one thread should fail (FileExistsError)")
        finally:
            # Cleanup
            for r in results:
                if r[0] == "success":
                    try:
                        os.close(r[1])
                    except Exception:
                        pass
            try:
                os.unlink(lock_path)
            except FileNotFoundError:
                pass

    def test_pid_exists_for_current_process(self):
        """pid_exists() should return True for the current process."""
        self.assertTrue(self.mod.pid_exists(os.getpid()),
                        "Current process PID should definitely exist")

    def test_pid_exists_returns_false_for_bogus_pid(self):
        """pid_exists() should return False for a PID that doesn't exist."""
        bogus_pid = 999999999  # Astronomically unlikely to be real
        self.assertFalse(self.mod.pid_exists(bogus_pid),
                         "Non-existent PID 999999999 should return False")

    def test_env_caching_in_robust_runner(self):
        """RobustAutomationRunner should cache env vars and survive deletion."""
        failures = []
        recoveries = []
        runner = self.mod.RobustAutomationRunner("test", failures, recoveries)

        # Set up an env var and cache it
        os.environ["DB_CONNECTION_URL"] = "postgres://test@localhost/db"
        runner.validate_env()

        # Delete it from environment
        del os.environ["DB_CONNECTION_URL"]

        # Runner should use cached value
        buf = io.StringIO()
        with redirect_stdout(buf):
            val = runner.get_env("DB_CONNECTION_URL")
        self.assertEqual(val, "postgres://test@localhost/db",
                         "Should return cached value after env var deletion")
        self.assertIn("env_var_missing", failures)
        self.assertIn("env_var_cached", recoveries)

    def test_full_demo_both_runners_complete(self):
        """Both parallel robust runners should complete successfully."""
        mod = import_module("task3", SUBMISSION_DIR / "task3_ghost_machine.py")
        buf = io.StringIO()
        try:
            with redirect_stdout(buf):
                mod.main()
        except SystemExit:
            pass
        except Exception as e:
            self.fail(f"task3 main() raised {type(e).__name__}: {e}")
        output = buf.getvalue()
        self.assertIn("robust-A: completed", output, "Instance A should complete")
        self.assertIn("robust-B: completed", output, "Instance B should complete")
        self.assertIn("stale_lock_removed", output, "Should report stale lock removal")
        self.assertIn("env_var_cached", output, "Should report env var cache recovery")


if __name__ == "__main__":
    unittest.main(verbosity=2)
