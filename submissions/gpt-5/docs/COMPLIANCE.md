Title: GPT-5 Compliance Matrix (Friction Challenge)

Scope: This maps the challenge requirements to concrete code paths, tests, and observability. All timestamps and logs are UTC; JSON key order is stable; files end with a single trailing newline.

Task 1 — Unreliable API client
- Requirement: Exponential backoff with jitter and retry budget.
  - Code: submissions/gpt-5/common/backoff.py (backoff_delays with full-jitter, cap, seeded RNG).
  - Tests: submissions/gpt-5/tests/test_backoff.py (determinism, caps, budgets).
  - Observability: structured logs via logging_util (UTC, correlation ID).
- Requirement: Circuit breaker with CLOSED/OPEN/HALF_OPEN and half-open guard.
  - Code: submissions/gpt-5/common/circuit_breaker.py
  - Tests: submissions/gpt-5/tests/test_circuit.py (state transitions, thread-safety).
- Requirement: Robust HTTP client with deadlines and schema validation.
  - Code: submissions/gpt-5/task1_client.py (deadline_s, timeout_s, retry_for_status, validate callback).
  - Notes: X-Request-ID propagated from correlation ID contextvar.

Task 2 — Data corruption and repair
- Requirement: Sanitize NULs/Unicode Cf, NFC normalize; deterministic whitespace.
  - Code: submissions/gpt-5/task2_repair.py::sanitize_text.
- Requirement: Anomaly detection and deterministic CSV repair preserving header and row order.
  - Code: submissions/gpt-5/task2_repair.py::{detect_csv_anomalies, repair_csv}.
  - Tests: submissions/gpt-5/tests/test_task2_repair.py.
- Requirement: Integrity helpers.
  - Code: submissions/gpt-5/task2_repair.py::sha256_bytes.

Task 3 — Environment hardening and ghost lock handling
- Requirement: Atomic lock acquisition; detect stale by age and dead PID; safe reclamation with provenance.
  - Code: submissions/gpt-5/task3_env.py::{acquire_lock, _pid_alive}.
  - Tests: submissions/gpt-5/tests/test_task3_env.py::test_stale_lock_reclamation.
  - Cross-process: submissions/gpt-5/tests/test_task3_cross_process.py (this PR) asserts exclusivity and dead-PID reclamation across processes.
- Requirement: Owner-verified release.
  - Code: submissions/gpt-5/task3_env.py::release_lock.
- Requirement: Env caching with explicit invalidation; tmpdir positive probe.
  - Code: submissions/gpt-5/task3_env.py::{getenv_cached, invalidate_env_cache, ensure_tmpdir}.
  - Tests: submissions/gpt-5/tests/test_task3_env.py::test_getenv_cached_and_invalidate, ::test_ensure_tmpdir_writable.

CI / Reproducibility
- Unit tests: .github/workflows/ci.yml (pinned actions) run pytest on submissions/gpt-5/tests.
- Lint + type: .github/workflows/lint-type.yml (pinned) runs ruff and mypy with fixed versions.
- Dependencies pinned in pyproject.toml; no network in tests; seeded randomness; TZ=UTC.

Auditability
- Lock reclamation writes a .stale.<epoch> note with {reclaimed_by, reclaimed_at, reason}.
- Logs: JSON lines with UTC timestamps and correlation IDs for request attempts and outcomes.
