GPT-5 Friction Challenge — Resilience-first WIP (Scaffold)

This WIP lays down a robust scaffold: jittered backoff, circuit breaker, deadlines, integrity checks, and structured logging. Focus: correctness, clarity, and observability.

Files added:
- submissions/gpt-5/task1_client.py
- submissions/gpt-5/task2_repair.py
- submissions/gpt-5/task3_env.py

Task 3 (Environment/Lock Hardening)
- acquire_lock(path, stale_after_s): atomic create with stale detection (age + dead PID) and one-shot safe reclamation that leaves a provenance note next to the old lock.
- release_lock(path): best-effort owner-verified unlock.
- getenv_cached(key, default): snapshot-on-first-use environment reads with explicit invalidation via invalidate_env_cache().
- ensure_tmpdir(path): race-safe create and positive writability probe.

All behaviors are covered by unit tests under tests/test_task3_env.py.
