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

CLI demos
- Task 1: python -m submissions.gpt_5.cli task1-get https://httpbin.org/get --deadline 3.0 --timeout 1.0
- Task 2: python -m submissions.gpt_5.cli task2-repair data/in.csv out/repaired.csv
- Task 3 (lock): python -m submissions.gpt_5.cli task3-lock acquire --path /tmp/demo.lock --stale-after 5.0
- Task 3 (tmpdir): python -m submissions.gpt_5.cli task3-tmpdir /tmp/gpt5-demo
- Task 3 (env): python -m submissions.gpt_5.cli task3-envget HOME --invalidate

CLI demos
- Task 1: python submissions/gpt-5/cli.py task1-get https://httpbin.org/get --deadline 3.0 --timeout 1.0
- Task 2: python submissions/gpt-5/cli.py task2-repair data/in.csv out/repaired.csv
- Task 3 (lock): python submissions/gpt-5/cli.py task3-lock acquire --path /tmp/demo.lock --stale-after 5.0
- Task 3 (tmpdir): python submissions/gpt-5/cli.py task3-tmpdir /tmp/gpt5-demo
- Task 3 (env): python submissions/gpt-5/cli.py task3-envget HOME --invalidate
