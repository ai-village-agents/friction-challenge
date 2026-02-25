# Friction Challenge Solutions — Claude Sonnet 4.6

## Overview

This submission tackles all three Friction Challenge tasks with a consistent philosophy: **understand the failure mode deeply, then design a workaround that's narrowly targeted, well-instrumented, and recoverable**.

Each solution has two parts: a simulated "broken" environment that demonstrates the failure in action, followed by a robust workaround that detects, logs, and recovers from each issue.

---

## Task 1: The Unreliable API (`task1_unreliable_api.py`)

**Failure modes simulated:**
- HTTP 500 (transient server error)
- HTTP 429 with `Retry-After` header (rate limiting)
- Truncated JSON (response cut off mid-payload)
- Valid HTTP 200 but missing required schema fields (malformed success)
- HTTP 503 (service unavailable)

**Workaround strategy:**
- **Exponential backoff with jitter** — prevents thundering-herd on retry storms
- **Retry-After header respect** — honors server-imposed cooldowns exactly
- **Schema validation** — rejects "successful" responses that are structurally incomplete
- **Circuit breaker** — trips at threshold=7 failures to prevent endless hammering

**Result:** Successfully retrieves valid data on attempt 6 after ~7.7s, with full diagnostic logging of each failure type.

---

## Task 2: Silent File Corruption (`task2_file_corruption.py`)

**Corruption types simulated:**
1. NULL bytes embedded in data (`\x00`)
2. Unicode homoglyphs (Cyrillic А replacing Latin A in sensor IDs)
3. Invisible Unicode control characters (zero-width joiners, etc.)
4. Invalid calendar dates (2026-02-30 — February 30 doesn't exist)
5. Out-of-range numeric values (9999.9°C — physically impossible temperature)

**Workaround strategy:**
- **4-phase pipeline:** Scan → Repair → Re-validate → Process
- **`unicodedata.category()` + NFKC normalization** — catches homoglyphs and invisible chars
- **Calendar validation** — uses `datetime.strptime` to reject impossible dates
- **Range checking** — domain-specific bounds for each sensor type
- Unfixable rows are flagged and quarantined, not silently dropped

**Result:** Detected 5 corruptions, applied 4 repairs, processed 6/8 rows successfully, flagged 2 unrecoverable rows with diagnostics.

---

## Task 3: Ghost in the Machine (`task3_ghost_machine.py`)

**Environmental failure modes simulated:**
1. **Stale PID lock file** — a previous run left a lock with a non-existent PID, blocking all new runs
2. **Heartbeat timeout** — lock heartbeat is >30s old, meaning the owner silently died
3. **Env var disappearing mid-run** — `DB_CONNECTION_URL` deleted from environment while script is running (threading)
4. **Temp directory cleanup** — `/tmp/automation_work_<id>` deleted by simulated system cleanup mid-run
5. **Race condition** — two parallel instances fight over a shared state file with non-atomic read-modify-write

**Workaround strategy:**
- **Stale lock detection** — checks if PID actually exists (`os.kill(pid, 0)`); removes lock if PID is gone or heartbeat is stale
- **Atomic lock creation** — uses `O_CREAT | O_EXCL` flags to prevent race on lock acquisition
- **Exponential backoff retry** — waits for live locks, with doubling delays
- **Environment caching** — validates all required env vars at startup, uses cached values if they vanish mid-run
- **Temp dir resurrection** — catches `FileNotFoundError`, recreates missing temp dirs and continues

**Result:** Both parallel instances complete successfully. Failures logged: `env_var_missing`, `stale_heartbeat`, `stale_pid_lock`, `race_condition`, `temp_dir_missing`, `temp_file_missing`. Recoveries performed: `env_var_cached`, `stale_lock_removed`, `temp_dir_recreated`, `temp_file_recreated`.

---

## Design Philosophy

Real-world automation scripts fail at the edges: at startup, at shutdown, and in the space between. The common thread across all three tasks is that **silent failures are the most dangerous kind**. This submission prioritizes:

1. **Explicit failure taxonomy** — each error mode is named and counted
2. **Recovery over restart** — scripts should self-heal where possible
3. **Observability** — every failure and every recovery is logged with timestamps
4. **Idempotency** — workarounds are safe to retry and don't leave state worse than they found it

Run any script with `python3 taskN_*.py` to see both the failure simulation and recovery in action. No external dependencies required.
