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

## Compliance Matrix

| Task | Failure Mode | Root Cause | Detection Method | Recovery Strategy | Verified |
|------|-------------|------------|-----------------|-------------------|----------|
| **Task 1** | HTTP 500 (server error) | Transient server instability | HTTP status code check | Exponential backoff + jitter retry | ✅ |
| **Task 1** | HTTP 429 (rate limit) | Request throttling | HTTP status code + `Retry-After` header | Wait exactly `Retry-After` seconds | ✅ |
| **Task 1** | HTTP 503 (unavailable) | Service overload | HTTP status code + `Retry-After` header | Wait per Retry-After header | ✅ |
| **Task 1** | Truncated JSON | Response cut off mid-stream | `json.JSONDecodeError` on parse | Treat as transient error, retry | ✅ |
| **Task 1** | Schema-invalid 200 OK | API bug returns partial payload | Key presence validation after parse | Log missing fields, retry | ✅ |
| **Task 1** | Circuit breaker trip | Accumulated 7+ consecutive failures | Failure counter vs. threshold | Raise `CircuitBreakerOpen`, halt | ✅ |
| **Task 2** | NULL bytes (`\x00`) | Binary corruption in text field | Char-by-char `ord() > 127` or `== 0` | Strip NULL bytes from field value | ✅ |
| **Task 2** | Unicode homoglyphs | Cyrillic chars visually identical to Latin | `unicodedata.name()` + NFKC normalization | Normalize to canonical ASCII form | ✅ |
| **Task 2** | Invisible control chars | Zero-width joiners/spaces injected | `unicodedata.category()` == 'Cf' check | Remove all format/control characters | ✅ |
| **Task 2** | Invalid calendar date | 2026-02-30 (Feb 30 doesn't exist) | `datetime.strptime` raises `ValueError` | Flag as `INVALID_DATE`, quarantine row | ✅ |
| **Task 2** | Out-of-range numeric value | 9999.9°C (physically impossible) | Domain-specific bounds: [-50, 60]°C | Flag as `INVALID_VALUE`, quarantine row | ✅ |
| **Task 2** | Silent drop of bad rows | Traditional pattern hides data loss | Explicit quarantine counter + log | Quarantine list returned with diagnostics | ✅ |
| **Task 3** | Stale PID lock (dead process) | Previous run crashed without cleanup | `os.kill(pid, 0)` — `ProcessLookupError` if dead | Remove stale lock, proceed | ✅ |
| **Task 3** | Stale heartbeat (>30s) | Process frozen / zombie / disk stall | Compare heartbeat timestamp to `time.time()` | Treat as stale, remove lock | ✅ |
| **Task 3** | Race on lock creation | Two processes both detect no lock | `O_CREAT \| O_EXCL` atomic open — only one wins | Losing process retries with backoff | ✅ |
| **Task 3** | Env var vanishes mid-run | External process unsets variable | `os.environ.get()` returns `None` | Use startup-cached value, log warning | ✅ |
| **Task 3** | Temp dir deleted mid-run | OS cleanup / cron job removes `/tmp/work` | `FileNotFoundError` on write/read | Recreate dir and temp file, resume | ✅ |
| **Task 3** | Zombie lock (heartbeat stopped) | Owner thread suspended/stuck | Heartbeat age > `HEARTBEAT_TIMEOUT=30s` | Break lock after timeout, log incident | ✅ |

---

## Design Philosophy

Real-world automation scripts fail at the edges: at startup, at shutdown, and in the space between. The common thread across all three tasks is that **silent failures are the most dangerous kind**. This submission prioritizes:

1. **Explicit failure taxonomy** — each error mode is named and counted
2. **Recovery over restart** — scripts should self-heal where possible
3. **Observability** — every failure and every recovery is logged with timestamps
4. **Idempotency** — workarounds are safe to retry and don't leave state worse than they found it

---

## Running the Solutions

```bash
# Task 1: Demonstrates API retry with backoff + circuit breaker
python3 task1_unreliable_api.py

# Task 2: Demonstrates CSV corruption pipeline (scan→repair→validate→process)
python3 task2_file_corruption.py

# Task 3: Demonstrates ghost env + stale locks + concurrent instances
python3 task3_ghost_machine.py

# Smoke tests (runs all three demos)
python3 test_smoke.py
```

No external dependencies required — all three solutions use Python 3 standard library only.
