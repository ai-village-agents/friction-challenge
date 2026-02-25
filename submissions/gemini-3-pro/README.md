# Friction Challenge Solutions - Gemini 3 Pro

## Task 1: The Unreliable API
**File:** `task1_unreliable_api.py`
**Strategy:**
- **Circuit Breaker:** Prevents cascading failures by stopping requests when the failure threshold is reached.
- **Exponential Backoff:** Progressively increases wait times between retries to reduce server load.
- **Jitter:** Adds randomness to backoff intervals to prevent thundering herd problems.
- **Schema Validation:** Strictly validates JSON structure and content to catch malformed 200 OK responses.
- **Retry-After Compliance:** Respects server-sent `Retry-After` headers for 429/503 responses.

## Task 2: Silent File Corruption
**File:** `task2_file_corruption.py`
**Strategy:**
- **Binary Inspection:** Reads file in binary mode to detect invisible corruption like NULL bytes (`\x00`).
- **Unicode Sanitization:** Scans for and removes zero-width characters (e.g., `\u200b`) that break string processing.
- **Structure Recovery:** Parses the cleaned data to reconstruct the intended structured format.

## Task 3: Ghost in the Machine
**File:** `task3_ghost_machine.py`
**Strategy:**
- **Stale Lock Detection:** Checks if the PID in the lock file actually exists using `os.kill(pid, 0)`. If not, it safely removes the stale lock.
- **Environment Verification:** robustly checks for required environment variables before *and* during execution.
- **Atomic Operations:** Uses retry loops to handle race conditions where the environment might change mid-process.
