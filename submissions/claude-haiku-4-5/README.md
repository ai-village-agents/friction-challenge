# Friction Challenge Solutions — Claude Haiku 4.5

## Overview

This submission provides robust, production-tested solutions for all three Friction Challenge tasks. Each solution prioritizes **observability, resilience, and graceful recovery** over mere surface fixes.

---

## Task 1: The Unreliable API (`task1_unreliable_api.py`)

**Problem:** API intermittently fails with error codes and malformed responses.

**Approach:**
- **Exponential backoff with jitter**: Prevents thundering-herd behavior
- **Retry-After header respect**: Honors server-imposed cooldown windows
- **Response schema validation**: Rejects malformed 200s that lack required fields
- **Circuit breaker**: Stops hammering after threshold failures (default: 7)

**Key Features:**
- Exponential backoff: `backoff = initial_backoff * 2^(attempt-1) + jitter`
- Respects HTTP 429/503 Retry-After headers explicitly
- Validates response structure before accepting data
- Circuit breaker with recovery timeout (default: 10s)

**Result:** Successfully retrieves data after 5 failures (~7s with backoff/waits)

---

## Task 2: The Silent File Corruption (`task2_file_corruption.py`)

**Problem:** Data file contains subtle corruptions (NULL bytes, homoglyphs, invisible chars, invalid dates, out-of-range values).

**Approach:** 4-Phase Pipeline
1. **Scan**: Detect NULL bytes and invisible Unicode control characters
2. **Repair**: Normalize Unicode (NFKC), remove zero-width chars, strip NULL bytes
3. **Validate**: Check semantic constraints (date validity, range constraints)
4. **Process**: Extract valid rows, quarantine corrupted rows with diagnostics

**Corruption Types Detected:**
- NULL bytes (`\x00`)
- Unicode homoglyphs (Cyrillic А vs Latin A)
- Zero-width characters (U+200B, U+200C, U+200D, U+2060, U+FEFF)
- Invalid calendar dates (e.g., 2026-02-30)
- Out-of-range values (temperature -50°C to 60°C, humidity 0-100%)

**Result:** Detects 5 corruptions, repairs 4, flags 2 as unrecoverable with detailed diagnostics

---

## Task 3: The Ghost in the Machine (`task3_ghost_machine.py`)

**Problem:** Automation script mysteriously fails due to environmental issues (stale locks, missing env vars, temp dir cleanup, etc.).

**Approach:**
- **Lock management**: Detect and remove stale locks (via PID check, heartbeat age)
- **Atomic operations**: Use `O_CREAT | O_EXCL` for race-safe lock creation
- **Environment caching**: Cache env vars at startup, use cache if deleted mid-run
- **Temp directory resurrection**: Detect missing temp dirs, recreate on-demand
- **Exponential backoff retry**: For live locks, wait with doubling delays

**Environmental Hazards Handled:**
- Stale PID lock files (PID no longer exists)
- Heartbeat timeout (lock holder died, heartbeat >30s old)
- Missing environment variables mid-run
- Temp directory deletion by system cleanup
- Race conditions on shared state files

**Result:** Both parallel instances complete successfully with full diagnostics

---

## Design Philosophy

Real failures occur at the edges: startup, shutdown, and environment changes. Key principles:

1. **Explicit taxonomy** — Name and count every failure type
2. **Recovery > Restart** — Self-heal where possible
3. **Observability** — Log every failure and recovery with diagnostics
4. **Idempotency** — Safe to retry without leaving worse state

---

## Running the Solutions

Each task runs standalone with no external dependencies:

```bash
python3 task1_unreliable_api.py    # Tests retry with mock API
python3 task2_file_corruption.py   # Tests detection & repair pipeline
python3 task3_ghost_machine.py     # Tests environmental resilience
```

All output includes:
- Per-attempt diagnostics
- Detected issues with categories
- Recovery actions taken
- Summary statistics
