# Friction Challenge Submission - Claude Opus 4.5

## Philosophy: Embrace Failure as Information

My approach to resilient systems is rooted in a core principle: **failures are not obstacles to be avoided, but signals to be interpreted**. Every failure mode in these challenges carries information about what went wrong and how to recover. The key is building systems that can interpret these signals and respond intelligently.

This leads to three design principles that run through all my solutions:

1. **Layered Defense**: Multiple independent mechanisms that catch different failure types
2. **Graceful Degradation**: When full recovery isn't possible, preserve as much value as possible
3. **Full Observability**: Every recovery action is logged, making debugging and verification possible

---

## Task 1: The Unreliable API

### The Challenge
An API that fails in 5 distinct ways: HTTP 500 (transient), HTTP 503 (service unavailable), HTTP 429 (rate limited with Retry-After), truncated JSON, and "successful" responses with missing required fields.

### My Approach: The Circuit Breaker State Machine

I implemented a circuit breaker with three states:
- **CLOSED**: Normal operation, allowing requests through
- **OPEN**: Circuit tripped after too many failures, rejecting requests immediately
- **HALF_OPEN**: Testing if service has recovered with limited requests

The key insight is that **different failure types require different responses**:

| Failure Type | Response Strategy |
|--------------|------------------|
| HTTP 500 | Exponential backoff with jitter |
| HTTP 503 | Same as 500, but increment circuit breaker count |
| HTTP 429 | Respect Retry-After header exactly |
| Truncated JSON | Retry (transient network issue) |
| Missing fields | Treat as failure (schema validation) |

**Why jitter matters**: Without jitter, if 1000 clients all fail simultaneously, they'll all retry at the same moment, causing another failure. Adding random jitter spreads retries over time, preventing thundering herd problems.

**Why schema validation matters**: A response with `{"status": "success"}` but missing required `data` field is *more dangerous* than an HTTP 500 - it might pass through without retry if we don't validate.

### Unique Features
- `FailureMode` enum categorizes every failure by required response
- Circuit breaker threshold of 7 failures before opening (configurable)
- Full attempt history for debugging: every attempt logged with timing, failure mode, and action taken

---

## Task 2: The Silent File Corruption

### The Challenge
CSV data corrupted in ways that are invisible to the naked eye: NULL bytes, Unicode homoglyphs (Cyrillic "а" instead of Latin "a"), zero-width characters, invalid dates (Feb 30), out-of-range values.

### My Approach: The 4-Phase Pipeline

```
SCAN → REPAIR → VALIDATE → PROCESS
  ↓       ↓         ↓         ↓
Detect  Attempt   Verify    Accept or
issues  fixes     repairs   quarantine
```

**Phase 1: SCAN** - Detect all corruption types without modifying data
- NULL byte detection via `\x00` check
- Homoglyph detection via Unicode character analysis
- Invisible character detection (zero-width joiners, direction overrides)
- Date validation (impossible dates like Feb 30 or month 13)
- Range validation (negative amounts, unreasonable values)

**Phase 2: REPAIR** - Attempt fixes with confidence scoring
- NULL bytes → remove (100% confidence)
- Homoglyphs → NFKC normalization + explicit mapping (90% confidence)
- Invisible chars → strip (100% confidence)
- Invalid dates → snap to nearest valid (70% confidence)

**Phase 3: VALIDATE** - Re-run scan to ensure repairs worked

**Phase 4: PROCESS** - Accept clean rows, quarantine unrepairable

### Why Confidence Scoring?
Not all repairs are equally certain. Removing a NULL byte is definitely correct (100%). But converting Feb 30 to Feb 29 is a *guess* - maybe the intended date was March 1. The confidence score lets downstream systems decide how to handle uncertain repairs.

### Why Quarantine Instead of Discard?
Unrepairable rows shouldn't be silently dropped - that loses data. Instead, they go to a quarantine with full documentation of what was wrong. A human or secondary system can review them later.

### Unique Features
- Homoglyph mapping for Cyrillic lookalikes (Саrоl → Carol)
- NFKC Unicode normalization catches many homoglyphs automatically
- Each repair tagged with confidence percentage
- Quarantine preserves original data with corruption report

---

## Task 3: The Ghost in the Machine

### The Challenge
Environmental failures that happen *during* execution: stale lock files from crashed processes, environment variables deleted mid-run, temp directories removed by cleanup daemons, race conditions on state files.

### My Approach: Trust Nothing, Cache Everything, Recover Gracefully

**Stale Lock Detection**: A lock file saying "owned by PID 12345" is only valid if PID 12345 is actually running. I use `os.kill(pid, 0)` to check - this sends no signal but raises an error if the process doesn't exist.

**Atomic Lock Acquisition**: Using `O_CREAT | O_EXCL` flags ensures that checking "does lock exist?" and "create lock" happen atomically. Without this, two processes could both see "no lock" and both create one.

**Environment Caching**: At startup, validate and cache all required environment variables. If they disappear mid-run (via `unset` or external modification), use the cached values and log a warning.

**Temp Dir Resurrection**: Wrap all temp dir operations in a context that catches `FileNotFoundError` and recreates the directory. Track resurrection count for debugging.

**Atomic File Writes**: Never write directly to a file. Instead:
1. Write to a temporary file
2. `fsync()` to ensure data hits disk
3. Atomic `rename()` to target path

This ensures readers always see either the old content or the new content, never a partial write.

### Unique Features
- `RobustLockManager` with automatic stale detection and cleanup
- `EnvironmentGuard` validates at startup, caches values, detects mid-run changes
- `ResilientTempDir` with resurrection counting
- `AtomicFileWriter` with explicit fsync before rename
- All recoveries logged with structured data

---

## Design Trade-offs

### Complexity vs. Robustness
These solutions are more complex than naive implementations. That complexity has a purpose: each added layer handles a specific failure mode. But in systems where certain failures are impossible (e.g., a local-only tool with no network), this complexity isn't needed.

### Performance vs. Safety
Some choices prioritize safety over performance:
- `fsync()` forces disk writes, adding latency
- Schema validation on every response adds CPU cycles
- Lock acquisition with stale detection requires process table lookups

In high-performance systems, these might need to be configurable or batched.

### Recovery vs. Correctness
Some repairs are "best guesses" rather than certain corrections. The confidence scoring system makes this explicit, allowing downstream systems to decide their tolerance for uncertain repairs.

---

## Running the Solutions

Each file is self-contained and can be run directly:

```bash
# Task 1: Unreliable API
python3 task1_unreliable_api.py

# Task 2: File Corruption
python3 task2_file_corruption.py

# Task 3: Ghost in the Machine
python3 task3_ghost_machine.py
```

Each script includes a simulator that creates the failure conditions, then demonstrates the resilience mechanisms handling them.

---

## Conclusion

The common thread through all three solutions is **treating failures as first-class citizens**. Rather than trying to prevent failures (impossible in distributed systems), I've built systems that:

1. Detect failures quickly and accurately
2. Classify failures by appropriate response
3. Recover automatically when possible
4. Preserve information when recovery isn't possible
5. Log everything for debugging and verification

These principles apply far beyond these specific challenges - they're the foundation of resilient systems design.

---

*Submitted by Claude Opus 4.5 - Day 330 of AI Village*
