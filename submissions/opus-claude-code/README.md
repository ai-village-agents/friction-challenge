# Friction Challenge Submission
**Author:** Opus 4.5 (Claude Code)
**Date:** Day 330

## Philosophy

My approach to the Friction Challenge centers on one key insight: **friction is information**. Each failure tells us something about the system. Rather than treating failures as obstacles to overcome with brute force, I treat them as signals that inform adaptive strategies.

The three solutions share common principles:
1. **Observe before acting** - Capture baseline state to detect changes
2. **Adapt dynamically** - Learn from patterns to improve over time
3. **Fail gracefully** - Maintain partial functionality when possible
4. **Leave forensic traces** - Log enough to diagnose any failure

## Task 1: The Unreliable API

**Key Innovation: Adaptive Backoff**

Most solutions use fixed exponential backoff. My solution *learns* the optimal retry timing from observed success/failure patterns. After enough data, it knows that "waiting 3 seconds tends to work better than 1 second for this API."

Other features:
- **Failure fingerprinting** - Classify errors by signature for targeted handling
- **Health score tracking** - Continuous monitoring enables proactive decisions
- **Request deduplication** - Idempotency keys prevent duplicate side effects
- **Statistical analysis** - Track P95 latency, success rates over sliding window

## Task 2: The Silent File Corruption

**Key Innovation: Statistical Anomaly Detection**

Syntax checking catches obvious corruption. My solution goes further by profiling each field statistically, then flagging values that are syntactically valid but statistically improbable (outliers).

Other features:
- **Homoglyph detection** - Find Cyrillic 'a' masquerading as Latin 'a'
- **Mojibake repair** - Fix double-encoded UTF-8 artifacts
- **Confidence scoring** - Quantify certainty of each repair (100% for NULL removal, 70% for date guesses)
- **Multi-pass pipeline** - Raw bytes -> Characters -> Fields -> Statistics

## Task 3: The Ghost in the Machine

**Key Innovation: Environment Fingerprinting**

By capturing a complete snapshot of the environment at startup (working dir, permissions, env vars, disk space, etc.), we can diff against the current state when failures occur to identify what changed.

Other features:
- **Stale lock detection** - Verify lock-holding PIDs are still alive
- **Background watchdog** - Continuously verify critical assumptions
- **Self-healing** - Automatically recover missing temp directories
- **Graceful signal handling** - Clean shutdown on SIGTERM/SIGINT

## Architecture Diagram

```
+-------------------+     +-------------------+     +-------------------+
|   Task 1: API     |     | Task 2: Files     |     | Task 3: Env       |
+-------------------+     +-------------------+     +-------------------+
| AdaptiveBackoff   |     | CorruptionPipeline|     | Watchdog Thread   |
| HealthScore       |     | FieldProfile      |     | LockManager       |
| FailureFingerprint|     | HomoglyphDetector |     | EnvFingerprint    |
+-------------------+     +-------------------+     +-------------------+
         |                         |                         |
         v                         v                         v
+---------------------------------------------------------------+
|                     Observability Layer                       |
|  - Structured logging (timestamp, level, component, message)  |
|  - Metrics tracking (counts, latencies, success rates)        |
|  - Forensic capture (state snapshots on failure)              |
+---------------------------------------------------------------+
```

## Running the Code

Each task file includes a demo function that can be run standalone:

```bash
# Task 1
python submissions/opus-claude-code/task1_unreliable_api.py

# Task 2
python submissions/opus-claude-code/task2_file_corruption.py

# Task 3
python submissions/opus-claude-code/task3_ghost_machine.py
```

## Testing Approach

All solutions are designed to be testable:
- Task 1: Inject a mock API function to test retry logic
- Task 2: Create sample corrupted CSV to test detection
- Task 3: Manipulate environment during execution to test watchdog

## What Makes This Submission Different

1. **Learning over time** - Not just handling failures, but getting better at it
2. **Statistical reasoning** - Using data distributions to catch subtle issues
3. **Proactive monitoring** - Detecting problems before they cause failures
4. **Quantified confidence** - Every repair has a confidence score

This reflects my belief that robust systems are built through understanding, not just resilience.
