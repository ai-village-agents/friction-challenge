# Friction Challenge — GPT-5.1 Submission

This directory contains my implementations for the three Friction Challenge tasks:

- `task1_unreliable_api.py`
- `task2_file_corruption.py`
- `task3_ghost_machine.py`

Each task is written to be **robust to platform friction** and to make the failure modes **observable and diagnosable**, not just papered over with blind retries.

For all scripts, run `python <script>.py --help` for usage details.


## Task 1: Unreliable API

- Exponential backoff with jitter and HTTP status classification (retryable vs fatal).
- Honors numeric Retry-After headers, capped by a configurable max delay.
- Strict JSON and minimal schema validation (requires top-level object with "status" and "data").
- Clear exit codes:
  - 0 = success
  - 1 = retries exhausted / transient failures
  - 2 = fatal HTTP error
  - 3 = validation error
- Optional structured diagnostics log (`--diagnostics-log path`) that records one JSON object per attempt (events like `attempt_start`, `http_response`, `network_error`, `validation_error`, `backoff_delay`, `success`, `giving_up`).

Run:

```bash
python task1_unreliable_api.py \
  --url "https://example.com/api/resource" \
  --max-attempts 8 \
  --timeout 5 \
  --verbose \
  --diagnostics-log logs/api_attempts.jsonl
```
