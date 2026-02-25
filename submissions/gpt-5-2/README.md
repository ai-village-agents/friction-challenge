# GPT-5.2 — Friction Challenge Submission

All scripts are **stdlib-only** and intended for **Python 3.11+**.

Design goals across tasks:
- **Deterministic demos** (seeded) to reproduce failure modes.
- **Budgets**: explicit `--deadline-secs` and `--max-attempts`.
- **Resilience patterns**: exponential backoff + jitter, `Retry-After` compliance, circuit breaker (incl. half-open probing), atomic file operations.
- **Observability**: optional **JSONL logs** (one JSON object per event) for forensics.

Directory: `submissions/gpt-5-2/`

---

## Task 1 — The Unreliable API

Script: `task1_unreliable_api.py`

### What it simulates
A local `ThreadingHTTPServer` with `/data` that intermittently returns:
- HTTP **500** / **503**
- HTTP **429** with `Retry-After: 1–3`
- **truncated JSON** / **malformed JSON**
- **schema mismatch** (missing required keys)
- **wrong Content-Type** on an otherwise-valid JSON payload
- **connection drop** mid-response

Valid success payload is strict JSON:
```json
{"value": 123, "request_id": "req-4"}
```

### What the client does
- Enforces an overall **deadline** and **attempt budget**.
- Uses **exponential backoff with full jitter** (capped at 2s).
- Respects `Retry-After` on 429/503.
- **Strict parsing and validation** (JSON decode + exact key set + type checks + content-type check).
- **Circuit breaker**: opens after N consecutive failures; after cooldown it enters half-open and probes.
- Optional **JSONL** logging (`--log-jsonl`) with fields: `ts`, `elapsed`, `attempt`, `event`, `details`.

### How to run
Demo (starts server on ephemeral port, runs client, shuts down):
```bash
python submissions/gpt-5-2/task1_unreliable_api.py --mode demo --seed 13 --deadline-secs 8 --max-attempts 30
```

Serve:
```bash
python submissions/gpt-5-2/task1_unreliable_api.py --mode serve --host 127.0.0.1 --port 8000 --seed 1
```

Solve:
```bash
python submissions/gpt-5-2/task1_unreliable_api.py --mode solve \
  --url http://127.0.0.1:8000/data \
  --seed 1 --deadline-secs 15 --max-attempts 50 \
  --circuit-threshold 7 --circuit-cooldown-secs 2.5 \
  --log-jsonl ./task1_client.jsonl
```

On success, the recovered `value` is printed to **stdout**.

---

## Task 2 — The Silent File Corruption

Script: `task2_file_corruption.py`

### What it simulates
A CSV-like file with header:
`id,name,date,score`

Corruptions injected include:
- NUL bytes (`\x00`)
- zero-width characters
- control characters
- Unicode homoglyph substitutions (Cyrillic→Latin)
- impossible dates (e.g. `2025-02-30`)
- out-of-range scores (e.g. 150)
- structural damage (tab delimiters, mixed newlines)

### Repair strategy
Pipeline: **scan → normalize/repair → validate → process**
- Decode with `errors="replace"`.
- Normalize Unicode, strip zero-width and control chars.
- Map known homoglyphs back to Latin.
- Normalize newlines to `\n`.
- Invalid dates are cleared to empty; invalid scores are clamped into `0..100` (or cleared if unparsable).
- Output is intended to be **idempotent**: running repair again should not introduce new changes.

### How to run
Demo (generate clean → corrupt → repair → validate/process; prints a JSON report):
```bash
python submissions/gpt-5-2/task2_file_corruption.py --mode demo
```

Generate a corrupted file:
```bash
python submissions/gpt-5-2/task2_file_corruption.py --mode corrupt --output ./corrupt.csv
```

Repair:
```bash
python submissions/gpt-5-2/task2_file_corruption.py --mode repair \
  --input ./corrupt.csv --output ./repaired.csv --report-json ./repair_report.json
```

Process (requires validation to pass):
```bash
python submissions/gpt-5-2/task2_file_corruption.py --mode process \
  --input ./repaired.csv --report-json ./summary.json
```

---

## Task 3 — The Ghost in the Machine

Script: `task3_ghost_machine.py`

### What it simulates
An automation workflow that fails due to **environmental sabotage**, not deterministic code bugs:
- `REQUIRED_TOKEN` disappears mid-run
- temp/work directory is deleted
- a **stale lock file** exists (dead PID / old heartbeat)
- unsafe writes to shared state

### Stabilization strategy
- **Atomic lock acquisition** (`O_CREAT|O_EXCL`) with stale detection (`os.kill(pid, 0)` + heartbeat age).
- **Environment caching**: capture required env vars at start and restore each attempt.
- **Temp dir resurrection**: ensure the work directory exists before steps.
- **Atomic write** to shared state via `os.replace`.

### How to run
Demo (starts a background sabotager thread; runs flaky then stable):
```bash
python submissions/gpt-5-2/task3_ghost_machine.py --mode demo --max-attempts 5 --deadline-secs 5
```

Run a single flaky attempt:
```bash
python submissions/gpt-5-2/task3_ghost_machine.py --mode run_flaky
```

Run the stable runner:
```bash
python submissions/gpt-5-2/task3_ghost_machine.py --mode run_stable --max-attempts 20 --deadline-secs 20 --log-jsonl ./task3.jsonl
```

---

## Smoke tests

`test_smoke.py` runs demo modes for all three tasks:
```bash
python -m unittest -v submissions/gpt-5-2/test_smoke.py
```
