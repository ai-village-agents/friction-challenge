#!/usr/bin/env python3
"""
Task 1: The Unreliable API
===========================
Challenge: An API endpoint intermittently fails with various error codes and
malformed responses. Implement robust error handling to reliably retrieve data.

Diagnosis (5 failure modes identified):
1. HTTP 500 — Transient server error
2. HTTP 429 — Rate limiting (with Retry-After header)
3. HTTP 503 — Service unavailable (with Retry-After header)
4. HTTP 200 + malformed JSON — Truncated or garbage body
5. HTTP 200 + missing fields — Schema-valid JSON, semantically incomplete

Real-World Context:
In the AI Village, I've encountered all five of these failure modes working
with the GitHub API. Git pushes return HTTP 500 intermittently, rate limits
are enforced at 5000 req/hr, and the Contents API sometimes returns partial
JSON when servers are under load. The workarounds below are battle-tested.

Workaround Strategy:
- Adaptive retry with exponential backoff + jitter (prevents thundering herd)
- Retry-After header parsing (respects server guidance)
- Response schema validation (catches "silent" failures)
- Circuit breaker with half-open probe (fails fast during sustained outages)
- Full attempt logging for post-mortem diagnosis
"""

import json
import os
import random
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from typing import Any, Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════════════
# SIMULATED UNRELIABLE API SERVER
# ═══════════════════════════════════════════════════════════════════════════

class UnreliableServer(BaseHTTPRequestHandler):
    """Simulates an API with 5 distinct failure modes before succeeding."""
    _counter = 0
    _lock = threading.Lock()

    def log_message(self, *args):
        pass  # Suppress default logging

    def do_GET(self):
        with self._lock:
            UnreliableServer._counter += 1
            n = UnreliableServer._counter

        if n == 1:
            # Mode 1: HTTP 500 Internal Server Error
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Internal Server Error")
        elif n == 2:
            # Mode 2: HTTP 429 Rate Limited
            self.send_response(429)
            self.send_header("Retry-After", "1")
            self.end_headers()
            self.wfile.write(b"Rate limit exceeded")
        elif n == 3:
            # Mode 3: HTTP 200 but truncated JSON
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"data": {"value": 42, "items": [1, 2,')
        elif n == 4:
            # Mode 4: HTTP 200, valid JSON, but missing required "data.value"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok", "meta": {}}).encode())
        elif n == 5:
            # Mode 5: HTTP 503 Service Unavailable
            self.send_response(503)
            self.send_header("Retry-After", "2")
            self.end_headers()
            self.wfile.write(b"Service Unavailable")
        else:
            # Success
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            payload = {
                "status": "success",
                "data": {
                    "value": 42,
                    "message": "The answer to everything",
                    "timestamp": time.time(),
                },
            }
            self.wfile.write(json.dumps(payload).encode())


# ═══════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ═══════════════════════════════════════════════════════════════════════════

class CircuitBreaker:
    """
    Three-state circuit breaker: CLOSED → OPEN → HALF_OPEN → CLOSED.

    When too many consecutive failures occur, the breaker opens and rejects
    requests immediately (fail fast). After a cooldown, it enters half-open
    state and allows a single probe request through.
    """
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(self, threshold: int = 8, cooldown: float = 10.0):
        self.threshold = threshold
        self.cooldown = cooldown
        self.failures = 0
        self.state = self.CLOSED
        self.opened_at: Optional[float] = None

    def record_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold:
            self.state = self.OPEN
            self.opened_at = time.time()

    def record_success(self) -> None:
        self.failures = 0
        self.state = self.CLOSED

    def allow_request(self) -> bool:
        if self.state == self.CLOSED:
            return True
        if self.state == self.OPEN:
            if self.opened_at and time.time() - self.opened_at > self.cooldown:
                self.state = self.HALF_OPEN
                return True
            return False
        return True  # HALF_OPEN: allow probe


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════

def validate_response(data: Any) -> Tuple[bool, str]:
    """
    Validate that a response meets the expected schema.

    Why this matters: A response with HTTP 200 and valid JSON can still be
    a failure if required fields are missing. Without schema validation,
    these "silent successes" propagate corrupted data downstream. In the
    AI Village, the GitHub Contents API sometimes returns {"type": "file"}
    without the "content" field under load — classic silent failure.
    """
    if not isinstance(data, dict):
        return False, f"Expected dict, got {type(data).__name__}"
    if "data" not in data:
        return False, "Missing required key 'data'"
    if not isinstance(data["data"], dict):
        return False, f"'data' must be dict, got {type(data['data']).__name__}"
    if "value" not in data["data"]:
        return False, "Missing required key 'data.value'"
    return True, "OK"


# ═══════════════════════════════════════════════════════════════════════════
# ROBUST API CLIENT
# ═══════════════════════════════════════════════════════════════════════════

class AttemptRecord:
    """Records a single API attempt for diagnostic logging."""
    def __init__(self, number: int, status: Optional[int], outcome: str,
                 delay: float, detail: str):
        self.number = number
        self.status = status
        self.outcome = outcome
        self.delay = delay
        self.detail = detail
        self.timestamp = time.time()

    def __repr__(self):
        s = f"  #{self.number}: {self.outcome}"
        if self.status is not None:
            s += f" (HTTP {self.status})"
        s += f" — {self.detail}"
        if self.delay > 0:
            s += f" [wait {self.delay:.2f}s]"
        return s


def compute_delay(attempt: int, base: float = 0.3, cap: float = 8.0,
                  response: Optional[Any] = None) -> float:
    """
    Compute retry delay using exponential backoff with jitter.

    If a Retry-After header is present and reasonable, we honor it.
    Jitter is critical: without it, N clients that fail simultaneously
    will all retry at the same instant, causing another failure cascade.
    """
    # Check for Retry-After header
    if response is not None:
        retry_after = None
        if hasattr(response, 'headers'):
            retry_after = response.headers.get("Retry-After")
        elif hasattr(response, 'getheader'):
            retry_after = response.getheader("Retry-After")
        if retry_after:
            try:
                ra = float(retry_after)
                if 0 < ra <= cap * 2:
                    return min(ra, cap) + random.uniform(0, 0.1)
            except ValueError:
                pass

    # Exponential backoff with full jitter
    exp_delay = min(base * (2 ** (attempt - 1)), cap)
    return random.uniform(0, exp_delay)


def fetch_with_resilience(url: str, max_attempts: int = 12,
                          timeout: float = 3.0) -> Dict[str, Any]:
    """
    Fetch JSON from an unreliable endpoint with full resilience stack.

    Returns the validated response payload on success.
    Raises RuntimeError on exhaustion or circuit break.
    """
    breaker = CircuitBreaker(threshold=8, cooldown=5.0)
    log: List[AttemptRecord] = []

    for attempt in range(1, max_attempts + 1):
        if not breaker.allow_request():
            rec = AttemptRecord(attempt, None, "REJECTED", 0.0,
                                "Circuit breaker OPEN")
            log.append(rec)
            raise RuntimeError(f"Circuit breaker open after {breaker.failures} failures")

        status = None
        delay = 0.0
        http_resp = None

        try:
            req = Request(url, headers={"Accept": "application/json"})
            http_resp = urlopen(req, timeout=timeout)
            status = http_resp.status
            body = http_resp.read().decode("utf-8")

            # Try to parse JSON
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                delay = compute_delay(attempt)
                rec = AttemptRecord(attempt, status, "MALFORMED_JSON", delay,
                                    f"JSON parse error: {e}")
                log.append(rec)
                print(rec)
                breaker.record_failure()
                time.sleep(delay)
                continue

            # Validate schema
            valid, reason = validate_response(data)
            if not valid:
                delay = compute_delay(attempt)
                rec = AttemptRecord(attempt, status, "SCHEMA_INVALID", delay,
                                    f"Schema validation failed: {reason}")
                log.append(rec)
                print(rec)
                breaker.record_failure()
                time.sleep(delay)
                continue

            # Success!
            rec = AttemptRecord(attempt, status, "SUCCESS", 0.0,
                                f"value={data['data']['value']}")
            log.append(rec)
            print(rec)
            breaker.record_success()
            return data

        except HTTPError as e:
            status = e.code
            if status == 429 or status == 503:
                delay = compute_delay(attempt, response=e)
                outcome = "RATE_LIMITED" if status == 429 else "UNAVAILABLE"
                rec = AttemptRecord(attempt, status, outcome, delay,
                                    f"Retry-After honored")
                log.append(rec)
                print(rec)
                breaker.record_failure()
                time.sleep(delay)
            elif 500 <= status < 600:
                delay = compute_delay(attempt)
                rec = AttemptRecord(attempt, status, "SERVER_ERROR", delay,
                                    "Transient server error")
                log.append(rec)
                print(rec)
                breaker.record_failure()
                time.sleep(delay)
            else:
                rec = AttemptRecord(attempt, status, "FATAL", 0.0,
                                    f"Non-retryable HTTP {status}")
                log.append(rec)
                print(rec)
                raise RuntimeError(f"Fatal HTTP error: {status}")

        except (URLError, TimeoutError, OSError) as e:
            delay = compute_delay(attempt)
            rec = AttemptRecord(attempt, None, "NETWORK_ERROR", delay,
                                f"{type(e).__name__}: {e}")
            log.append(rec)
            print(rec)
            breaker.record_failure()
            time.sleep(delay)

    raise RuntimeError(f"Exhausted {max_attempts} attempts")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN — DEMONSTRATION
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 65)
    print("TASK 1: The Unreliable API")
    print("=" * 65)
    print()
    print("Diagnosis: API exhibits 5 failure modes:")
    print("  1. HTTP 500 — Server error (exponential backoff)")
    print("  2. HTTP 429 — Rate limited (respect Retry-After)")
    print("  3. HTTP 200 + truncated JSON (retry as transient)")
    print("  4. HTTP 200 + missing fields (schema validation catch)")
    print("  5. HTTP 503 — Unavailable (respect Retry-After)")
    print()
    print("Workaround: Adaptive retry + schema validation + circuit breaker")
    print("-" * 65)
    print()

    # Start simulated server
    server = HTTPServer(("127.0.0.1", 18901), UnreliableServer)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.05)

    start = time.time()
    try:
        result = fetch_with_resilience("http://127.0.0.1:18901/api/data",
                                       max_attempts=12, timeout=3.0)
        elapsed = time.time() - start
        print()
        print(f"RESULT: {json.dumps(result, indent=2)}")
        print(f"  Retrieved in {elapsed:.2f}s after "
              f"{UnreliableServer._counter} API calls")
        print()
        print("SUCCESS: All 5 failure modes survived.")
    except RuntimeError as e:
        print(f"\nFAILED: {e}")
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
