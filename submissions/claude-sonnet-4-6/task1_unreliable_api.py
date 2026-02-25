#!/usr/bin/env python3
"""
Task 1: The Unreliable API
==========================
Problem: An API endpoint intermittently fails with various error codes and
malformed responses. We need robust error handling and retry logic to
successfully retrieve the required data.

Diagnosis:
- HTTP 429: rate limiting → respect Retry-After header
- HTTP 500/503: transient server errors → exponential backoff
- HTTP 200 with malformed JSON → validate response schema, retry
- Missing required fields in response → treat as failure, retry
- Connection timeouts → set short timeout, retry

Workaround: Layered retry strategy with:
1. Exponential backoff with jitter (prevents thundering herd)
2. Circuit breaker (fail fast after sustained outage)
3. Response validation (rejects malformed 200s)
4. Retry-After header respect (for 429s/503s)
"""

import json
import time
import random
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ─── Simulated Unreliable Server ────────────────────────────────────────────

class UnreliableAPIHandler(BaseHTTPRequestHandler):
    """Simulates an API that fails in multiple ways before eventually succeeding."""
    call_count = 0
    lock = threading.Lock()

    def log_message(self, format, *args):
        pass  # Suppress server logs

    def do_GET(self):
        with UnreliableAPIHandler.lock:
            UnreliableAPIHandler.call_count += 1
            count = UnreliableAPIHandler.call_count

        # Failure modes cycle before success on attempts 6, 7, 8...
        if count == 1:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Internal Server Error")
        elif count == 2:
            self.send_response(429)
            self.send_header("Retry-After", "1")
            self.end_headers()
            self.wfile.write(b"Too Many Requests")
        elif count == 3:
            # Malformed JSON (truncated)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"data": [1, 2, 3, ')
        elif count == 4:
            # Valid JSON but missing required schema fields
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')  # Missing "data.value"
        elif count == 5:
            self.send_response(503)
            self.send_header("Retry-After", "1")
            self.end_headers()
            self.wfile.write(b"Service Unavailable")
        else:
            # Success!
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            payload = {"data": {"value": 42, "message": "Success!", "timestamp": time.time()}}
            self.wfile.write(json.dumps(payload).encode())


# ─── Circuit Breaker ─────────────────────────────────────────────────────────

class CircuitBreaker:
    """Prevents hammering a completely failed service indefinitely."""
    def __init__(self, failure_threshold=7, recovery_timeout=10):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None
        self.state = "CLOSED"

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            print(f"    [CircuitBreaker] OPEN — {self.failure_count} consecutive failures")

    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def can_attempt(self):
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                print("    [CircuitBreaker] HALF_OPEN — testing recovery")
                return True
            return False
        return True  # HALF_OPEN


# ─── Response Validator ──────────────────────────────────────────────────────

def validate_response(data: dict) -> bool:
    """Validate response contains required schema: data.value must be present."""
    return (
        isinstance(data, dict) and
        "data" in data and
        isinstance(data["data"], dict) and
        "value" in data["data"]
    )


# ─── Robust API Client ───────────────────────────────────────────────────────

def fetch_with_retry(url: str, max_attempts: int = 10, timeout: float = 2.0) -> dict:
    """
    Robust API client implementing the full workaround stack:
    - Exponential backoff with jitter
    - Retry-After header respect
    - Schema validation (rejects malformed 200s)
    - Circuit breaker pattern
    """
    circuit_breaker = CircuitBreaker(failure_threshold=7, recovery_timeout=5)
    base_delay = 0.2

    for attempt in range(1, max_attempts + 1):
        if not circuit_breaker.can_attempt():
            raise RuntimeError(f"Circuit breaker OPEN — service appears down")

        try:
            print(f"  Attempt {attempt}/{max_attempts}...", end=" ")
            response = urlopen(Request(url), timeout=timeout)
            body = response.read().decode("utf-8")

            # Validate JSON parsability
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                print(f"❌ Malformed JSON ({e})")
                circuit_breaker.record_failure()
                delay = base_delay * (2 ** min(attempt, 4)) + random.uniform(0, 0.1)
                time.sleep(min(delay, 5.0))
                continue

            # Validate required schema
            if not validate_response(data):
                print(f"❌ Schema invalid (got: {list(data.keys())})")
                circuit_breaker.record_failure()
                delay = base_delay * (2 ** min(attempt, 4)) + random.uniform(0, 0.1)
                time.sleep(min(delay, 5.0))
                continue

            print(f"✅ value={data['data']['value']} — \"{data['data']['message']}\"")
            circuit_breaker.record_success()
            return data

        except HTTPError as e:
            retry_after = e.headers.get("Retry-After")
            if e.code in (429, 503):
                wait = float(retry_after) if retry_after else base_delay * (2 ** min(attempt, 4))
                print(f"❌ HTTP {e.code} — waiting {wait:.1f}s (Retry-After)")
                circuit_breaker.record_failure()
                time.sleep(wait + random.uniform(0, 0.1))
            elif e.code == 500:
                delay = base_delay * (2 ** min(attempt, 4)) + random.uniform(0, 0.2)
                print(f"❌ HTTP 500 — backoff {delay:.2f}s")
                circuit_breaker.record_failure()
                time.sleep(min(delay, 5.0))
            else:
                print(f"❌ HTTP {e.code} — non-retryable, re-raising")
                raise

        except (URLError, TimeoutError, OSError) as e:
            delay = base_delay * (2 ** min(attempt, 4)) + random.uniform(0, 0.2)
            print(f"❌ Network error ({type(e).__name__}) — backoff {delay:.2f}s")
            circuit_breaker.record_failure()
            time.sleep(min(delay, 5.0))

    raise RuntimeError(f"Exhausted all {max_attempts} retry attempts")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("TASK 1: The Unreliable API — Workaround Demo")
    print("=" * 60)
    print()
    print("Diagnosis:")
    print("  The API exhibits 5 failure modes before succeeding:")
    print("  1) HTTP 500 (server error)")
    print("  2) HTTP 429 (rate limit) with Retry-After")
    print("  3) HTTP 200 with truncated/malformed JSON")
    print("  4) HTTP 200 with valid JSON but missing required fields")
    print("  5) HTTP 503 (service unavailable) with Retry-After")
    print()
    print("Workaround: Exponential backoff + schema validation + circuit breaker")
    print()

    server = HTTPServer(("localhost", 8001), UnreliableAPIHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.1)

    start = time.time()
    try:
        result = fetch_with_retry("http://localhost:8001/data", max_attempts=10)
        elapsed = time.time() - start
        print()
        print(f"RESULT: {json.dumps(result, indent=2)}")
        print(f"  Retrieved after {elapsed:.2f}s, {UnreliableAPIHandler.call_count} total API calls")
    except RuntimeError as e:
        print(f"\n❌ FAILED: {e}")
    finally:
        server.shutdown()

if __name__ == "__main__":
    main()
