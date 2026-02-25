#!/usr/bin/env python3
"""
Task 1: The Unreliable API
==========================
Problem: API intermittently fails with various error codes and malformed responses.
Need robust retry mechanism with exponential backoff, circuit breaker, and response validation.

Failure modes:
- HTTP 429: rate limiting with Retry-After
- HTTP 500/503: transient server errors
- HTTP 200 with malformed JSON/missing fields
- Connection timeouts

Workaround: Layered retry with exponential backoff, circuit breaker, and validation.
"""

import json
import time
import random
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError


class UnreliableAPIHandler(BaseHTTPRequestHandler):
    """Simulates API with multiple failure modes."""
    call_count = 0
    lock = threading.Lock()

    def log_message(self, format, *args):
        pass  # Suppress logs

    def do_GET(self):
        with UnreliableAPIHandler.lock:
            UnreliableAPIHandler.call_count += 1
            count = UnreliableAPIHandler.call_count

        # Failure sequence before success
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
            # Malformed JSON
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"data": [1, 2, 3, ')
        elif count == 4:
            # Missing required fields
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')
        elif count == 5:
            self.send_response(503)
            self.send_header("Retry-After", "1")
            self.end_headers()
            self.wfile.write(b"Service Unavailable")
        else:
            # Success
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            payload = {
                "data": {
                    "value": 42,
                    "message": "Success!",
                    "timestamp": time.time()
                }
            }
            self.wfile.write(json.dumps(payload).encode())


class CircuitBreaker:
    """Prevents repeated calls to failing service."""
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
            print(f"    [CircuitBreaker] OPEN — {self.failure_count} failures")

    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def is_open(self):
        if self.state == "OPEN":
            elapsed = time.time() - self.last_failure_time
            if elapsed > self.recovery_timeout:
                self.state = "HALF_OPEN"
                print(f"    [CircuitBreaker] HALF_OPEN (trying recovery)")
                return False
            return True
        return False


def fetch_with_retry(url, max_retries=15, initial_backoff=0.1):
    """Fetch with exponential backoff, circuit breaker, and validation."""
    circuit_breaker = CircuitBreaker(failure_threshold=7, recovery_timeout=10)
    
    for attempt in range(1, max_retries + 1):
        if circuit_breaker.is_open():
            print(f"  Attempt {attempt}: [CIRCUIT BREAKER OPEN] Waiting...")
            time.sleep(1)
            continue

        try:
            print(f"  Attempt {attempt}: Fetching from {url}...", end=" ", flush=True)
            req = Request(url, headers={"User-Agent": "RobustRetryClient/1.0"})
            response = urlopen(req, timeout=5)
            
            # Parse response
            data = json.loads(response.read().decode('utf-8'))
            
            # Validate schema
            if "data" not in data or not isinstance(data["data"], dict):
                raise ValueError("Invalid schema: missing or malformed 'data'")
            if "value" not in data["data"]:
                raise ValueError("Invalid schema: missing 'value' in data")
            
            print(f"✓ Success!")
            circuit_breaker.record_success()
            return data
            
        except HTTPError as e:
            circuit_breaker.record_failure()
            if e.code == 429 or e.code == 503:
                retry_after = e.headers.get("Retry-After", str(min(2 ** attempt, 30)))
                wait_time = float(retry_after)
                print(f"✗ HTTP {e.code}, waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"✗ HTTP {e.code}")
                backoff = initial_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                time.sleep(min(backoff, 30))
        except (URLError, json.JSONDecodeError, ValueError) as e:
            circuit_breaker.record_failure()
            print(f"✗ {type(e).__name__}")
            backoff = initial_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
            time.sleep(min(backoff, 30))
    
    raise RuntimeError(f"Failed to fetch after {max_retries} attempts")


if __name__ == "__main__":
    print("Task 1: The Unreliable API")
    print("=" * 60)
    
    # Start mock server
    server = HTTPServer(("127.0.0.1", 0), UnreliableAPIHandler)
    port = server.server_port
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    
    print(f"Mock API running on http://127.0.0.1:{port}/data\n")
    
    try:
        start = time.time()
        result = fetch_with_retry(f"http://127.0.0.1:{port}/data")
        elapsed = time.time() - start
        
        print(f"\n✓ Successfully retrieved data: {result}")
        print(f"✓ Total time: {elapsed:.2f}s")
    except Exception as e:
        print(f"\n✗ Failed: {e}")
    finally:
        server.shutdown()
