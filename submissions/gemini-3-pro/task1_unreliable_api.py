import json
import time
import random
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# --- Circuit Breaker ---
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=5):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.state = "CLOSED"
        self.last_failure_time = 0

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
            print(f"[CircuitBreaker] State changed to OPEN (Failures: {self.failures})")

    def record_success(self):
        self.failures = 0
        self.state = "CLOSED"

    def allow_request(self):
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                print("[CircuitBreaker] State changed to HALF_OPEN (Testing recovery)")
                return True
            return False
        return True # HALF_OPEN allows 1 request

# --- Mock Server ---
class UnreliableHandler(BaseHTTPRequestHandler):
    request_count = 0
    lock = threading.Lock()

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        with UnreliableHandler.lock:
            UnreliableHandler.request_count += 1
            count = UnreliableHandler.request_count

        # Simulate failures
        if count == 1:
            self.send_error(500, "Internal Server Error")
        elif count == 2:
            self.send_response(429)
            self.send_header("Retry-After", "1")
            self.end_headers()
        elif count == 3:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{'broken': 'json") # Malformed
        elif count == 4:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}') # Missing 'data' field
        elif count == 5:
            self.send_response(503)
            self.send_header("Retry-After", "1")
            self.end_headers()
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps({"data": "Success!", "id": count}).encode())

# --- Robust Client ---
def fetch_data(url):
    cb = CircuitBreaker()
    max_retries = 10
    base_delay = 0.5
    failure_count = 0

    for attempt in range(1, max_retries + 1):
        if not cb.allow_request():
            print(f"Attempt {attempt}: Circuit Breaker OPEN. Skipping.")
            time.sleep(1)
            continue

        print(f"Attempt {attempt}...", end=" ")
        
        req = Request(url)
        try:
            with urlopen(req, timeout=2) as response:
                body = response.read().decode()
                
                try:
                    data = json.loads(body)
                except json.JSONDecodeError:
                    print("Failed: Malformed JSON")
                    cb.record_failure()
                    time.sleep(base_delay * (2 ** failure_count) + random.uniform(0, 0.5))
                    failure_count += 1
                    continue

                if "data" not in data:
                    print("Failed: Missing 'data' field")
                    cb.record_failure()
                    time.sleep(base_delay * (2 ** failure_count) + random.uniform(0, 0.5))
                    failure_count += 1
                    continue
                
                print(f"Success! Received: {data}")
                cb.record_success()
                return data

        except HTTPError as e:
            cb.record_failure()
            if e.code in [429, 503]:
                retry_after = e.headers.get("Retry-After")
                wait_time = float(retry_after) if retry_after else base_delay * (2 ** failure_count)
                print(f"Failed: HTTP {e.code}. Retry-After: {wait_time}s")
                time.sleep(wait_time)
            else:
                print(f"Failed: HTTP {e.code}")
                time.sleep(base_delay * (2 ** failure_count) + random.uniform(0, 0.5))
            failure_count += 1

        except Exception as e:
            cb.record_failure()
            print(f"Failed: Network Error ({e})")
            time.sleep(base_delay * (2 ** failure_count) + random.uniform(0, 0.5))
            failure_count += 1

    raise Exception("Max retries exceeded")

# --- Main Execution ---
if __name__ == "__main__":
    server = HTTPServer(("localhost", 8082), UnreliableHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    print("Server started on port 8082")
    
    try:
        fetch_data("http://localhost:8082")
    finally:
        server.shutdown()
