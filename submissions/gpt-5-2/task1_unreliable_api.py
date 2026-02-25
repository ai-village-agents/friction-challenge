#!/usr/bin/env python3
"""Task 1: Unreliable API server and resilient client (stdlib only)."""
from __future__ import annotations

import argparse
import contextlib
import json
import random
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterable, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


# ----------------------------- Server --------------------------------------


class _ServerState:
    def __init__(self, seed: int):
        self.seed = seed
        self.request_count = 0
        self.lock = threading.Lock()

    def next_mode(self) -> Tuple[str, int, random.Random]:
        with self.lock:
            self.request_count += 1
            count = self.request_count
        rng = random.Random(self.seed + count)  # mix request count into seed for variety
        # Cover the required failure modes with a slight bias toward success.
        modes = [
            "success",
            "success",
            "http_500",
            "http_503",
            "http_429",
            "conn_drop",
            "truncated",
            "malformed",
            "schema",
            "bad_content_type",
        ]
        mode = rng.choice(modes)
        return mode, count, rng


def _make_handler(state: _ServerState):
    class Handler(BaseHTTPRequestHandler):
        server_version = "UnreliableAPI/1.0"
        error_content_type = "text/plain"

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return  # quiet server logs

        def do_GET(self) -> None:  # noqa: N802
            if self.path != "/data":
                self.send_error(404, "Not Found")
                return

            mode, request_id, rng = state.next_mode()

            if mode == "http_500":
                self.send_error(500, "Internal Server Error")
                return
            if mode == "http_503":
                retry_after = rng.randint(1, 3)
                self.send_response(503, "Service Unavailable")
                self.send_header("Retry-After", str(retry_after))
                self.end_headers()
                return
            if mode == "http_429":
                retry_after = rng.randint(1, 3)
                self.send_response(429, "Too Many Requests")
                self.send_header("Retry-After", str(retry_after))
                self.end_headers()
                return
            if mode == "truncated":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"value": 123, "request_id": "')
                return
            if mode == "conn_drop":
                # Send part of a response and then drop the connection abruptly.
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"value": 123, "request_id": "req-')
                self.wfile.flush()
                self.close_connection = True
                with contextlib.suppress(OSError):
                    self.connection.shutdown(socket.SHUT_RDWR)
                return
            if mode == "malformed":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"value": 123,}')
                return
            if mode == "schema":
                payload = {"value": rng.randint(1, 500)}  # missing request_id
                body = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            value = rng.randint(1, 1000)
            payload = {"value": value, "request_id": f"req-{request_id}"}
            body = json.dumps(payload).encode()
            if mode == "bad_content_type":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def run_server(host: str, port: int, seed: int) -> ThreadingHTTPServer:
    state = _ServerState(seed)
    server = ThreadingHTTPServer((host, port), _make_handler(state))
    return server


# ----------------------------- Client --------------------------------------


class CircuitBreaker:
    def __init__(self, threshold: int, cooldown: float):
        self.threshold = threshold
        self.cooldown = cooldown
        self.failures = 0
        self.state = "closed"
        self.open_until = 0.0

    def allow(self) -> bool:
        if self.state == "open" and time.monotonic() >= self.open_until:
            self.state = "half-open"
        return self.state in {"closed", "half-open"}

    def on_result(self, success: bool) -> None:
        if success:
            self.failures = 0
            self.state = "closed"
            self.open_until = 0.0
            return
        self.failures += 1
        if self.state == "half-open":
            self.state = "open"
            self.open_until = time.monotonic() + self.cooldown
            self.failures = max(self.failures, self.threshold)
            return
        if self.failures >= self.threshold:
            self.state = "open"
            self.open_until = time.monotonic() + self.cooldown


def _make_logger(path: Optional[str], start: float):
    if not path:
        return lambda attempt, event, details: None

    lock = threading.Lock()

    def log(attempt: int, event: str, details: Dict) -> None:
        record = {
            "ts": time.time(),
            "elapsed": round(time.monotonic() - start, 6),
            "attempt": attempt,
            "event": event,
            "details": details,
        }
        line = json.dumps(record, sort_keys=True)
        with lock:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")

    return log


def _validate_payload(data: object) -> Tuple[bool, str]:
    if not isinstance(data, dict):
        return False, "payload_not_object"
    if set(data.keys()) != {"value", "request_id"}:
        return False, "missing_keys"
    value = data.get("value")
    request_id = data.get("request_id")
    if isinstance(value, bool) or not isinstance(value, int):
        return False, "value_not_int"
    if not isinstance(request_id, str):
        return False, "request_id_not_str"
    return True, ""


def fetch_with_resilience(
    url: str,
    deadline_secs: float,
    max_attempts: int,
    circuit_threshold: int,
    circuit_cooldown: float,
    seed: int,
    log_path: Optional[str],
) -> Tuple[Optional[int], int]:
    start = time.monotonic()
    deadline_at = start + deadline_secs
    attempt = 0
    rng = random.Random(seed)
    breaker = CircuitBreaker(circuit_threshold, circuit_cooldown)
    log = _make_logger(log_path, start)

    while attempt < max_attempts and time.monotonic() < deadline_at:
        if not breaker.allow():
            now = time.monotonic()
            remaining_deadline = max(0.0, deadline_at - now)
            remaining_cooldown = max(0.0, breaker.open_until - now)
            log(
                attempt,
                "circuit_open",
                {"open_until": breaker.open_until, "remaining_cooldown": remaining_cooldown},
            )
            if remaining_deadline <= 0:
                break
            sleep_for = min(remaining_cooldown or 0.0, remaining_deadline)
            sleep_for = min(sleep_for if sleep_for > 0 else remaining_deadline, 0.5)
            if sleep_for:
                time.sleep(sleep_for)
            continue

        attempt += 1
        log(attempt, "attempt", {"url": url})
        try:
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=5) as resp:
                status = resp.getcode()
                body = resp.read()
                ctype = resp.headers.get("Content-Type", "")
                retry_after_hdr = resp.headers.get("Retry-After")

                if status in {429, 503}:
                    delay = float(retry_after_hdr) if retry_after_hdr else 1.0
                    log(attempt, "retry_after", {"status": status, "delay": delay})
                    breaker.on_result(False)
                    remaining = deadline_at - time.monotonic()
                    if remaining > 0:
                        time.sleep(min(delay, remaining))
                    continue
                if status >= 500:
                    log(attempt, "server_error", {"status": status})
                    breaker.on_result(False)
                    # fall through to backoff
                elif status >= 400:
                    log(attempt, "client_error", {"status": status})
                    breaker.on_result(False)
                    # fall through to backoff
                else:
                    if "application/json" not in ctype.lower():
                        log(attempt, "bad_content_type", {"content_type": ctype})
                        breaker.on_result(False)
                        # fall through to backoff
                    else:
                        try:
                            parsed = json.loads(body.decode("utf-8"))
                        except json.JSONDecodeError as exc:
                            log(attempt, "json_error", {"error": str(exc)})
                            breaker.on_result(False)
                        else:
                            ok, reason = _validate_payload(parsed)
                            if not ok:
                                log(attempt, "schema_error", {"reason": reason, "payload": parsed})
                                breaker.on_result(False)
                            else:
                                breaker.on_result(True)
                                log(attempt, "success", {"payload": parsed})
                                return int(parsed["value"]), attempt
        except HTTPError as exc:
            status = getattr(exc, "code", None)
            retry_after_hdr = exc.headers.get("Retry-After") if exc.headers else None
            if status in {429, 503}:
                delay = float(retry_after_hdr) if retry_after_hdr else 1.0
                log(attempt, "retry_after_error", {"status": status, "delay": delay})
                breaker.on_result(False)
                remaining = deadline_at - time.monotonic()
                if remaining > 0:
                    time.sleep(min(delay, remaining))
                continue
            log(attempt, "http_error", {"status": status})
            breaker.on_result(False)
        except URLError as exc:
            log(attempt, "network_error", {"reason": str(exc)})
            breaker.on_result(False)
        except Exception as exc:  # noqa: BLE001
            log(attempt, "exception", {"type": exc.__class__.__name__, "msg": str(exc)})
            breaker.on_result(False)

        # Exponential backoff with full jitter, capped at 2s.
        backoff = min(2.0, 0.2 * (2 ** min(attempt - 1, 6)))
        delay = rng.uniform(0, backoff)
        remaining = deadline_at - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(delay, remaining))

    return None, attempt


# ----------------------------- CLI / Demo ----------------------------------


def _run_demo(args: argparse.Namespace) -> int:
    server = run_server(args.host, args.port, args.seed)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    url = f"http://{host}:{port}/data"
    value, attempts = fetch_with_resilience(
        url,
        deadline_secs=args.deadline_secs,
        max_attempts=args.max_attempts,
        circuit_threshold=args.circuit_threshold,
        circuit_cooldown=args.circuit_cooldown_secs,
        seed=args.seed,
        log_path=args.log_jsonl,
    )
    server.shutdown()
    thread.join(timeout=2)
    if value is None:
        print("demo failed", file=sys.stderr)
        return 1
    print(value)
    print(f"demo completed after {attempts} attempts", file=sys.stderr)
    return 0


def _serve(args: argparse.Namespace) -> int:
    server = run_server(args.host, args.port, args.seed)
    host, port = server.server_address
    print(f"serving on http://{host}:{port}/data", file=sys.stderr)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()
    return 0


def _solve(args: argparse.Namespace) -> int:
    if not args.url:
        print("--url is required in solve mode", file=sys.stderr)
        return 1
    parsed = urlparse(args.url)
    if parsed.scheme not in {"http", "https"}:
        print("unsupported URL scheme", file=sys.stderr)
        return 1
    value, attempts = fetch_with_resilience(
        args.url,
        deadline_secs=args.deadline_secs,
        max_attempts=args.max_attempts,
        circuit_threshold=args.circuit_threshold,
        circuit_cooldown=args.circuit_cooldown_secs,
        seed=args.seed,
        log_path=args.log_jsonl,
    )
    if value is None:
        print("failed to fetch value within deadline", file=sys.stderr)
        return 1
    print(value)
    print(f"attempts={attempts}", file=sys.stderr)
    return 0


def _parse_args(argv: Optional[Iterable[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["serve", "solve", "demo"], required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--url")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--deadline-secs", type=float, default=15.0)
    parser.add_argument("--max-attempts", type=int, default=50)
    parser.add_argument("--circuit-threshold", type=int, default=7)
    parser.add_argument("--circuit-cooldown-secs", type=float, default=2.5)
    parser.add_argument("--log-jsonl")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if args.mode == "serve":
        return _serve(args)
    if args.mode == "demo":
        return _run_demo(args)
    return _solve(args)


if __name__ == "__main__":
    import sys

    raise SystemExit(main())
