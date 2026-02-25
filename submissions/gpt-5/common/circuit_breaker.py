"""A simple, thread-safe circuit breaker with CLOSED/OPEN/HALF_OPEN."""
from __future__ import annotations

from typing import Callable, Any
import threading
import time


class CircuitOpenError(RuntimeError):
    """Raised when the breaker is OPEN and a call is attempted."""


class CircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout_s: float = 5.0,
        half_open_max_calls: int = 1,
    ) -> None:
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be > 0")
        if recovery_timeout_s <= 0:
            raise ValueError("recovery_timeout_s must be > 0")
        if half_open_max_calls <= 0:
            raise ValueError("half_open_max_calls must be > 0")
        self._failure_threshold = failure_threshold
        self._recovery_timeout_s = recovery_timeout_s
        self._half_open_max_calls = half_open_max_calls

        self._lock = threading.Lock()
        self._state = "CLOSED"
        self._fail_count = 0
        self._opened_at = 0.0
        self._half_open_in_flight = 0

    def state(self) -> str:
        with self._lock:
            return self._state

    def _maybe_transition_locked(self) -> None:
        if self._state == "OPEN":
            if (time.monotonic() - self._opened_at) >= self._recovery_timeout_s:
                self._state = "HALF_OPEN"
                self._half_open_in_flight = 0

    def record_success(self) -> None:
        with self._lock:
            self._fail_count = 0
            self._state = "CLOSED"
            self._half_open_in_flight = 0

    def record_failure(self) -> None:
        with self._lock:
            if self._state == "CLOSED":
                self._fail_count += 1
                if self._fail_count >= self._failure_threshold:
                    self._state = "OPEN"
                    self._opened_at = time.monotonic()
            elif self._state == "HALF_OPEN":
                self._state = "OPEN"
                self._opened_at = time.monotonic()
                self._half_open_in_flight = 0
            # if OPEN, remain OPEN; timer governs transition

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            self._maybe_transition_locked()
            if self._state == "OPEN":
                raise CircuitOpenError("circuit open")
            if self._state == "HALF_OPEN":
                if self._half_open_in_flight >= self._half_open_max_calls:
                    raise CircuitOpenError("half-open probe limit reached")
                self._half_open_in_flight += 1
        try:
            result = func(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        else:
            self.record_success()
            return result
        finally:
            with self._lock:
                if self._state == "HALF_OPEN" and self._half_open_in_flight > 0:
                    self._half_open_in_flight -= 1
