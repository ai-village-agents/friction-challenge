"""Robust HTTP client for Task 1 (Unreliable API).

Features
- Full-jitter exponential backoff with cap and attempt budget awareness
- Circuit breaker with half-open probe
- Deadlines: overall operation deadline and per-attempt timeout
- Integrity handling: JSON decode guard; optional schema validator
- Correlation ID propagation via X-Request-ID

Usage (example):
    from submissions.gpt_5.task1_client import RobustHttpClient
    from submissions.gpt_5.common.logging_util import new_correlation_id, set_correlation_id

    set_correlation_id(new_correlation_id())
    client = RobustHttpClient()
    data = client.request('GET', 'https://example.com/api/resource', validate=lambda d: 'id' in d)
    print(data)
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple, Union
import time
import json

import requests

# Local imports via direct relative path style for runtime; pytest imports modules by file path
from .common.backoff import backoff_delays
from .common.circuit_breaker import CircuitBreaker, CircuitOpenError
from .common.logging_util import get_logger, _CORR_ID  # type: ignore


class RobustHttpError(RuntimeError):
    def __init__(self, message: str, *, status: Optional[int] = None, body_snippet: Optional[str] = None) -> None:
        super().__init__(message)
        self.status = status
        self.body_snippet = body_snippet


class RobustHttpClient:
    """HTTP client with retries, jittered backoff, circuit breaker, and validation."""

    def __init__(
        self,
        *,
        base_delay_s: float = 0.2,
        cap_delay_s: float = 2.0,
        max_retries: int = 6,
        breaker: Optional[CircuitBreaker] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._base = base_delay_s
        self._cap = cap_delay_s
        self._max_retries = max_retries
        self._breaker = breaker or CircuitBreaker(failure_threshold=5, recovery_timeout_s=3.0, half_open_max_calls=1)
        self._session = session or requests.Session()
        self._log = get_logger("task1.robust_http")

    def request(
        self,
        method: str,
        url: str,
        *,
        json: Any | None = None,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_s: float | None = None,
        deadline_s: float | None = None,
        retry_for_status: Sequence[int] = (500, 502, 503, 504, 429),
        validate: Callable[[Any], bool] | None = None,
    ) -> Any:
        start = time.monotonic()
        corr = _CORR_ID.get()
        hdrs: Dict[str, str] = dict(headers or {})
        if corr and 'X-Request-ID' not in {k.title(): v for k, v in hdrs.items()}:
            hdrs['X-Request-ID'] = corr

        delays = backoff_delays(base=self._base, cap=self._cap, max_retries=self._max_retries)
        attempts = 1 + len(delays)

        last_status: Optional[int] = None
        last_body_snippet: Optional[str] = None
        for idx in range(attempts):
            now = time.monotonic()
            remaining = None
            if deadline_s is not None:
                remaining = max(0.0, deadline_s - (now - start))
                if remaining <= 0.0:
                    raise RobustHttpError("deadline exceeded before attempt")
            per_timeout = timeout_s if timeout_s is not None else (min(remaining, 10.0) if remaining is not None else 10.0)

            try:
                def do_request() -> requests.Response:
                    return self._session.request(method=method, url=url, json=json, params=params, headers=hdrs, timeout=per_timeout)

                resp = self._breaker.call(do_request)
            except CircuitOpenError as e:
                self._log.info(f"circuit_open on attempt {idx+1}")
                raise RobustHttpError("circuit open; refusing request") from e
            except requests.RequestException as e:
                self._log.info(f"network_error attempt={idx+1}: {e}")
                if idx < attempts - 1:
                    time.sleep(delays[idx])
                    continue
                raise RobustHttpError(f"network error after {attempts} attempts: {e}") from e

            last_status = int(resp.status_code)
            ctype = resp.headers.get('Content-Type', '')
            body_text: Optional[str] = None
            try:
                body_text = resp.text if resp.content is not None else ''
            except Exception:
                body_text = ''
            last_body_snippet = (body_text or '')[:256]

            # Retry on designated status codes
            if last_status in retry_for_status:
                self._log.info(f"retryable_status attempt={idx+1} status={last_status}")
                if idx < attempts - 1:
                    time.sleep(delays[idx])
                    continue
                raise RobustHttpError(f"retryable status but attempts exhausted: {last_status}", status=last_status, body_snippet=last_body_snippet)

            # Success (2xx) but ensure integrity if JSON expected
            parsed: Any
            if 'application/json' in ctype.lower():
                try:
                    parsed = resp.json()
                except json.JSONDecodeError as e:
                    self._log.info(f"json_decode_error attempt={idx+1}: {e}")
                    if idx < attempts - 1:
                        time.sleep(delays[idx])
                        continue
                    raise RobustHttpError("malformed JSON body after retries", status=last_status, body_snippet=last_body_snippet)
                if validate is not None:
                    try:
                        ok = bool(validate(parsed))
                    except Exception as e:
                        ok = False
                        self._log.info(f"validation_exception attempt={idx+1}: {e}")
                    if not ok:
                        if idx < attempts - 1:
                            time.sleep(delays[idx])
                            continue
                        raise RobustHttpError("validation failed after retries", status=last_status, body_snippet=last_body_snippet)
                return parsed

            # If not JSON, return raw text/content
            return body_text if body_text is not None else resp.content

        # Should not reach here
        raise RobustHttpError("exhausted attempts without explicit failure", status=last_status, body_snippet=last_body_snippet)
