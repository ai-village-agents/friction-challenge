#!/usr/bin/env python3
"""Robust client for an unreliable JSON API.

Goals for this task:

1. Survive intermittent failures:
   - HTTP 5xx errors
   - timeouts / connection resets
   - malformed or partial JSON bodies
2. Avoid hammering the service:
   - bounded exponential backoff with jitter
   - respect Retry-After when present
3. Validate that *successful* responses are structurally sound.
4. Emit structured diagnostics so humans can understand what is happening.

Because the original challenge description intentionally leaves the API
unspecified, this script is deliberately **configurable**:

- The base URL and endpoint are passed as CLI arguments.
- Optional JSON schema-like expectations can be declared in code and adapted
  once the real API shape is known.

Example usage:

    python task1_unreliable_api.py \
        --url "https://example.com/api/resource" \
        --max-attempts 8 \
        --timeout 5 \
        --verbose \
        --diagnostics-log api_attempts.jsonl

On success, the validated JSON is printed to stdout.
On failure, a non-zero exit code is returned and details are logged to stderr.
If a diagnostics log path is provided, one JSON object per attempt is also
written to that file (JSON Lines format).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from requests import Response


@dataclass
class RetryConfig:
    max_attempts: int = 8
    base_delay: float = 0.5  # seconds
    max_delay: float = 10.0  # seconds
    timeout: float = 5.0  # per-request timeout in seconds


RETRYABLE_STATUS_CODES = {500, 502, 503, 504}
FATAL_STATUS_CODES = {400, 401, 403, 404, 422}


class ApiError(Exception):
    """Base error for API retrieval problems."""


class FatalApiError(ApiError):
    """Non-retryable error (e.g., 4xx indicating a bad request)."""


class ValidationError(ApiError):
    """Response JSON did not meet expectations."""


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Robust client for an unreliable JSON API")
    parser.add_argument("--url", required=True, help="Full URL of the JSON API endpoint")
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=RetryConfig.max_attempts,
        help="Maximum number of attempts before giving up (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=RetryConfig.timeout,
        help="Per-request timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed diagnostics to stderr",
    )
    parser.add_argument(
        "--diagnostics-log",
        help=(
            "Optional path to a JSONL file where each attempt will be logged as "
            "a structured JSON object. Existing files are appended to."
        ),
    )
    return parser.parse_args(argv)


def log(msg: str, *, verbose: bool = True) -> None:
    if verbose:
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()


def append_diagnostic(path: Optional[str], event: Dict[str, Any]) -> None:
    """Append a single JSON-serializable event to the diagnostics log, if set."""

    if not path:
        return

    # Best-effort directory creation
    directory = os.path.dirname(os.path.abspath(path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

    # Attach a timestamp if the caller didn't provide one
    event.setdefault("ts", time.time())

    with open(path, "a", encoding="utf-8") as f:
        json.dump(event, f, ensure_ascii=False)
        f.write("\n")


def compute_backoff_delay(attempt: int, cfg: RetryConfig, response: Optional[Response]) -> float:
    """Compute delay before the next attempt.

    - Uses exponential backoff with jitter.
    - If Retry-After is present and sane, honors it (capped by cfg.max_delay).
    """

    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                delay = float(retry_after)
                if 0 <= delay <= cfg.max_delay * 2:
                    return min(delay, cfg.max_delay)
            except ValueError:
                # Non-numeric Retry-After; ignore and fall back to exponential
                pass

    # Exponential backoff with jitter
    exp = min(attempt, 10)
    base = min(cfg.base_delay * (2 ** (exp - 1)), cfg.max_delay)
    jitter = random.uniform(0, base / 2)
    return base + jitter


def is_retryable_http_status(status_code: int) -> bool:
    if status_code in RETRYABLE_STATUS_CODES:
        return True
    if status_code in FATAL_STATUS_CODES:
        return False
    # Other codes (e.g., 408, 429) are treated as retryable by default
    return True


def validate_json_payload(payload: Any) -> None:
    """Validate that the JSON response is structurally sound.

    This is intentionally conservative and documented, so it can be adapted
    once the true schema is known.
    """

    if not isinstance(payload, dict):
        raise ValidationError(f"Expected top-level object, got {type(payload)!r}")

    # Example minimal contract: status + data keys.
    if "status" not in payload:
        raise ValidationError("Missing required key: 'status'")
    if "data" not in payload:
        raise ValidationError("Missing required key: 'data'")


def fetch_with_retries(
    url: str,
    cfg: RetryConfig,
    *,
    verbose: bool = True,
    diagnostics_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch JSON from an unreliable endpoint with retries and diagnostics.

    On each attempt, a structured diagnostics event is optionally written to
    ``diagnostics_path`` if provided.
    """

    last_error: Optional[Exception] = None

    for attempt in range(1, cfg.max_attempts + 1):
        append_diagnostic(
            diagnostics_path,
            {
                "event": "attempt_start",
                "attempt": attempt,
                "max_attempts": cfg.max_attempts,
                "url": url,
            },
        )

        resp: Optional[Response] = None
        try:
            log(f"Attempt {attempt}/{cfg.max_attempts} to GET {url}", verbose=verbose)
            resp = requests.get(url, timeout=cfg.timeout)
            status = resp.status_code
            log(f"  -> HTTP {status}", verbose=verbose)

            append_diagnostic(
                diagnostics_path,
                {
                    "event": "http_response",
                    "attempt": attempt,
                    "status": status,
                },
            )

            if 200 <= status < 300:
                # Try to parse JSON
                try:
                    data = resp.json()
                except ValueError as e:
                    # Malformed JSON despite 2xx
                    append_diagnostic(
                        diagnostics_path,
                        {
                            "event": "json_parse_error",
                            "attempt": attempt,
                            "error": str(e),
                        },
                    )
                    raise ValidationError(f"Malformed JSON body on 2xx response: {e}") from e

                # Validate structure
                try:
                    validate_json_payload(data)
                except ValidationError as e:
                    append_diagnostic(
                        diagnostics_path,
                        {
                            "event": "schema_validation_error",
                            "attempt": attempt,
                            "error": str(e),
                        },
                    )
                    raise

                append_diagnostic(
                    diagnostics_path,
                    {
                        "event": "success",
                        "attempt": attempt,
                    },
                )
                return data

            # Non-2xx responses
            if not is_retryable_http_status(status):
                err = FatalApiError(f"Non-retryable HTTP status {status} from {url}")
                append_diagnostic(
                    diagnostics_path,
                    {
                        "event": "fatal_http_status",
                        "attempt": attempt,
                        "status": status,
                        "error": str(err),
                    },
                )
                raise err

            # Retryable HTTP error
            last_error = ApiError(f"Retryable HTTP status {status} from {url}")
            append_diagnostic(
                diagnostics_path,
                {
                    "event": "retryable_http_status",
                    "attempt": attempt,
                    "status": status,
                    "error": str(last_error),
                },
            )

        except (requests.Timeout, requests.ConnectionError) as e:
            last_error = e
            log(f"  -> Network error: {e}", verbose=verbose)
            append_diagnostic(
                diagnostics_path,
                {
                    "event": "network_error",
                    "attempt": attempt,
                    "error_type": type(e).__name__,
                    "error": str(e),
                },
            )
        except ValidationError as e:
            # Validation errors are treated as fatal by default
            append_diagnostic(
                diagnostics_path,
                {
                    "event": "validation_error",
                    "attempt": attempt,
                    "error": str(e),
                },
            )
            raise

        # If we got here, consider another attempt
        if attempt < cfg.max_attempts:
            delay = compute_backoff_delay(attempt, cfg, resp)
            log(f"  -> Will retry after {delay:.2f}s", verbose=verbose)
            append_diagnostic(
                diagnostics_path,
                {
                    "event": "backoff_delay",
                    "attempt": attempt,
                    "delay_seconds": delay,
                },
            )
            time.sleep(delay)
        else:
            break

    append_diagnostic(
        diagnostics_path,
        {
            "event": "giving_up",
            "attempts": cfg.max_attempts,
            "last_error": str(last_error) if last_error else None,
        },
    )
    raise ApiError(
        f"Failed to retrieve a valid response from {url} after "
        f"{cfg.max_attempts} attempts: {last_error}"
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    cfg = RetryConfig(max_attempts=args.max_attempts, timeout=args.timeout)

    try:
        payload = fetch_with_retries(
            args.url,
            cfg,
            verbose=args.verbose,
            diagnostics_path=args.diagnostics_log,
        )
    except FatalApiError as e:
        log(f"FATAL: {e}", verbose=True)
        return 2
    except ValidationError as e:
        log(f"VALIDATION ERROR: {e}", verbose=True)
        return 3
    except ApiError as e:
        log(f"ERROR: {e}", verbose=True)
        return 1

    # On success, print normalized JSON to stdout
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
