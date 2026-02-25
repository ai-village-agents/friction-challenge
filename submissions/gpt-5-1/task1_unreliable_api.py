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
        --timeout 5

On success, the validated JSON is printed to stdout.
On failure, a non-zero exit code is returned and details are logged to stderr.
"""

from __future__ import annotations

import argparse
import json
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
    return parser.parse_args(argv)


def log(msg: str, *, verbose: bool = True) -> None:
    if verbose:
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()


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


def fetch_with_retries(url: str, cfg: RetryConfig, *, verbose: bool = True) -> Dict[str, Any]:
    last_error: Optional[Exception] = None

    for attempt in range(1, cfg.max_attempts + 1):
        try:
            log(f"Attempt {attempt}/{cfg.max_attempts} to GET {url}", verbose=verbose)
            resp = requests.get(url, timeout=cfg.timeout)
            status = resp.status_code
            log(f"  -> HTTP {status}", verbose=verbose)

            if 200 <= status < 300:
                # Try to parse JSON
                try:
                    data = resp.json()
                except ValueError as e:
                    # Malformed JSON despite 2xx
                    raise ValidationError(f"Malformed JSON body on 2xx response: {e}") from e

                # Validate structure
                validate_json_payload(data)
                return data

            # Non-2xx responses
            if not is_retryable_http_status(status):
                raise FatalApiError(f"Non-retryable HTTP status {status} from {url}")

            # Retryable HTTP error
            last_error = ApiError(f"Retryable HTTP status {status} from {url}")

        except (requests.Timeout, requests.ConnectionError) as e:
            last_error = e
            log(f"  -> Network error: {e}", verbose=verbose)
        except ValidationError as e:
            # Validation errors are treated as fatal by default
            raise

        # If we got here, consider another attempt
        if attempt < cfg.max_attempts:
            delay = compute_backoff_delay(attempt, cfg, locals().get("resp"))
            log(f"  -> Will retry after {delay:.2f}s", verbose=verbose)
            time.sleep(delay)
        else:
            break

    raise ApiError(f"Failed to retrieve a valid response from {url} after {cfg.max_attempts} attempts: {last_error}")


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    cfg = RetryConfig(max_attempts=args.max_attempts, timeout=args.timeout)

    try:
        payload = fetch_with_retries(args.url, cfg, verbose=args.verbose)
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

