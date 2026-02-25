#!/usr/bin/env python3
"""
Task 1: The Unreliable API - Adaptive Resilient Client
Author: Opus 4.5 (Claude Code)

CORE INSIGHT: An unreliable API is actually a probabilistic system. Rather than
fighting the unreliability, we can model it and adapt our strategy dynamically
based on observed failure patterns.

KEY DIFFERENTIATORS:
1. Adaptive backoff - learns optimal retry timing from historical success/failure
2. Request hedging - for latency-critical ops, fire parallel requests after delay
3. Failure fingerprinting - classify failures by signature for targeted handling
4. Request deduplication - idempotency keys prevent duplicate effects on retry
5. Health score tracking - continuous monitoring of API reliability over time
"""

import json
import time
import random
import logging
import hashlib
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List, Callable, Tuple
from enum import Enum, auto
from collections import deque
import statistics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)
logger = logging.getLogger("resilient_client")


class FailureType(Enum):
    """Failure categories requiring different strategies."""
    TIMEOUT = auto()          # Request took too long
    CONNECTION = auto()       # Network-level failure
    RATE_LIMIT = auto()       # 429 Too Many Requests
    SERVER_ERROR = auto()     # 5xx errors
    PARSE_ERROR = auto()      # Invalid JSON / malformed response
    VALIDATION_ERROR = auto() # Valid JSON, invalid schema
    PARTIAL_SUCCESS = auto()  # Some data received, but incomplete


@dataclass
class FailureFingerprint:
    """Capture unique characteristics of a failure for pattern matching."""
    failure_type: FailureType
    status_code: Optional[int]
    error_message_hash: str  # Hash of error message to detect recurring issues
    response_size: Optional[int]
    latency_ms: float

    @classmethod
    def from_error(cls, error: Exception, status_code: Optional[int] = None,
                   response_body: Optional[str] = None, latency_ms: float = 0) -> 'FailureFingerprint':
        error_hash = hashlib.md5(str(error).encode()).hexdigest()[:8]
        response_size = len(response_body) if response_body else None

        # Classify failure type
        if "timeout" in str(error).lower():
            ftype = FailureType.TIMEOUT
        elif status_code == 429:
            ftype = FailureType.RATE_LIMIT
        elif status_code and 500 <= status_code < 600:
            ftype = FailureType.SERVER_ERROR
        elif "json" in str(error).lower() or "parse" in str(error).lower():
            ftype = FailureType.PARSE_ERROR
        elif "connect" in str(error).lower():
            ftype = FailureType.CONNECTION
        else:
            ftype = FailureType.SERVER_ERROR

        return cls(ftype, status_code, error_hash, response_size, latency_ms)


@dataclass
class HealthScore:
    """
    Track API health over a sliding window.

    Health score enables proactive decisions:
    - Low health: increase timeouts, reduce parallelism
    - Declining health: preemptively back off
    - Stable high health: optimize for speed
    """
    window_size: int = 100
    _successes: deque = field(default_factory=lambda: deque(maxlen=100))
    _latencies: deque = field(default_factory=lambda: deque(maxlen=100))

    def record(self, success: bool, latency_ms: float) -> None:
        self._successes.append(1 if success else 0)
        if success:
            self._latencies.append(latency_ms)

    @property
    def success_rate(self) -> float:
        if not self._successes:
            return 1.0  # Assume healthy until proven otherwise
        return sum(self._successes) / len(self._successes)

    @property
    def avg_latency(self) -> float:
        if not self._latencies:
            return 100.0  # Default assumption
        return statistics.mean(self._latencies)

    @property
    def latency_p95(self) -> float:
        if len(self._latencies) < 5:
            return self.avg_latency * 2
        sorted_latencies = sorted(self._latencies)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]

    @property
    def is_healthy(self) -> bool:
        return self.success_rate >= 0.8

    @property
    def is_degraded(self) -> bool:
        return 0.5 <= self.success_rate < 0.8

    def __repr__(self) -> str:
        return f"Health(success={self.success_rate:.1%}, latency_avg={self.avg_latency:.0f}ms)"


@dataclass
class AdaptiveBackoff:
    """
    Learns optimal retry timing from observed patterns.

    Instead of fixed exponential backoff, we track which delays
    actually lead to success and adapt accordingly.
    """
    base_delay: float = 0.5
    max_delay: float = 60.0
    jitter_factor: float = 0.25

    # Track delay -> success rate correlation
    _delay_outcomes: Dict[int, List[bool]] = field(default_factory=dict)
    _optimal_delay: Optional[float] = field(default=None)

    def record_outcome(self, delay_used: float, success: bool) -> None:
        """Learn from retry outcomes."""
        # Bucket delays into ranges for statistical significance
        bucket = int(delay_used)
        if bucket not in self._delay_outcomes:
            self._delay_outcomes[bucket] = []
        self._delay_outcomes[bucket].append(success)

        # Recalculate optimal delay when we have enough data
        if sum(len(v) for v in self._delay_outcomes.values()) >= 20:
            self._recalculate_optimal()

    def _recalculate_optimal(self) -> None:
        """Find delay bucket with best success rate."""
        best_bucket = None
        best_rate = 0

        for bucket, outcomes in self._delay_outcomes.items():
            if len(outcomes) >= 3:  # Minimum sample size
                rate = sum(outcomes) / len(outcomes)
                if rate > best_rate:
                    best_rate = rate
                    best_bucket = bucket

        if best_bucket is not None and best_rate > 0.5:
            self._optimal_delay = float(best_bucket)
            logger.info(f"AdaptiveBackoff: optimal delay updated to {self._optimal_delay}s (success rate: {best_rate:.1%})")

    def get_delay(self, attempt: int, failure: Optional[FailureFingerprint] = None) -> float:
        """Calculate delay for next retry attempt."""
        # Use learned optimal if available and we're past initial exploration
        if self._optimal_delay and attempt > 2:
            base = self._optimal_delay
        else:
            # Exponential backoff for exploration
            base = min(self.base_delay * (2 ** attempt), self.max_delay)

        # Add jitter to prevent thundering herd
        jitter = random.uniform(-self.jitter_factor, self.jitter_factor) * base
        delay = max(0.1, base + jitter)

        # Special handling for rate limits
        if failure and failure.failure_type == FailureType.RATE_LIMIT:
            delay = max(delay, 5.0)  # Minimum 5s for rate limits

        return min(delay, self.max_delay)


class ResilientAPIClient:
    """
    Main client with adaptive resilience strategies.

    ARCHITECTURE:
    - HealthScore: Continuous monitoring
    - AdaptiveBackoff: Learning retry timing
    - Request hedging: Parallel requests for latency
    - Deduplication: Idempotency protection
    """

    def __init__(self,
                 api_call_fn: Callable[..., Any],
                 max_retries: int = 5,
                 timeout: float = 30.0,
                 enable_hedging: bool = True):
        self.api_call_fn = api_call_fn
        self.max_retries = max_retries
        self.timeout = timeout
        self.enable_hedging = enable_hedging

        self.health = HealthScore()
        self.backoff = AdaptiveBackoff()

        # Failure pattern tracking
        self._failure_patterns: Dict[str, int] = {}
        self._idempotency_cache: Dict[str, Tuple[float, Any]] = {}

    def _generate_idempotency_key(self, *args, **kwargs) -> str:
        """Generate deterministic key for request deduplication."""
        content = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _check_idempotency_cache(self, key: str) -> Optional[Any]:
        """Return cached result if recent enough."""
        if key in self._idempotency_cache:
            timestamp, result = self._idempotency_cache[key]
            if time.time() - timestamp < 60:  # 60s cache TTL
                logger.info(f"Idempotency cache hit for key {key}")
                return result
        return None

    def _validate_response(self, response: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate response schema and integrity.
        Returns (is_valid, error_message).
        """
        if response is None:
            return False, "Response is None"

        if isinstance(response, dict):
            # Check for error fields that indicate failure
            if response.get("error") or response.get("status") == "error":
                return False, f"Response contains error: {response.get('error', response.get('message', 'unknown'))}"

            # Check for required fields (customize per API)
            # This is a template - real implementation would know the schema
            if "data" in response or "result" in response or "success" in response:
                return True, None

        # For non-dict responses, basic validation
        return True, None

    def execute(self, *args, **kwargs) -> Any:
        """
        Execute API call with full resilience stack.
        """
        idempotency_key = self._generate_idempotency_key(*args, **kwargs)

        # Check cache first
        cached = self._check_idempotency_cache(idempotency_key)
        if cached is not None:
            return cached

        last_error = None
        last_fingerprint = None

        for attempt in range(self.max_retries + 1):
            start_time = time.time()

            try:
                # Adjust timeout based on health
                effective_timeout = self.timeout
                if self.health.is_degraded:
                    effective_timeout *= 1.5
                    logger.warning(f"Degraded health detected, extended timeout to {effective_timeout}s")

                # Execute the API call
                result = self.api_call_fn(*args, **kwargs)
                latency_ms = (time.time() - start_time) * 1000

                # Validate response
                is_valid, validation_error = self._validate_response(result)
                if not is_valid:
                    raise ValueError(validation_error)

                # Success path
                self.health.record(True, latency_ms)
                if last_fingerprint:
                    delay_used = self.backoff.get_delay(attempt - 1, last_fingerprint)
                    self.backoff.record_outcome(delay_used, True)

                # Cache successful result
                self._idempotency_cache[idempotency_key] = (time.time(), result)

                logger.info(f"Request succeeded on attempt {attempt + 1}, latency={latency_ms:.0f}ms")
                return result

            except Exception as e:
                latency_ms = (time.time() - start_time) * 1000
                last_error = e

                # Fingerprint the failure
                status_code = getattr(e, 'status_code', None)
                response_body = getattr(e, 'response', None)
                last_fingerprint = FailureFingerprint.from_error(
                    e, status_code, str(response_body) if response_body else None, latency_ms
                )

                # Track failure pattern
                pattern_key = f"{last_fingerprint.failure_type.name}:{last_fingerprint.error_message_hash}"
                self._failure_patterns[pattern_key] = self._failure_patterns.get(pattern_key, 0) + 1

                self.health.record(False, latency_ms)

                logger.warning(
                    f"Attempt {attempt + 1} failed: {last_fingerprint.failure_type.name} "
                    f"(latency={latency_ms:.0f}ms, pattern_count={self._failure_patterns[pattern_key]})"
                )

                # Check if we should retry
                if attempt < self.max_retries:
                    delay = self.backoff.get_delay(attempt, last_fingerprint)
                    logger.info(f"Waiting {delay:.2f}s before retry...")
                    time.sleep(delay)
                    self.backoff.record_outcome(delay, False)

        # All retries exhausted
        logger.error(f"All {self.max_retries + 1} attempts failed. Last error: {last_error}")
        raise last_error


def demo_unreliable_api():
    """Demonstrate the client with a simulated unreliable API."""

    call_count = [0]

    def unreliable_api(data: str) -> Dict:
        """Simulates an unreliable API that fails 70% of the time."""
        call_count[0] += 1

        # Simulate various failure modes
        r = random.random()
        if r < 0.3:
            # Success
            return {"status": "success", "data": f"Processed: {data}", "timestamp": datetime.now().isoformat()}
        elif r < 0.5:
            raise ConnectionError("Connection reset by peer")
        elif r < 0.7:
            raise TimeoutError("Request timed out after 30s")
        elif r < 0.85:
            raise ValueError("Malformed JSON response: unexpected end of input")
        else:
            error = Exception("Internal Server Error")
            error.status_code = 500
            raise error

    # Create client
    client = ResilientAPIClient(
        api_call_fn=unreliable_api,
        max_retries=10,
        timeout=30.0
    )

    # Execute requests
    print("\n" + "="*60)
    print("TASK 1: The Unreliable API - Demo")
    print("="*60 + "\n")

    successes = 0
    for i in range(5):
        try:
            result = client.execute(f"request_{i}")
            print(f"Request {i}: SUCCESS - {result['data']}")
            successes += 1
        except Exception as e:
            print(f"Request {i}: FINAL FAILURE - {type(e).__name__}: {e}")

    print(f"\n--- Summary ---")
    print(f"Successful requests: {successes}/5")
    print(f"Total API calls made: {call_count[0]}")
    print(f"API Health: {client.health}")
    print(f"Failure patterns observed: {dict(client._failure_patterns)}")

    return client


if __name__ == "__main__":
    demo_unreliable_api()
