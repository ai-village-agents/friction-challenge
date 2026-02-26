#!/usr/bin/env python3
"""
Task 1: The Unreliable API - Robust Client Implementation
Author: Claude Opus 4.5

PHILOSOPHY: The key insight is that "unreliable" is actually a spectrum. Different
failure modes require different responses. A 429 with Retry-After is fundamentally
different from a 500 (server error) or truncated JSON (network/parsing issue).

This implementation uses a LAYERED RESILIENCE approach:
1. Schema validation - catch malformed "success" responses before processing
2. Exponential backoff with jitter - avoid thundering herd, respect server load
3. Circuit breaker - fail fast when server is overwhelmed, give it time to recover
4. Adaptive delay - honor Retry-After headers when provided
5. Structured logging - full observability for diagnosis
"""

import json
import time
import random
import logging
import hashlib
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List, Callable
from enum import Enum, auto

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class FailureMode(Enum):
    """Categorize failures by required response strategy."""
    TRANSIENT = auto()      # Retry immediately with backoff (500, network errors)
    RATE_LIMITED = auto()   # Honor Retry-After header (429)
    PARSE_ERROR = auto()    # Truncated/malformed JSON - retry with fresh connection
    SCHEMA_ERROR = auto()   # Valid JSON but missing required fields
    CIRCUIT_OPEN = auto()   # Circuit breaker triggered - wait before any retries


@dataclass
class CircuitBreaker:
    """
    Circuit breaker pattern prevents cascading failures.
    
    WHY: When a service is overwhelmed, hammering it with retries makes things worse.
    The circuit breaker "opens" after consecutive failures, forcing a cool-down period.
    """
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 3
    
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: Optional[float] = field(default=None, init=False)
    _state: str = field(default="CLOSED", init=False)
    _half_open_calls: int = field(default=0, init=False)
    
    def record_success(self) -> None:
        """Reset failure count on successful call."""
        self._failure_count = 0
        self._half_open_calls = 0
        if self._state != "CLOSED":
            logger.info(f"Circuit breaker: {self._state} -> CLOSED (success)")
            self._state = "CLOSED"
    
    def record_failure(self) -> None:
        """Track failure and potentially open circuit."""
        self._failure_count += 1
        self._last_failure_time = time.time()
        
        if self._state == "HALF_OPEN":
            logger.warning("Circuit breaker: HALF_OPEN -> OPEN (failure during recovery)")
            self._state = "OPEN"
        elif self._failure_count >= self.failure_threshold:
            logger.warning(f"Circuit breaker: CLOSED -> OPEN (threshold reached: {self._failure_count})")
            self._state = "OPEN"
    
    def can_execute(self) -> bool:
        """Check if calls are allowed through the circuit."""
        if self._state == "CLOSED":
            return True
        
        if self._state == "OPEN":
            elapsed = time.time() - (self._last_failure_time or 0)
            if elapsed >= self.recovery_timeout:
                logger.info(f"Circuit breaker: OPEN -> HALF_OPEN (recovery timeout elapsed)")
                self._state = "HALF_OPEN"
                self._half_open_calls = 0
                return True
            return False
        
        # HALF_OPEN state - allow limited calls to test recovery
        if self._half_open_calls < self.half_open_max_calls:
            self._half_open_calls += 1
            return True
        return False
    
    @property
    def state(self) -> str:
        return self._state


@dataclass
class RetryConfig:
    """Configuration for retry behavior with exponential backoff."""
    max_retries: int = 10
    base_delay: float = 0.5
    max_delay: float = 60.0
    jitter_factor: float = 0.3  # Adds randomness to prevent thundering herd
    
    def calculate_delay(self, attempt: int, retry_after: Optional[float] = None) -> float:
        """
        Calculate delay before next retry.
        
        WHY jitter: If 100 clients all retry at exactly 2^n seconds, they'll
        all hit the server at the same instant. Jitter spreads out the load.
        """
        if retry_after is not None:
            # Server explicitly told us when to retry - honor it
            return retry_after + random.uniform(0, 1)  # Small jitter even here
        
        # Exponential backoff: delay = base * 2^attempt
        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        
        # Add jitter: ±jitter_factor percent
        jitter_range = delay * self.jitter_factor
        delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0.1, delay)  # Minimum 100ms delay


@dataclass
class ApiResponse:
    """Structured response with validation state."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    failure_mode: Optional[FailureMode] = None
    raw_response: Optional[str] = None
    http_status: Optional[int] = None
    retry_after: Optional[float] = None


class SchemaValidator:
    """
    Validates API responses against expected schema.
    
    WHY: A 200 OK with missing fields is worse than a clear 500 error.
    We must validate the CONTENT, not just the HTTP status code.
    """
    
    REQUIRED_FIELDS = ['id', 'status', 'data', 'timestamp']
    
    @classmethod
    def validate(cls, response_data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate response against expected schema.
        Returns (is_valid, error_message).
        """
        missing_fields = [f for f in cls.REQUIRED_FIELDS if f not in response_data]
        if missing_fields:
            return False, f"Missing required fields: {missing_fields}"
        
        # Type validation
        if not isinstance(response_data.get('id'), (int, str)):
            return False, "Field 'id' must be int or string"
        
        if response_data.get('status') not in ['success', 'complete', 'ok']:
            return False, f"Unexpected status value: {response_data.get('status')}"
        
        if not isinstance(response_data.get('data'), dict):
            return False, "Field 'data' must be an object"
        
        return True, None


# =============================================================================
# SIMULATED API - Models realistic failure modes
# =============================================================================

class UnreliableApiSimulator:
    """
    Simulates an unreliable API with various failure modes.
    
    This models real-world scenarios:
    - Overloaded servers returning 500s
    - Rate limiting with Retry-After headers
    - Network issues causing truncated responses
    - Bugs causing malformed but "successful" responses
    """
    
    def __init__(self, failure_rate: float = 0.7, seed: Optional[int] = None):
        self.failure_rate = failure_rate
        self.call_count = 0
        self.rng = random.Random(seed)
        
    def call(self) -> tuple[int, str, Dict[str, str]]:
        """
        Simulate API call. Returns (status_code, body, headers).
        """
        self.call_count += 1
        
        if self.rng.random() > self.failure_rate:
            # Success case
            return 200, json.dumps({
                "id": self.call_count,
                "status": "success",
                "data": {"value": self.rng.randint(1, 100), "computed": True},
                "timestamp": datetime.now().isoformat()
            }), {}
        
        # Choose a failure mode
        failure_type = self.rng.choice([
            'http_500', 'http_503', 'http_429', 
            'truncated_json', 'malformed_success'
        ])
        
        if failure_type == 'http_500':
            return 500, '{"error": "Internal Server Error"}', {}
        
        elif failure_type == 'http_503':
            return 503, '{"error": "Service Unavailable"}', {}
        
        elif failure_type == 'http_429':
            retry_after = self.rng.randint(1, 5)
            return 429, '{"error": "Too Many Requests"}', {'Retry-After': str(retry_after)}
        
        elif failure_type == 'truncated_json':
            # Simulates network interruption mid-response
            return 200, '{"id": 1, "status": "succ', {}
        
        else:  # malformed_success
            # Missing required fields - looks successful but isn't
            return 200, json.dumps({
                "id": self.call_count,
                "partial": True
                # Missing: status, data, timestamp
            }), {}


# =============================================================================
# ROBUST API CLIENT - Implements all resilience patterns
# =============================================================================

class RobustApiClient:
    """
    Production-grade API client with comprehensive resilience patterns.
    
    Design principles:
    1. Never trust HTTP status alone - validate response content
    2. Categorize failures and respond appropriately to each
    3. Fail fast when overwhelmed (circuit breaker)
    4. Full observability through structured logging
    """
    
    def __init__(
        self, 
        api: UnreliableApiSimulator,
        retry_config: Optional[RetryConfig] = None,
        circuit_breaker: Optional[CircuitBreaker] = None
    ):
        self.api = api
        self.retry_config = retry_config or RetryConfig()
        self.circuit = circuit_breaker or CircuitBreaker()
        self.attempt_history: List[Dict[str, Any]] = []
    
    def _parse_response(self, status: int, body: str, headers: Dict) -> ApiResponse:
        """Parse and categorize API response."""
        
        # Handle HTTP errors
        if status == 429:
            retry_after = float(headers.get('Retry-After', 5))
            return ApiResponse(
                success=False,
                failure_mode=FailureMode.RATE_LIMITED,
                http_status=status,
                retry_after=retry_after,
                error_message="Rate limited",
                raw_response=body
            )
        
        if status >= 500:
            return ApiResponse(
                success=False,
                failure_mode=FailureMode.TRANSIENT,
                http_status=status,
                error_message=f"Server error: {status}",
                raw_response=body
            )
        
        # Try to parse JSON
        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            return ApiResponse(
                success=False,
                failure_mode=FailureMode.PARSE_ERROR,
                http_status=status,
                error_message=f"JSON parse error: {e}",
                raw_response=body
            )
        
        # Validate schema
        is_valid, error_msg = SchemaValidator.validate(data)
        if not is_valid:
            return ApiResponse(
                success=False,
                failure_mode=FailureMode.SCHEMA_ERROR,
                http_status=status,
                error_message=error_msg,
                data=data,
                raw_response=body
            )
        
        # Full success!
        return ApiResponse(
            success=True,
            data=data,
            http_status=status,
            raw_response=body
        )
    
    def fetch(self) -> ApiResponse:
        """
        Fetch data from API with full resilience patterns.
        
        This is the main entry point - it orchestrates:
        1. Circuit breaker check
        2. API call with retry logic
        3. Response validation
        4. Adaptive backoff
        """
        
        for attempt in range(self.retry_config.max_retries + 1):
            attempt_record = {
                "attempt": attempt + 1,
                "timestamp": datetime.now().isoformat(),
                "circuit_state": self.circuit.state
            }
            
            # Circuit breaker check
            if not self.circuit.can_execute():
                wait_time = self.circuit.recovery_timeout
                logger.warning(f"Circuit OPEN - waiting {wait_time:.1f}s before retry")
                attempt_record["action"] = "circuit_wait"
                attempt_record["wait_time"] = wait_time
                self.attempt_history.append(attempt_record)
                time.sleep(wait_time)
                continue
            
            # Make the API call
            try:
                status, body, headers = self.api.call()
                attempt_record["http_status"] = status
                
                response = self._parse_response(status, body, headers)
                attempt_record["failure_mode"] = response.failure_mode.name if response.failure_mode else None
                
                if response.success:
                    self.circuit.record_success()
                    attempt_record["result"] = "SUCCESS"
                    self.attempt_history.append(attempt_record)
                    logger.info(f"✓ Attempt {attempt + 1}: SUCCESS - got valid response")
                    return response
                
                # Handle failure
                self.circuit.record_failure()
                
                if attempt == self.retry_config.max_retries:
                    attempt_record["result"] = "FINAL_FAILURE"
                    self.attempt_history.append(attempt_record)
                    logger.error(f"✗ Attempt {attempt + 1}: FAILED (max retries reached)")
                    return response
                
                # Calculate backoff
                delay = self.retry_config.calculate_delay(
                    attempt, 
                    response.retry_after
                )
                attempt_record["backoff_delay"] = delay
                attempt_record["result"] = "RETRY"
                self.attempt_history.append(attempt_record)
                
                logger.warning(
                    f"✗ Attempt {attempt + 1}: {response.failure_mode.name} - "
                    f"waiting {delay:.2f}s before retry"
                )
                time.sleep(delay)
                
            except Exception as e:
                self.circuit.record_failure()
                attempt_record["result"] = "EXCEPTION"
                attempt_record["error"] = str(e)
                self.attempt_history.append(attempt_record)
                logger.error(f"✗ Attempt {attempt + 1}: Exception - {e}")
                
                if attempt == self.retry_config.max_retries:
                    return ApiResponse(
                        success=False,
                        failure_mode=FailureMode.TRANSIENT,
                        error_message=str(e)
                    )
                
                delay = self.retry_config.calculate_delay(attempt)
                time.sleep(delay)
        
        return ApiResponse(
            success=False,
            failure_mode=FailureMode.TRANSIENT,
            error_message="Max retries exceeded"
        )
    
    def get_diagnostics(self) -> Dict[str, Any]:
        """Return full diagnostic information about all attempts."""
        return {
            "total_attempts": len(self.attempt_history),
            "circuit_state": self.circuit.state,
            "attempt_history": self.attempt_history,
            "failure_mode_counts": self._count_failure_modes()
        }
    
    def _count_failure_modes(self) -> Dict[str, int]:
        """Count occurrences of each failure mode."""
        counts: Dict[str, int] = {}
        for record in self.attempt_history:
            mode = record.get("failure_mode", "SUCCESS" if record.get("result") == "SUCCESS" else "UNKNOWN")
            counts[mode] = counts.get(mode, 0) + 1
        return counts


# =============================================================================
# MAIN - Demonstration
# =============================================================================

def main():
    """Demonstrate the robust API client handling unreliable responses."""
    
    print("=" * 70)
    print("TASK 1: THE UNRELIABLE API")
    print("Demonstrating resilient API client with circuit breaker, exponential")
    print("backoff, and schema validation")
    print("=" * 70)
    print()
    
    # Create simulator with 70% failure rate
    api = UnreliableApiSimulator(failure_rate=0.7, seed=42)
    
    # Create client with custom config
    client = RobustApiClient(
        api=api,
        retry_config=RetryConfig(
            max_retries=10,
            base_delay=0.1,  # Fast for demo
            max_delay=5.0,
            jitter_factor=0.2
        ),
        circuit_breaker=CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=2.0  # Short for demo
        )
    )
    
    # Attempt to fetch data
    print("Starting API fetch with resilient client...\n")
    result = client.fetch()
    
    print("\n" + "=" * 70)
    print("RESULT")
    print("=" * 70)
    
    if result.success:
        print(f"✓ SUCCESS after {len(client.attempt_history)} attempts")
        print(f"  Data: {json.dumps(result.data, indent=2)}")
    else:
        print(f"✗ FAILURE after {len(client.attempt_history)} attempts")
        print(f"  Error: {result.error_message}")
        print(f"  Failure Mode: {result.failure_mode}")
    
    print("\n" + "=" * 70)
    print("DIAGNOSTICS")
    print("=" * 70)
    
    diagnostics = client.get_diagnostics()
    print(f"Total attempts: {diagnostics['total_attempts']}")
    print(f"Circuit state: {diagnostics['circuit_state']}")
    print(f"Failure mode distribution: {diagnostics['failure_mode_counts']}")
    
    print("\nAttempt timeline:")
    for record in diagnostics['attempt_history']:
        mode = record.get('failure_mode', '-')
        result_str = record.get('result', '-')
        delay = record.get('backoff_delay', 0)
        print(f"  {record['attempt']:2d}. {result_str:15s} | mode: {str(mode):20s} | delay: {delay:.2f}s")


if __name__ == "__main__":
    main()
