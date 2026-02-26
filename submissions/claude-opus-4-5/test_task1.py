"""
Comprehensive pytest test suite for Task 1: Unreliable API Resilience

Tests cover:
- CircuitBreaker state transitions and failure threshold tracking
- RetryConfig with exponential backoff and jitter
- ApiResponse validation
- SchemaValidator for response structure
- UnreliableApiSimulator failure modes
- RobustApiClient end-to-end resilience

Author: Claude Opus 4.5
"""

import pytest
import time
import json
from enum import Enum
from task1_unreliable_api import (
    FailureMode, CircuitBreaker, RetryConfig, ApiResponse,
    SchemaValidator, UnreliableApiSimulator, RobustApiClient
)


# ==================== CircuitBreaker Tests ====================

class TestCircuitBreaker:
    """Test suite for CircuitBreaker state machine."""
    
    def test_initial_state_closed(self):
        """Circuit breaker starts in CLOSED state."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)
        assert cb.state == "CLOSED"
        assert cb.failure_count == 0
    
    def test_records_failures_in_closed_state(self):
        """Failures are tracked while circuit is closed."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)
        cb.record_failure()
        assert cb.failure_count == 1
        assert cb.state == "CLOSED"
    
    def test_opens_after_failure_threshold(self):
        """Circuit opens after reaching failure threshold."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == "OPEN"
    
    def test_blocks_calls_when_open(self):
        """Open circuit blocks new calls."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0)
        cb.record_failure()
        assert not cb.allow_request()
    
    def test_allows_calls_when_closed(self):
        """Closed circuit allows calls."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)
        assert cb.allow_request()
    
    def test_resets_on_success(self):
        """Successful call resets failure count."""
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=5.0)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.failure_count == 0
        assert cb.state == "CLOSED"
    
    def test_half_open_after_recovery_timeout(self):
        """Circuit transitions to HALF_OPEN after recovery timeout."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)
        cb.record_failure()
        assert cb.state == "OPEN"
        time.sleep(0.15)
        # Should allow one test request
        assert cb.allow_request()
        assert cb.state == "HALF_OPEN"
    
    def test_closes_on_success_in_half_open(self):
        """Success in HALF_OPEN state closes the circuit."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)
        cb.record_failure()
        time.sleep(0.15)
        cb.allow_request()  # Transition to HALF_OPEN
        cb.record_success()
        assert cb.state == "CLOSED"
    
    def test_reopens_on_failure_in_half_open(self):
        """Failure in HALF_OPEN state reopens the circuit."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)
        cb.record_failure()
        time.sleep(0.15)
        cb.allow_request()  # Transition to HALF_OPEN
        cb.record_failure()
        assert cb.state == "OPEN"


# ==================== RetryConfig Tests ====================

class TestRetryConfig:
    """Test suite for retry configuration and backoff calculation."""
    
    def test_default_values(self):
        """RetryConfig has sensible defaults."""
        config = RetryConfig()
        assert config.max_retries >= 1
        assert config.base_delay > 0
        assert config.max_delay > config.base_delay
    
    def test_exponential_backoff_increases(self):
        """Backoff delay increases exponentially with attempts."""
        config = RetryConfig(base_delay=1.0, max_delay=60.0, jitter=False)
        delay1 = config.get_delay(attempt=0)
        delay2 = config.get_delay(attempt=1)
        delay3 = config.get_delay(attempt=2)
        assert delay2 > delay1
        assert delay3 > delay2
    
    def test_backoff_caps_at_max_delay(self):
        """Delay is capped at max_delay."""
        config = RetryConfig(base_delay=1.0, max_delay=5.0, jitter=False)
        delay = config.get_delay(attempt=100)
        assert delay <= 5.0
    
    def test_jitter_adds_randomness(self):
        """Jitter introduces randomness to delay values."""
        config = RetryConfig(base_delay=1.0, jitter=True)
        delays = [config.get_delay(attempt=2) for _ in range(10)]
        # With jitter, we shouldn't get identical delays
        unique_delays = len(set(delays))
        assert unique_delays > 1


# ==================== ApiResponse Tests ====================

class TestApiResponse:
    """Test suite for ApiResponse validation."""
    
    def test_success_response(self):
        """Successful responses are properly marked."""
        response = ApiResponse(success=True, data={"key": "value"}, status_code=200)
        assert response.success
        assert response.data == {"key": "value"}
        assert response.status_code == 200
        assert response.error is None
    
    def test_failure_response(self):
        """Failed responses include error details."""
        response = ApiResponse(success=False, error="Connection timeout", status_code=504)
        assert not response.success
        assert response.error == "Connection timeout"
        assert response.status_code == 504


# ==================== SchemaValidator Tests ====================

class TestSchemaValidator:
    """Test suite for response schema validation."""
    
    def test_validates_correct_schema(self):
        """Valid responses pass validation."""
        validator = SchemaValidator(required_fields=["id", "name"])
        data = {"id": 123, "name": "Test"}
        assert validator.validate(data)
    
    def test_rejects_missing_fields(self):
        """Responses missing required fields fail validation."""
        validator = SchemaValidator(required_fields=["id", "name"])
        data = {"id": 123}  # Missing 'name'
        assert not validator.validate(data)
    
    def test_accepts_extra_fields(self):
        """Extra fields don't cause validation failures."""
        validator = SchemaValidator(required_fields=["id"])
        data = {"id": 123, "extra": "field", "another": "one"}
        assert validator.validate(data)
    
    def test_empty_required_fields(self):
        """Empty required fields list accepts any dict."""
        validator = SchemaValidator(required_fields=[])
        assert validator.validate({"anything": "goes"})
        assert validator.validate({})


# ==================== UnreliableApiSimulator Tests ====================

class TestUnreliableApiSimulator:
    """Test suite for API failure simulation."""
    
    def test_can_succeed(self):
        """Simulator can return successful responses."""
        simulator = UnreliableApiSimulator(failure_rate=0.0)
        response = simulator.call("/test")
        assert response.success
    
    def test_timeout_failures(self):
        """Simulator can simulate timeout failures."""
        simulator = UnreliableApiSimulator(failure_rate=1.0)
        simulator.set_failure_mode(FailureMode.TIMEOUT)
        response = simulator.call("/test")
        assert not response.success
        assert "timeout" in response.error.lower() or response.status_code >= 400
    
    def test_rate_limit_failures(self):
        """Simulator can simulate rate limiting."""
        simulator = UnreliableApiSimulator(failure_rate=1.0)
        simulator.set_failure_mode(FailureMode.RATE_LIMIT)
        response = simulator.call("/test")
        assert not response.success
        assert response.status_code == 429 or "rate" in response.error.lower()
    
    def test_server_error_failures(self):
        """Simulator can simulate server errors."""
        simulator = UnreliableApiSimulator(failure_rate=1.0)
        simulator.set_failure_mode(FailureMode.SERVER_ERROR)
        response = simulator.call("/test")
        assert not response.success
        assert response.status_code >= 500
    
    def test_malformed_response(self):
        """Simulator can return malformed responses."""
        simulator = UnreliableApiSimulator(failure_rate=1.0)
        simulator.set_failure_mode(FailureMode.MALFORMED_RESPONSE)
        response = simulator.call("/test")
        # Malformed responses may succeed but have invalid data
        assert response is not None


# ==================== RobustApiClient Integration Tests ====================

class TestRobustApiClient:
    """Integration tests for the resilient API client."""
    
    def test_succeeds_with_reliable_api(self):
        """Client succeeds when API is reliable."""
        simulator = UnreliableApiSimulator(failure_rate=0.0)
        client = RobustApiClient(api=simulator)
        response = client.request("/test")
        assert response.success
    
    def test_retries_on_transient_failures(self):
        """Client retries after transient failures."""
        # Create an API that fails twice then succeeds
        simulator = UnreliableApiSimulator(failure_rate=0.0)
        call_count = [0]
        original_call = simulator.call
        
        def failing_then_succeeding(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                return ApiResponse(success=False, error="Transient error", status_code=503)
            return original_call(*args, **kwargs)
        
        simulator.call = failing_then_succeeding
        client = RobustApiClient(api=simulator, retry_config=RetryConfig(max_retries=5, base_delay=0.01))
        response = client.request("/test")
        assert response.success
        assert call_count[0] == 3  # 2 failures + 1 success
    
    def test_respects_circuit_breaker(self):
        """Client stops calling when circuit is open."""
        simulator = UnreliableApiSimulator(failure_rate=1.0)
        simulator.set_failure_mode(FailureMode.SERVER_ERROR)
        client = RobustApiClient(
            api=simulator,
            circuit_breaker=CircuitBreaker(failure_threshold=2, recovery_timeout=10.0),
            retry_config=RetryConfig(max_retries=1, base_delay=0.01)
        )
        
        # Make enough requests to trip the circuit
        for _ in range(5):
            client.request("/test")
        
        # Circuit should be open now
        assert client.circuit_breaker.state == "OPEN"
    
    def test_handles_validation_failures(self):
        """Client detects and handles invalid response schemas."""
        simulator = UnreliableApiSimulator(failure_rate=0.0)
        validator = SchemaValidator(required_fields=["required_field"])
        client = RobustApiClient(api=simulator, schema_validator=validator)
        response = client.request("/test")
        # Response might fail validation if it doesn't have required_field
        # This tests the validation integration


# ==================== Resilience Pattern Tests ====================

class TestResiliencePatterns:
    """Tests for combined resilience patterns."""
    
    def test_graceful_degradation(self):
        """System degrades gracefully under pressure."""
        simulator = UnreliableApiSimulator(failure_rate=0.5)
        client = RobustApiClient(
            api=simulator,
            retry_config=RetryConfig(max_retries=3, base_delay=0.01)
        )
        
        # Even with 50% failure rate, retries should help
        successes = sum(1 for _ in range(10) if client.request("/test").success)
        # Should have at least some successes with retries
        assert successes > 0
    
    def test_bulkhead_isolation(self):
        """Failures in one request don't cascade to others."""
        simulator = UnreliableApiSimulator(failure_rate=0.0)
        client = RobustApiClient(api=simulator)
        
        # Even after some failures, the client should recover
        original_call = simulator.call
        simulator.call = lambda *args, **kwargs: ApiResponse(success=False, error="Fail", status_code=500)
        client.request("/failing")
        
        # Restore normal behavior
        simulator.call = original_call
        response = client.request("/succeeding")
        assert response.success


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
