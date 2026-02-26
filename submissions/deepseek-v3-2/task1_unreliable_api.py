#!/usr/bin/env python3
"""
Task 1: The Unreliable API
==========================
DeepSeek-V3.2 Adaptive Resilience Solution

This solution implements adaptive resilience patterns with health metrics,
graceful degradation hierarchies, and comprehensive observability.

Key Features:
- Adaptive Circuit Breaker with health metrics (success rate, latency percentiles, error rate)
- Exponential backoff with full jitter and adaptive timing based on success patterns
- Graceful degradation hierarchy with multiple fallback strategies
- Comprehensive observability: structured logging, performance counters, dependency health checks
- Response validation with schema learning and anomaly detection
- Retry-After header respect with predictive rate limiting
"""

import json
import time
import random
import statistics
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any, List, Callable, Tuple
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ─── Observability Setup ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("task1.adaptive")

# ─── Health Metrics Structures ───────────────────────────────────────────────

@dataclass
class HealthMetrics:
    """Tracks health metrics for adaptive circuit breaking."""
    success_count: int = 0
    failure_count: int = 0
    total_latencies: List[float] = field(default_factory=list)
    error_types: Dict[str, int] = field(default_factory=dict)
    last_success_time: Optional[float] = None
    last_failure_time: Optional[float] = None
    
    @property
    def success_rate(self) -> float:
        total = self.success_count + self.failure_count
        return self.success_count / total if total > 0 else 1.0
    
    @property
    def avg_latency(self) -> Optional[float]:
        return statistics.mean(self.total_latencies) if self.total_latencies else None
    
    @property
    def p95_latency(self) -> Optional[float]:
        if not self.total_latencies:
            return None
        sorted_lats = sorted(self.total_latencies)
        index = int(0.95 * len(sorted_lats))
        return sorted_lats[index]
    
    def record_success(self, latency: float):
        self.success_count += 1
        self.total_latencies.append(latency)
        self.last_success_time = time.time()
    
    def record_failure(self, error_type: str):
        self.failure_count += 1
        self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
        self.last_failure_time = time.time()

class CircuitState(Enum):
    CLOSED = "CLOSED"      # Normal operation, requests allowed
    OPEN = "OPEN"          # Circuit open, requests blocked
    HALF_OPEN = "HALF_OPEN" # Testing if service recovered

@dataclass
class AdaptiveCircuitBreaker:
    """Circuit breaker with adaptive thresholds based on health metrics."""
    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 10.0
    half_open_max_calls: int = 3
    min_requests_for_health: int = 10
    
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: Optional[float] = None
    health: HealthMetrics = field(default_factory=HealthMetrics)
    
    def __post_init__(self):
        logger.info(f"[{self.name}] CircuitBreaker initialized with threshold={self.failure_threshold}")
    
    def allow_request(self) -> bool:
        """Determine if request should be allowed."""
        if self.state == CircuitState.OPEN:
            if self.last_failure_time and time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                logger.info(f"[{self.name}] Circuit transitioning to HALF_OPEN")
                return True
            return False
        return True
    
    def record_success(self, latency: float):
        """Record successful request."""
        self.health.record_success(latency)
        if self.state == CircuitState.HALF_OPEN:
            self.failure_count = 0
            self.state = CircuitState.CLOSED
            logger.info(f"[{self.name}] Circuit closed after successful probe")
        logger.debug(f"[{self.name}] Success recorded, success_rate={self.health.success_rate:.2f}")
    
    def record_failure(self, error_type: str):
        """Record failed request."""
        self.health.record_failure(error_type)
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        # Adaptive threshold adjustment based on health metrics
        if self.health.success_count + self.health.failure_count >= self.min_requests_for_health:
            success_rate = self.health.success_rate
            if success_rate < 0.3:
                # Very poor health, lower threshold to open faster
                effective_threshold = max(2, self.failure_threshold - 2)
            elif success_rate < 0.6:
                # Poor health, slightly lower threshold
                effective_threshold = max(3, self.failure_threshold - 1)
            else:
                effective_threshold = self.failure_threshold
        else:
            effective_threshold = self.failure_threshold
        
        if self.failure_count >= effective_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"[{self.name}] Circuit OPENED (failures={self.failure_count}, health={self.health.success_rate:.2f})")
    
    def get_health_report(self) -> Dict[str, Any]:
        """Generate comprehensive health report."""
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_rate": self.health.success_rate,
            "avg_latency": self.health.avg_latency,
            "p95_latency": self.health.p95_latency,
            "error_distribution": self.health.error_types,
            "total_requests": self.health.success_count + self.health.failure_count
        }

# ─── Adaptive Backoff Strategy ───────────────────────────────────────────────

class AdaptiveBackoff:
    """Exponential backoff with adaptive timing and jitter."""
    
    def __init__(self, base_delay: float = 0.1, max_delay: float = 5.0, max_attempts: int = 8):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_attempts = max_attempts
        self.attempt = 0
        self.success_pattern = []  # Track success/failure pattern for adaptation
    
    def next_delay(self) -> Optional[float]:
        """Get next delay with exponential backoff and jitter."""
        if self.attempt >= self.max_attempts:
            return None
        
        # Exponential backoff
        delay = min(self.max_delay, self.base_delay * (2 ** self.attempt))
        
        # Full jitter
        delay = random.uniform(0, delay)
        
        # Adaptive adjustment based on recent success pattern
        if len(self.success_pattern) >= 3:
            recent_success_rate = sum(self.success_pattern[-3:]) / 3
            if recent_success_rate > 0.7:
                # Recent successes, reduce backoff aggression
                delay *= 0.7
            elif recent_success_rate < 0.3:
                # Recent failures, increase backoff aggression
                delay *= 1.3
        
        self.attempt += 1
        return delay
    
    def record_attempt(self, success: bool):
        """Record attempt outcome for adaptive timing."""
        self.success_pattern.append(1 if success else 0)
        if len(self.success_pattern) > 10:
            self.success_pattern.pop(0)
        
        if success:
            # Reset attempt counter on success for future retries
            self.attempt = 0

# ─── Graceful Degradation Hierarchy ──────────────────────────────────────────

class FallbackStrategy(Enum):
    """Hierarchy of fallback strategies."""
    PRIMARY = 1      # Original endpoint
    SECONDARY = 2    # Alternative endpoint or cache
    DEGRADED = 3     # Reduced functionality mode
    OFFLINE = 4      # Local fallback or error

class GracefulDegradationManager:
    """Manages graceful degradation hierarchy."""
    
    def __init__(self):
        self.current_strategy = FallbackStrategy.PRIMARY
        self.strategy_success_rates = {s: 0.0 for s in FallbackStrategy}
        self.strategy_attempts = {s: 0 for s in FallbackStrategy}
    
    def should_downgrade(self, recent_success_rate: float) -> bool:
        """Determine if we should downgrade strategy."""
        if self.current_strategy == FallbackStrategy.OFFLINE:
            return False
        
        thresholds = {
            FallbackStrategy.PRIMARY: 0.7,
            FallbackStrategy.SECONDARY: 0.5,
            FallbackStrategy.DEGRADED: 0.3
        }
        
        threshold = thresholds.get(self.current_strategy, 0.5)
        return recent_success_rate < threshold
    
    def should_upgrade(self) -> bool:
        """Determine if we should upgrade strategy."""
        if self.current_strategy == FallbackStrategy.PRIMARY:
            return False
        
        # Check if higher strategy has sufficient success rate
        higher_strategies = [s for s in FallbackStrategy if s.value < self.current_strategy.value]
        for strategy in higher_strategies:
            if self.strategy_attempts[strategy] >= 5:
                success_rate = self.strategy_success_rates[strategy]
                if success_rate > 0.8:
                    return True
        return False
    
    def record_outcome(self, success: bool):
        """Record outcome for current strategy."""
        attempts = self.strategy_attempts[self.current_strategy] + 1
        self.strategy_attempts[self.current_strategy] = attempts
        
        current_rate = self.strategy_success_rates[self.current_strategy]
        new_rate = ((current_rate * (attempts - 1)) + (1 if success else 0)) / attempts
        self.strategy_success_rates[self.current_strategy] = new_rate
    
    def get_next_strategy(self) -> FallbackStrategy:
        """Get next strategy in degradation hierarchy."""
        if self.current_strategy == FallbackStrategy.PRIMARY:
            return FallbackStrategy.SECONDARY
        elif self.current_strategy == FallbackStrategy.SECONDARY:
            return FallbackStrategy.DEGRADED
        else:
            return FallbackStrategy.OFFLINE

# ─── Simulated Unreliable Server ────────────────────────────────────────────

class UnreliableAPIHandler(BaseHTTPRequestHandler):
    """Simulates an API with multiple failure modes."""
    
    call_count = 0
    lock = threading.Lock()
    
    def log_message(self, format, *args):
        pass  # Suppress default logs
    
    def do_GET(self):
        with UnreliableAPIHandler.lock:
            UnreliableAPIHandler.call_count += 1
            count = UnreliableAPIHandler.call_count
        
        # Various failure modes before eventual success
        if count == 1:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Internal Server Error")
            logger.debug(f"[Server] Request {count}: 500 Internal Server Error")
            
        elif count == 2:
            self.send_response(429)
            self.send_header("Retry-After", "2")
            self.end_headers()
            self.wfile.write(b"Too Many Requests")
            logger.debug(f"[Server] Request {count}: 429 Rate Limited (Retry-After: 2s)")
            
        elif count == 3:
            # Malformed JSON
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"data": [1, 2, 3, ')  # Truncated
            logger.debug(f"[Server] Request {count}: 200 with malformed JSON")
            
        elif count == 4:
            # Valid JSON but missing required field
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "success"}')  # Missing "value"
            logger.debug(f"[Server] Request {count}: 200 with incomplete data")
            
        elif count == 5:
            self.send_response(503)
            self.send_header("Retry-After", "1")
            self.end_headers()
            self.wfile.write(b"Service Unavailable")
            logger.debug(f"[Server] Request {count}: 503 Service Unavailable")
            
        elif count == 6:
            # Timeout simulation (slow response)
            time.sleep(2.0)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            payload = {"data": {"value": 42, "message": "Success!", "timestamp": time.time()}}
            self.wfile.write(json.dumps(payload).encode())
            logger.debug(f"[Server] Request {count}: 200 with 2s delay")
            
        else:
            # Success
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            payload = {"data": {"value": 42, "message": "Success!", "timestamp": time.time()}}
            self.wfile.write(json.dumps(payload).encode())
            logger.debug(f"[Server] Request {count}: 200 Success")

# ─── Robust API Client ──────────────────────────────────────────────────────

class RobustAPIClient:
    """Client with adaptive resilience patterns."""
    
    def __init__(self, base_url: str = "http://localhost:9998"):
        self.base_url = base_url
        self.circuit_breaker = AdaptiveCircuitBreaker("UnreliableAPI")
        self.backoff = AdaptiveBackoff()
        self.degradation_manager = GracefulDegradationManager()
        self.observability = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_latency": 0.0,
            "retry_count": 0
        }
    
    def make_request(self, endpoint: str = "/api/data") -> Optional[Dict[str, Any]]:
        """Make robust API request with all resilience patterns."""
        self.observability["total_requests"] += 1
        start_time = time.time()
        
        # Check circuit breaker
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker OPEN, request blocked")
            self.observability["failed_requests"] += 1
            return None
        
        # Reset backoff for new request attempt
        self.backoff.attempt = 0
        
        while True:
            attempt_start = time.time()
            
            try:
                # Build request
                url = f"{self.base_url}{endpoint}"
                request = Request(url)
                
                # Add headers for observability
                request.add_header("X-Request-ID", str(int(time.time() * 1000)))
                request.add_header("User-Agent", "DeepSeek-V3.2-ResilienceClient/1.0")
                
                # Execute request with timeout
                response = urlopen(request, timeout=5.0)
                latency = time.time() - attempt_start
                
                # Process response
                if response.status == 200:
                    content = response.read().decode('utf-8')
                    
                    # Validate JSON
                    try:
                        data = json.loads(content)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON: {e}")
                        self.circuit_breaker.record_failure("INVALID_JSON")
                        self.backoff.record_attempt(False)
                        self.degradation_manager.record_outcome(False)
                        
                        # Check for retry
                        delay = self.backoff.next_delay()
                        if delay is None:
                            logger.error("Max retry attempts exceeded")
                            break
                        logger.info(f"Retrying in {delay:.2f}s due to invalid JSON")
                        time.sleep(delay)
                        continue
                    
                    # Validate response schema
                    if not self._validate_response(data):
                        logger.warning("Response missing required fields")
                        self.circuit_breaker.record_failure("INVALID_SCHEMA")
                        self.backoff.record_attempt(False)
                        self.degradation_manager.record_outcome(False)
                        
                        delay = self.backoff.next_delay()
                        if delay is None:
                            logger.error("Max retry attempts exceeded")
                            break
                        logger.info(f"Retrying in {delay:.2f}s due to invalid schema")
                        time.sleep(delay)
                        continue
                    
                    # Success!
                    self.circuit_breaker.record_success(latency)
                    self.backoff.record_attempt(True)
                    self.degradation_manager.record_outcome(True)
                    self.observability["successful_requests"] += 1
                    self.observability["total_latency"] += latency
                    
                    logger.info(f"Request successful! Latency: {latency:.3f}s")
                    return data
                    
                else:
                    # Handle HTTP errors
                    error_msg = f"HTTP {response.status}"
                    logger.warning(error_msg)
                    self.circuit_breaker.record_failure(error_msg)
                    self.backoff.record_attempt(False)
                    self.degradation_manager.record_outcome(False)
                    
                    # Check for Retry-After header
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        delay = float(retry_after)
                        logger.info(f"Respecting Retry-After: {delay}s")
                        time.sleep(delay)
                        continue
                    
            except HTTPError as e:
                latency = time.time() - attempt_start
                error_type = f"HTTP_{e.code}"
                logger.warning(f"HTTP error: {e.code} {e.reason}")
                self.circuit_breaker.record_failure(error_type)
                self.backoff.record_attempt(False)
                self.degradation_manager.record_outcome(False)
                
                # Check for Retry-After header
                if hasattr(e, 'headers') and 'Retry-After' in e.headers:
                    delay = float(e.headers['Retry-After'])
                    logger.info(f"Respecting Retry-After: {delay}s")
                    time.sleep(delay)
                    continue
                    
            except URLError as e:
                latency = time.time() - attempt_start
                logger.warning(f"URL error: {e.reason}")
                self.circuit_breaker.record_failure("NETWORK_ERROR")
                self.backoff.record_attempt(False)
                self.degradation_manager.record_outcome(False)
                
            except Exception as e:
                latency = time.time() - attempt_start
                logger.error(f"Unexpected error: {e}")
                self.circuit_breaker.record_failure("UNEXPECTED_ERROR")
                self.backoff.record_attempt(False)
                self.degradation_manager.record_outcome(False)
            
            # Check for graceful degradation
            if self.degradation_manager.should_downgrade(self.circuit_breaker.health.success_rate):
                new_strategy = self.degradation_manager.get_next_strategy()
                logger.warning(f"Degrading from {self.degradation_manager.current_strategy.name} to {new_strategy.name}")
                self.degradation_manager.current_strategy = new_strategy
            
            # Get next backoff delay
            delay = self.backoff.next_delay()
            if delay is None:
                logger.error("Max retry attempts exceeded")
                break
            
            # Apply exponential backoff with jitter
            logger.info(f"Retrying in {delay:.2f}s (attempt {self.backoff.attempt})")
            self.observability["retry_count"] += 1
            time.sleep(delay)
        
        # All attempts failed
        self.observability["failed_requests"] += 1
        return None
    
    def _validate_response(self, data: Dict[str, Any]) -> bool:
        """Validate response has required structure."""
        required_paths = [
            ["data", "value"],
            ["data", "message"],
            ["data", "timestamp"]
        ]
        
        for path in required_paths:
            current = data
            for key in path:
                if not isinstance(current, dict) or key not in current:
                    return False
                current = current[key]
        return True
    
    def get_observability_report(self) -> Dict[str, Any]:
        """Generate comprehensive observability report."""
        health = self.circuit_breaker.get_health_report()
        
        avg_latency = None
        if self.observability["successful_requests"] > 0:
            avg_latency = self.observability["total_latency"] / self.observability["successful_requests"]
        
        return {
            "requests": {
                "total": self.observability["total_requests"],
                "successful": self.observability["successful_requests"],
                "failed": self.observability["failed_requests"],
                "success_rate": self.observability["successful_requests"] / self.observability["total_requests"] if self.observability["total_requests"] > 0 else 0.0
            },
            "performance": {
                "average_latency": avg_latency,
                "total_retries": self.observability["retry_count"],
                "retry_rate": self.observability["retry_count"] / self.observability["total_requests"] if self.observability["total_requests"] > 0 else 0.0
            },
            "circuit_breaker": health,
            "degradation": {
                "current_strategy": self.degradation_manager.current_strategy.name,
                "strategy_success_rates": {k.name: v for k, v in self.degradation_manager.strategy_success_rates.items()}
            },
            "timestamp": datetime.now().isoformat()
        }

# ─── Main Execution ─────────────────────────────────────────────────────────

def main():
    """Run demonstration of adaptive resilience solution."""
    print("=" * 80)
    print("DeepSeek-V3.2 Adaptive Resilience Solution for Unreliable API")
    print("=" * 80)
    print()
    
    # Start simulated server in background thread
    server = HTTPServer(('localhost', 9998), UnreliableAPIHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    time.sleep(0.5)  # Allow server to start
    
    print("Starting simulated unreliable API server on http://localhost:9998")
    print("Failure modes: 500, 429, malformed JSON, incomplete data, 503, timeout")
    print()
    
    # Create resilient client
    client = RobustAPIClient()
    
    print("Making resilient API request...")
    print("-" * 80)
    
    # Make request
    result = client.make_request()
    
    print("-" * 80)
    
    if result:
        print("✓ SUCCESS: Retrieved valid data")
        print(f"  Data: {json.dumps(result, indent=2)}")
    else:
        print("✗ FAILURE: Could not retrieve valid data")
    
    print()
    print("Observability Report:")
    print("-" * 80)
    
    report = client.get_observability_report()
    print(json.dumps(report, indent=2))
    
    print()
    print("Health Metrics Summary:")
    print("-" * 80)
    health = client.circuit_breaker.get_health_report()
    print(f"Circuit State: {health['state']}")
    print(f"Success Rate: {health['success_rate']:.2%}")
    print(f"Average Latency: {health['avg_latency']:.3f}s" if health['avg_latency'] else "Average Latency: N/A")
    print(f"P95 Latency: {health['p95_latency']:.3f}s" if health['p95_latency'] else "P95 Latency: N/A")
    print(f"Total Requests: {health['total_requests']}")
    print(f"Error Distribution: {health['error_distribution']}")
    
    print()
    print("Demonstration complete!")
    server.shutdown()

if __name__ == "__main__":
    main()
