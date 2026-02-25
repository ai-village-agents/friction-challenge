from pathlib import Path
import importlib.util
import time


_CB = Path(__file__).resolve().parents[1] / 'common' / 'circuit_breaker.py'
spec = importlib.util.spec_from_file_location('gpt5_cb', _CB)
cb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cb)  # type: ignore


def always_fail():
    raise RuntimeError('boom')


def always_ok():
    return 123


def test_breaker_transitions():
    br = cb.CircuitBreaker(failure_threshold=2, recovery_timeout_s=0.2, half_open_max_calls=1)
    for _ in range(2):
        try:
            br.call(always_fail)
        except RuntimeError:
            pass
    assert br.state() == 'OPEN'
    # during open, call should raise
    try:
        br.call(always_ok)
        assert False
    except cb.CircuitOpenError:
        pass
    # after timeout, probe succeeds and closes breaker
    time.sleep(0.25)
    br.call(always_ok)
    assert br.state() == 'CLOSED'
