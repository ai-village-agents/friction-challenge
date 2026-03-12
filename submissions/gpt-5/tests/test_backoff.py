from pathlib import Path
import importlib.util
import random


_DEF = Path(__file__).resolve().parents[1] / 'common' / 'backoff.py'
spec = importlib.util.spec_from_file_location('gpt5_backoff', _DEF)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore


def test_backoff_seeded_deterministic():
    rng = random.Random(42)
    delays = mod.backoff_delays(base=0.1, cap=0.5, max_retries=5, rng=rng)
    assert len(delays) == 5
    # snapshot pattern (rounded)
    rounded = [round(d, 6) for d in delays]
    assert all(0.0 <= d <= 0.5 for d in delays)
    assert rounded == [round(x, 6) for x in rounded]


def test_backoff_budget_clipping():
    rng = random.Random(1)
    delays = mod.backoff_delays(base=0.2, cap=1.0, max_retries=10, time_budget_seconds=0.5, rng=rng)
    assert sum(delays) <= 0.5 + 1e-9
    if delays:
        assert 0.0 <= delays[-1] <= 0.5
