from __future__ import annotations

from pathlib import Path
import importlib.util
import json
import stat
import time


_FILE = Path(__file__).resolve().parents[1] / 'task3_env.py'
spec = importlib.util.spec_from_file_location('gpt5_t3', _FILE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore


def test_acquire_and_release_lock_basic(tmp_path: Path):
    lk = tmp_path / 't.lock'
    ok, why = mod.acquire_lock(lk, stale_after_s=0.0)
    assert ok and why in {"created", "reclaimed"}
    # Second acquire without staleness should fail
    ok2, why2 = mod.acquire_lock(lk, stale_after_s=0.0)
    assert not ok2 and why2 in {"held", "raced"}
    # Release and reacquire should succeed
    assert mod.release_lock(lk) is True
    ok3, _ = mod.acquire_lock(lk, stale_after_s=0.0)
    assert ok3 is True


def test_stale_lock_reclamation(tmp_path: Path):
    lk = tmp_path / 'stale.lock'
    ok, _ = mod.acquire_lock(lk, stale_after_s=0.0)
    assert ok
    # Force staleness by editing created_at to far past and pid to an unlikely value
    data = json.loads(lk.read_text())
    data['created_at'] = time.time() - 99999.0
    data['pid'] = 999999  # almost certainly non-existent
    lk.write_text(json.dumps(data))
    ok2, why2 = mod.acquire_lock(lk, stale_after_s=1.0)
    assert ok2 and why2 == 'reclaimed'
    # A provenance note should exist
    notes = list(tmp_path.glob('stale.lock.stale.*'))
    assert notes, 'expected a stale note file to be written'


def test_getenv_cached_and_invalidate(monkeypatch):
    monkeypatch.setenv('X_T3_KEY', 'A')
    assert mod.getenv_cached('X_T3_KEY') == 'A'
    # Change env; cached value should persist
    monkeypatch.setenv('X_T3_KEY', 'B')
    assert mod.getenv_cached('X_T3_KEY') == 'A'
    # Invalidate and observe updated value
    mod.invalidate_env_cache('X_T3_KEY')
    assert mod.getenv_cached('X_T3_KEY') == 'B'


def test_ensure_tmpdir_writable(tmp_path: Path):
    p = tmp_path / 'w'
    out = mod.ensure_tmpdir(p)
    assert out.exists() and out.is_dir()
    # Make a non-writable dir and expect failure
    q = tmp_path / 'ro'
    q.mkdir()
    q.chmod(stat.S_IRUSR | stat.S_IXUSR)  # 0500
    try:
        raised = False
        try:
            mod.ensure_tmpdir(q)
        except RuntimeError:
            raised = True
        assert raised, 'ensure_tmpdir should raise on non-writable dir'
    finally:
        # Restore perms so tmp cleanup can remove it
        q.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
