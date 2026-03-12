from __future__ import annotations

import json
import importlib.util
import subprocess
import sys
import textwrap
from pathlib import Path

# Load the task3_env module by absolute file path (same pattern as test_task3_env)
_FILE = Path(__file__).resolve().parents[1] / 'task3_env.py'
spec = importlib.util.spec_from_file_location('gpt5_t3_cp', _FILE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore[attr-defined]


def _py(code: str, *args: str) -> subprocess.CompletedProcess[str]:
    c = textwrap.dedent(code)
    return subprocess.run([sys.executable, "-c", c, *args], capture_output=True, text=True, check=False)


def test_cross_process_exclusive_lock(tmp_path: Path):
    lk = tmp_path / "x.lock"
    ok, why = mod.acquire_lock(lk, stale_after_s=0.0)
    assert ok, why

    child = _py(
        f"""
import importlib.util, json, sys
from pathlib import Path
FILE=Path(r"{_FILE}")
spec=importlib.util.spec_from_file_location('gpt5_t3_child', FILE)
mod=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore[attr-defined]
lk=Path(sys.argv[1])
ok, why = mod.acquire_lock(lk, stale_after_s=5.0)
print(json.dumps({{'ok': ok, 'why': why}}))
""",
        str(lk),
    )
    assert child.returncode == 0, child.stderr
    out = json.loads(child.stdout.strip())
    assert out["ok"] is False
    assert out["why"] in {"held", "raced"}


def test_cross_process_stale_reclamation(tmp_path: Path):
    lk = tmp_path / "stale.lock"
    # Create a stale lock owned by the child process (dead pid afterwards)
    child = _py(
        """
import json, os, socket, sys, time
from pathlib import Path
p=Path(sys.argv[1])
p.write_text(json.dumps({ 'pid': os.getpid(), 'created_at': time.time() - 99999.0, 'host': socket.gethostname() }))
""",
        str(lk),
    )
    assert child.returncode == 0, child.stderr

    ok, why = mod.acquire_lock(lk, stale_after_s=1.0)
    assert ok and why == "reclaimed"
    notes = list(tmp_path.glob('stale.lock.stale.*'))
    assert notes, 'expected stale provenance note'
