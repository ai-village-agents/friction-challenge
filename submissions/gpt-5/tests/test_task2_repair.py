from pathlib import Path
import importlib.util

_FILE = Path(__file__).resolve().parents[1] / 'task2_repair.py'
spec = importlib.util.spec_from_file_location('gpt5_t2', _FILE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore


def test_sanitize_removes_nul_and_cf_and_normalizes():
    s = "A\x00B\u200bC\u212b"  # ZERO WIDTH + Angstrom (should NFC to Å)
    out = mod.sanitize_text(s)
    assert "\x00" not in out
    assert "\u200b" not in out
    assert "Å" in out  # normalized form present


def test_detect_and_repair_csv():
    lines = [
        "id,date,name\n",
        "1,2026-02-25,Alpha\n",
        "2,2026-02-30,Beta\n",  # bad date
        "3,2026-02-24,Ga\x00mma\n",  # NUL inside
        "4,2026-02-24,\u200bDelta\n",  # zero-width
        "5,2026-02-24\n",  # truncated
    ]
    counts = mod.detect_csv_anomalies(lines)
    assert counts["nul"] >= 1
    assert counts["control"] >= 1
    assert counts["bad_dates"] >= 1

    repaired = mod.repair_csv(lines)
    # Should keep header plus well-formed rows (drop truncated)
    assert repaired[0].startswith("id,date,name")
    assert all("\x00" not in ln for ln in repaired)
    assert all("\u200b" not in ln for ln in repaired)
    # All rows should have 3 columns
    import csv
    import io
    rows = list(csv.reader(io.StringIO("".join(repaired))))
    assert all(len(r) == 3 for r in rows)
