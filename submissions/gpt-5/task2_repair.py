"""Utilities for detecting and repairing subtle text/CSV corruption (Task 2).

Functions
- sanitize_text(s): remove NULs and zero-width (Cf) characters; NFC normalize
- sha256_bytes(b): hex digest
- detect_csv_anomalies(lines): count anomalies across CSV lines
- repair_csv(lines): sanitize and drop malformed trailing/inconsistent rows
"""
from __future__ import annotations

from typing import List, Dict
import csv
import hashlib
import io
import re
import unicodedata as ud
from datetime import date

_CF_RE = re.compile("[\u200B\u200C\u200D\uFEFF]")  # common Cf chars
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def sanitize_text(s: str) -> str:
    # Remove NULs
    s = s.replace("\x00", "")
    # Remove general Cf class
    s = "".join(ch for ch in s if ud.category(ch) != "Cf")
    # Also ensure removal of a few common Cf explicitly (belt-and-suspenders)
    s = _CF_RE.sub("", s)
    # NFC normalize
    s = ud.normalize("NFC", s)
    return s


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _parse_csv(lines: List[str]) -> List[List[str]]:
    src = io.StringIO("".join(lines))
    return list(csv.reader(src))


def _validate_iso_date(s: str) -> bool:
    if not _DATE_RE.match(s):
        return False
    y, m, d = (int(x) for x in s.split("-"))
    try:
        date(y, m, d)
    except ValueError:
        return False
    return True


def detect_csv_anomalies(lines: List[str]) -> Dict[str, int]:
    counts = {"nul": 0, "control": 0, "empty_fields": 0, "bad_dates": 0}
    # Raw scan for NUL and Cf
    for ln in lines:
        if "\x00" in ln:
            counts["nul"] += 1
        if any(ud.category(ch) == "Cf" for ch in ln):
            counts["control"] += 1
    # Parse sanitized for structural checks
    san_lines = [sanitize_text(ln) for ln in lines]
    rows = _parse_csv(san_lines)
    if not rows:
        return counts
    header = rows[0]
    ncols = len(header)
    date_idx = header.index("date") if "date" in header else -1
    for r in rows[1:]:
        # Count empty fields only for rows with expected column count
        if len(r) == ncols:
            counts["empty_fields"] += sum(1 for fld in r if fld == "")
            if date_idx >= 0 and r[date_idx] and not _validate_iso_date(r[date_idx]):
                counts["bad_dates"] += 1
    return counts


def repair_csv(lines: List[str]) -> List[str]:
    # Sanitize each line first
    san = [sanitize_text(ln) for ln in lines]
    rows = _parse_csv(san)
    if not rows:
        return san
    header = rows[0]
    ncols = len(header)
    out_rows: List[List[str]] = [header]
    for r in rows[1:]:
        if len(r) != ncols:
            # drop inconsistent row (often truncated tail)
            continue
        out_rows.append(r)
    # Re-serialize with csv.writer (deterministic, no trailing spaces)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    for r in out_rows:
        w.writerow(r)
    return buf.getvalue().splitlines(True)
