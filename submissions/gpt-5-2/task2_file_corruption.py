#!/usr/bin/env python3
"""Task 2: File corruption scan/repair/process pipeline."""
from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import json
import os
import random
import re
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
import unicodedata


HEADER = ["id", "name", "date", "score"]
ZERO_WIDTH = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060\u2061\u2062\u2063\u2064]")
CONTROL_CHARS = "".join(chr(i) for i in range(0, 32) if chr(i) not in ("\n", "\t"))
CONTROL_RE = re.compile(f"[{re.escape(CONTROL_CHARS)}]")
HOMOGLYPH_MAP = str.maketrans(
    {
        "а": "a",
        "е": "e",
        "о": "o",
        "р": "p",
        "с": "c",
        "х": "x",
        "А": "A",
        "В": "B",
        "Е": "E",
        "К": "K",
        "М": "M",
        "Н": "H",
        "О": "O",
        "Р": "P",
        "С": "C",
        "Т": "T",
        "Х": "X",
    }
)


def generate_clean(rows: int = 30) -> List[List[str]]:
    data = []
    rng = random.Random(0)
    base_date = dt.date(2024, 1, 1)
    for i in range(rows):
        day = base_date + dt.timedelta(days=i)
        score = rng.randint(0, 100)
        data.append([str(i + 1), f"User {i+1}", day.isoformat(), str(score)])
    return data


def inject_corruption(rows: List[List[str]]) -> bytes:
    rng = random.Random(123)
    parts: List[bytes] = []
    header = ",".join(HEADER) + "\r\n"
    parts.append(header.encode("utf-8"))
    for row in rows:
        id_, name, date, score = row
        # Randomly apply corruptions.
        if rng.random() < 0.2:
            name = name.replace("a", "а")  # homoglyph
        if rng.random() < 0.1:
            date = "2025-02-30"  # impossible
        if rng.random() < 0.1:
            score = str(150)
        line = f"{id_},{name},{date},{score}"
        if rng.random() < 0.1:
            line = "\x00" + line  # NUL
        if rng.random() < 0.1:
            line = line + "\u200b"  # zero width
        if rng.random() < 0.1:
            line = line.replace(",", "\t")  # structural issues
        if rng.random() < 0.1:
            line = line.replace("e", "\x1e")  # control char
        newline = "\r\n" if rng.random() < 0.5 else "\n"
        parts.append(line.encode("utf-8") + newline.encode())
    # Mixed binary corruption
    blob = b"".join(parts)
    if rng.random() < 0.5:
        blob = blob.replace(b",", b",\x00", 1)
    return blob


@dataclasses.dataclass
class ScanReport:
    nul_offsets: List[int]
    zero_width_lines: List[int]
    control_lines: List[int]
    invalid_dates: List[int]
    bad_scores: List[int]

    def to_dict(self) -> Dict:
        return dataclasses.asdict(self)


def scan_bytes(raw: bytes) -> ScanReport:
    nul_offsets = [i for i, b in enumerate(raw) if b == 0]
    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    zero_width_lines = []
    control_lines = []
    invalid_dates = []
    bad_scores = []
    for idx, line in enumerate(lines[1:], start=2):
        if ZERO_WIDTH.search(line):
            zero_width_lines.append(idx)
        if CONTROL_RE.search(line):
            control_lines.append(idx)
        cols = re.split(r"[,\t]", line)
        if len(cols) != 4:
            invalid_dates.append(idx)
            bad_scores.append(idx)
            continue
        date = cols[2]
        score = cols[3]
        try:
            y, m, d = map(int, date.split("-"))
            dt.date(y, m, d)
        except Exception:  # noqa: BLE001
            invalid_dates.append(idx)
        try:
            sval = int(score)
            if sval < 0 or sval > 100:
                bad_scores.append(idx)
        except Exception:  # noqa: BLE001
            bad_scores.append(idx)
    return ScanReport(
        nul_offsets=nul_offsets,
        zero_width_lines=zero_width_lines,
        control_lines=control_lines,
        invalid_dates=invalid_dates,
        bad_scores=bad_scores,
    )


def clean_bytes(raw: bytes) -> bytes:
    text = raw.decode("utf-8", errors="replace")
    text = unicodedata.normalize("NFC", text)
    text = ZERO_WIDTH.sub("", text)
    text = CONTROL_RE.sub("", text)
    text = text.translate(HOMOGLYPH_MAP)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")
    fixed_lines: List[str] = []
    for line in lines:
        if not line:
            continue
        cols = line.split(",")
        if len(cols) != 4:
            cols = re.split(r"[,\t]", line)
        if cols == HEADER:
            fixed_lines.append(",".join(cols))
            continue
        if len(cols) != 4:
            continue
        id_, name, date, score = cols
        try:
            y, m, d = map(int, date.split("-"))
            dt.date(y, m, d)
        except Exception:  # noqa: BLE001
            date = ""
        try:
            sval = int(score)
            if sval < 0:
                sval = 0
            if sval > 100:
                sval = 100
            score = str(sval)
        except Exception:  # noqa: BLE001
            score = ""
        fixed_lines.append(",".join([id_.strip(), name.strip(), date, score]))
    if fixed_lines and fixed_lines[0] != ",".join(HEADER):
        fixed_lines.insert(0, ",".join(HEADER))
    cleaned = "\n".join(fixed_lines) + ("\n" if fixed_lines else "")
    return cleaned.encode("utf-8")


def validate(path: Path) -> Tuple[bool, str]:
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        rows = list(reader)
    if not rows or rows[0] != HEADER:
        return False, "bad header"
    seen = set()
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) != 4:
            return False, f"line {idx} wrong column count"
        id_, _name, date, score = row
        try:
            iid = int(id_)
        except Exception:  # noqa: BLE001
            return False, f"line {idx} non-int id"
        if iid in seen:
            return False, f"line {idx} duplicate id"
        seen.add(iid)
        if date:
            try:
                y, m, d = map(int, date.split("-"))
                dt.date(y, m, d)
            except Exception:  # noqa: BLE001
                return False, f"line {idx} invalid date"
        try:
            sval = int(score)
            if sval < 0 or sval > 100:
                return False, f"line {idx} score out of range"
        except Exception:  # noqa: BLE001
            return False, f"line {idx} bad score"
    return True, "ok"


def process(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    scores = [int(r["score"]) for r in rows if r["score"]]
    ids = [int(r["id"]) for r in rows if r.get("id")]
    top = sorted(zip(ids, scores), key=lambda x: x[1], reverse=True)[:3]
    avg = sum(scores) / len(scores) if scores else 0.0
    report = {"rows": len(rows), "average_score": avg, "top": top}
    return report


def run_demo() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        clean_path = base / "clean.csv"
        corrupt_path = base / "corrupt.csv"
        repaired_path = base / "repaired.csv"
        data = generate_clean()
        blob = inject_corruption(data)
        clean_path.write_text(",".join(HEADER) + "\n", encoding="utf-8")
        corrupt_path.write_bytes(blob)
        scan = scan_bytes(blob)
        repaired = clean_bytes(blob)
        repaired_path.write_bytes(repaired)
        ok, reason = validate(repaired_path)
        summary = process(repaired_path) if ok else {}
        print(
            json.dumps(
                {
                    "scan": scan.to_dict(),
                    "valid": ok,
                    "reason": reason,
                    "summary": summary,
                }
            )
        )
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["corrupt", "repair", "process", "demo"], required=True)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--report-json", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.mode == "demo":
        return run_demo()

    if args.mode == "corrupt":
        if not args.output:
            parser.error("--output required for corrupt mode")
        rows = generate_clean()
        blob = inject_corruption(rows)
        args.output.write_bytes(blob)
        return 0

    if args.mode == "repair":
        if not args.input or not args.output:
            parser.error("--input and --output required for repair mode")
        raw = args.input.read_bytes()
        repaired = clean_bytes(raw)
        args.output.write_bytes(repaired)
        report = scan_bytes(repaired)
        if args.report_json:
            args.report_json.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
        return 0

    if args.mode == "process":
        if not args.input:
            parser.error("--input required for process mode")
        ok, reason = validate(args.input)
        if not ok:
            print(f"validation failed: {reason}")
            return 1
        summary = process(args.input)
        if args.report_json:
            args.report_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary))
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
