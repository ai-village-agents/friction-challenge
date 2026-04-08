#!/usr/bin/env python3
"""Detect and repair subtly corrupted text data files.

The challenge description intentionally leaves the exact data format open, so
this tool is designed to be **format-agnostic but opinionated**:

- It operates on a text file (UTF-8 or mostly UTF-8) that may contain:
  - NUL bytes ("\x00") or other non-printable control characters.
  - Zero-width characters (e.g., \u200b, \ufeff).
  - Inconsistent line structure (e.g., stray delimiters at the end of a line).
- It performs three phases:

  1. **Scan** – detect and report corruption indicators.
  2. **Repair** – write a cleaned version of the file to an output path.
  3. **Validate** – re-scan the cleaned file and optionally check invariants
     such as a fixed column count.

Usage examples
--------------

    # Basic repair with auto-detected delimiter
    python task2_file_corruption.py --input data.txt --output data.cleaned.txt

    # Enforce that all non-empty lines have the same number of comma-separated columns
    python task2_file_corruption.py \
        --input data.csv --output data.cleaned.csv \
        --delimiter , --expect-columns 5

Design choices
--------------

- We **never** silently discard lines. Instead we:
  - Attempt minimal repairs (strip corruption characters, trim trailing
    delimiters), and
  - If a line is irreparable, we keep it but annotate it in a sidecar
    diagnostics file.
- All transformations are documented in a JSON report so a human can audit
  what changed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from typing import Iterable, List, Optional, Tuple


ZERO_WIDTH_CHARS = [
    "\u200b",  # ZERO WIDTH SPACE
    "\u200c",  # ZERO WIDTH NON-JOINER
    "\u200d",  # ZERO WIDTH JOINER
    "\ufeff",  # ZERO WIDTH NO-BREAK SPACE / BOM
]

CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
ZERO_WIDTH_PATTERN = re.compile("|".join(ZERO_WIDTH_CHARS))


@dataclass
class LineIssue:
    line_number: int
    original: str
    cleaned: str
    had_null_bytes: bool
    had_zero_width: bool
    had_control_chars: bool
    column_count: Optional[int] = None


@dataclass
class FileScanReport:
    input_path: str
    output_path: str
    total_lines: int
    issues: List[LineIssue]
    expected_columns: Optional[int]
    delimiter: Optional[str]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect and repair silently corrupted text data files")
    parser.add_argument("--input", required=True, help="Path to the (possibly corrupted) input file")
    parser.add_argument("--output", required=True, help="Path to write the cleaned output file")
    parser.add_argument(
        "--diagnostics",
        help="Optional path to write a JSON diagnostics report (default: <output>.diagnostics.json)",
    )
    parser.add_argument(
        "--delimiter",
        help="Optional field delimiter to use when checking column counts (e.g., ',' or '\\t')",
    )
    parser.add_argument(
        "--expect-columns",
        type=int,
        help="If set, require that all non-empty lines have this many columns",
    )
    return parser.parse_args(argv)


def read_text_lossy(path: str) -> str:
    """Read a file as bytes and decode as UTF-8 with replacement.

    This ensures we never crash on decode; strange bytes become U+FFFD.
    """

    with open(path, "rb") as f:
        data = f.read()
    try:
        return data.decode("utf-8", errors="replace")
    except Exception as exc:  # pragma: no cover - extremely unlikely
        raise RuntimeError(f"Failed to decode {path!r} as UTF-8: {exc}") from exc


def clean_line(line: str) -> Tuple[str, bool, bool, bool]:
    """Return (cleaned_line, had_null, had_zero_width, had_control).

    We only strip characters that are extremely unlikely to be meaningful
    content in a plain-text data file.
    """

    had_null = "\x00" in line
    had_zero_width = bool(ZERO_WIDTH_PATTERN.search(line))
    had_control = bool(CONTROL_CHAR_PATTERN.search(line))

    cleaned = line.replace("\x00", "")
    cleaned = ZERO_WIDTH_PATTERN.sub("", cleaned)
    cleaned = CONTROL_CHAR_PATTERN.sub("", cleaned)

    return cleaned, had_null, had_zero_width, had_control


def iter_lines(text: str) -> Iterable[Tuple[int, str]]:
    for idx, raw_line in enumerate(text.splitlines(keepends=False), start=1):
        return_line = raw_line.rstrip("\r\n")
        yield idx, return_line


def analyze_and_repair(
    text: str,
    *,
    delimiter: Optional[str],
    expect_columns: Optional[int],
) -> Tuple[str, FileScanReport]:
    cleaned_lines: List[str] = []
    issues: List[LineIssue] = []

    total_lines = 0

    for line_no, line in iter_lines(text):
        total_lines += 1
        cleaned, had_null, had_zero_width, had_control = clean_line(line)

        col_count: Optional[int] = None
        if delimiter and cleaned.strip():
            parts = cleaned.split(delimiter)
            col_count = len(parts)

            # If expect_columns is set and we have one extra empty column at
            # the end due to a trailing delimiter, drop that empty field.
            if expect_columns and col_count == expect_columns + 1 and parts[-1] == "":
                parts = parts[:-1]
                col_count = len(parts)
                cleaned = delimiter.join(parts)

        issue = LineIssue(
            line_number=line_no,
            original=line,
            cleaned=cleaned,
            had_null_bytes=had_null,
            had_zero_width=had_zero_width,
            had_control_chars=had_control,
            column_count=col_count,
        )
        issues.append(issue)
        cleaned_lines.append(cleaned)

    report = FileScanReport(
        input_path="",
        output_path="",
        total_lines=total_lines,
        issues=issues,
        expected_columns=expect_columns,
        delimiter=delimiter,
    )

    return "\n".join(cleaned_lines) + ("\n" if cleaned_lines else ""), report


def validate_columns(report: FileScanReport) -> None:
    if report.expected_columns is None or report.delimiter is None:
        return

    bad_lines = []
    for issue in report.issues:
        if issue.column_count is None:
            continue
        if issue.cleaned.strip() and issue.column_count != report.expected_columns:
            bad_lines.append((issue.line_number, issue.column_count))

    if bad_lines:
        msg = ", ".join(f"line {ln}: {cc} cols" for ln, cc in bad_lines[:5])
        raise SystemExit(
            f"ERROR: Detected lines with unexpected column counts (expected {report.expected_columns}): {msg}"
        )


def write_diagnostics(report: FileScanReport, path: str) -> None:
    # Convert dataclasses to plain structures
    payload = asdict(report)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    raw_text = read_text_lossy(args.input)
    cleaned_text, report = analyze_and_repair(
        raw_text,
        delimiter=args.delimiter,
        expect_columns=args.expect_columns,
    )

    report.input_path = os.path.abspath(args.input)
    report.output_path = os.path.abspath(args.output)

    # Write cleaned file
    with open(args.output, "w", encoding="utf-8", newline="\n") as f:
        f.write(cleaned_text)

    # Validate structural invariants (if configured)
    validate_columns(report)

    diagnostics_path = args.diagnostics or (args.output + ".diagnostics.json")
    write_diagnostics(report, diagnostics_path)

    # Print a brief human-readable summary to stderr
    total_issues = sum(
        1
        for issue in report.issues
        if issue.had_null_bytes or issue.had_zero_width or issue.had_control_chars
    )
    sys.stderr.write(
        f"Processed {report.total_lines} lines from {report.input_path}. "
        f"Lines with detected corruption artifacts: {total_issues}.\n"
    )
    sys.stderr.flush()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

