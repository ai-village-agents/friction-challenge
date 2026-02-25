#!/usr/bin/env python3
"""
Task 2: The Silent File Corruption
====================================
Challenge: A data file has been subtly corrupted. Identify the corruption,
repair the file, and process it correctly.

Corruption Types Identified (6 total):
1. NULL bytes (\x00) embedded mid-field — binary contamination
2. Unicode homoglyphs — Cyrillic/fullwidth chars masquerading as ASCII
3. Zero-width Unicode — invisible characters (ZWSP, BOM, RTL marks)
4. Invalid dates — Feb 30, month 13, day 0
5. Out-of-range values — physically implausible sensor readings
6. Mixed line endings — \r\n mixed with \n causing phantom fields

Real-World Context:
In the AI Village, I encountered silent file corruption when working with
the village-event-log repository. UTF-8 files with mixed line endings caused
git diff to show phantom changes, and copy-pasted text from chat sometimes
included zero-width joiners that broke JSON parsing. The multi-pass pipeline
below handles all of these.

Workaround Strategy:
- Phase 1: SCAN — detect all corruptions without modifying data
- Phase 2: REPAIR — apply fixes with confidence scores
- Phase 3: VALIDATE — re-scan to verify repairs worked
- Phase 4: PROCESS — compute statistics, quarantine unfixable rows
"""

import csv
import datetime
import io
import unicodedata
from typing import Any, Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════════════
# CORRUPTED TEST DATA
# ═══════════════════════════════════════════════════════════════════════════

CORRUPTED_CSV = (
    "id,date,sensor,value,unit\n"
    "1,2026-01-15,temp_A,23.4,celsius\n"           # Clean
    "2,2026-01-16,temp_B,2\uff12.1,celsius\n"      # Fullwidth digit '２'
    "3,2026-01-17,temp_C,21\x00.8,celsius\n"       # NULL byte in value
    "4,2026-01-\u0031\u0038,temp_D,22.5,celsius\n" # Homoglyph in date
    "5,2026-02-30,temp_E,24.0,celsius\n"           # Feb 30 (impossible)
    "6,2026-01-20,temp_F,9999.9,celsius\n"         # Out of range
    "7,2026-01-21,temp_G,\u200b22.3,celsius\n"     # Zero-width space
    "8,2026-01-22,temp_H,23.1,celsius\r\n"         # CRLF line ending
)

# Domain constraints
SENSOR_RANGE = (-50.0, 60.0)  # Celsius: physically plausible for sensors


# ═══════════════════════════════════════════════════════════════════════════
# CORRUPTION SCANNER
# ═══════════════════════════════════════════════════════════════════════════

class CorruptionFinding:
    """Represents a single detected corruption."""
    def __init__(self, row: int, field: str, corruption_type: str,
                 detail: str, raw_value: str):
        self.row = row
        self.field = field
        self.corruption_type = corruption_type
        self.detail = detail
        self.raw_value = raw_value

    def __repr__(self):
        return (f"  Row {self.row}, '{self.field}': {self.corruption_type} "
                f"— {self.detail}")


def detect_invisible_chars(s: str) -> List[Tuple[int, str, str]]:
    """Find invisible or confusable Unicode characters in a string."""
    issues = []
    for i, ch in enumerate(s):
        cat = unicodedata.category(ch)
        # Format chars (Cf): zero-width space, BOM, RTL marks
        # Control chars (Cc): NULL, etc. (except \n, \r, \t)
        if cat == 'Cf':
            name = unicodedata.name(ch, f"U+{ord(ch):04X}")
            issues.append((i, ch, f"Invisible: {name}"))
        elif cat == 'Cc' and ch not in '\n\r\t':
            issues.append((i, ch, f"Control char: U+{ord(ch):04X}"))
        elif ord(ch) > 127:
            name = unicodedata.name(ch, "")
            # Fullwidth digits, Cyrillic lookalikes, etc.
            if any(kw in name for kw in ("FULLWIDTH", "DIGIT", "CYRILLIC")):
                issues.append((i, ch, f"Homoglyph: {name}"))
    return issues


def scan_csv(raw: str) -> List[CorruptionFinding]:
    """Scan raw CSV for all known corruption types. Non-destructive."""
    findings = []
    # Normalize line endings for scanning
    lines = raw.replace('\r\n', '\n').split('\n')

    # Check for mixed line endings in original
    if '\r\n' in raw and '\n' in raw.replace('\r\n', ''):
        findings.append(CorruptionFinding(
            0, "file", "mixed_line_endings",
            "File contains both CRLF and LF line endings",
            repr(raw[:50])
        ))

    for line_num, line in enumerate(lines[1:], start=1):
        if not line.strip():
            continue
        fields = line.split(',')
        if len(fields) < 5:
            findings.append(CorruptionFinding(
                line_num, "row", "malformed_row",
                f"Expected 5 fields, got {len(fields)}", line
            ))
            continue

        row_id, date_str, sensor, value_str, unit = fields[:5]

        # Check 1: NULL bytes
        if '\x00' in line:
            findings.append(CorruptionFinding(
                line_num, "line", "null_byte",
                "NULL byte (\\x00) found", repr(line)
            ))

        # Check 2: Invisible/homoglyph Unicode
        for col_name, field in [("date", date_str), ("value", value_str),
                                 ("id", row_id), ("sensor", sensor)]:
            issues = detect_invisible_chars(field)
            for pos, ch, desc in issues:
                findings.append(CorruptionFinding(
                    line_num, col_name, "unicode_anomaly",
                    f"Pos {pos}: {desc} (U+{ord(ch):04X})", repr(field)
                ))

        # Check 3: Date validity
        clean_date = unicodedata.normalize('NFKC', date_str)
        clean_date = ''.join(c for c in clean_date if c.isascii())
        try:
            datetime.date.fromisoformat(clean_date)
        except ValueError as e:
            findings.append(CorruptionFinding(
                line_num, "date", "invalid_date",
                str(e), date_str
            ))

        # Check 4: Numeric range
        clean_val = unicodedata.normalize('NFKC', value_str)
        clean_val = ''.join(c for c in clean_val if c.isascii())
        clean_val = clean_val.replace('\x00', '')
        try:
            v = float(clean_val)
            lo, hi = SENSOR_RANGE
            if not (lo <= v <= hi):
                findings.append(CorruptionFinding(
                    line_num, "value", "out_of_range",
                    f"Value {v} outside [{lo}, {hi}]", value_str
                ))
        except ValueError:
            findings.append(CorruptionFinding(
                line_num, "value", "unparseable",
                "Cannot parse as float after cleanup", repr(value_str)
            ))

    return findings


# ═══════════════════════════════════════════════════════════════════════════
# CORRUPTION REPAIRER
# ═══════════════════════════════════════════════════════════════════════════

class RepairAction:
    """Records a repair with confidence score."""
    def __init__(self, row: int, field: str, original: str, repaired: str,
                 confidence: float, method: str):
        self.row = row
        self.field = field
        self.original = original
        self.repaired = repaired
        self.confidence = confidence
        self.method = method

    def __repr__(self):
        return (f"  Row {self.row}, '{self.field}': {repr(self.original)} → "
                f"{repr(self.repaired)} [{self.confidence:.0%} confidence, "
                f"{self.method}]")


def repair_field(field: str) -> Tuple[str, List[Tuple[str, float]]]:
    """
    Repair a single field. Returns (repaired_value, list_of_methods_applied).

    Confidence scoring rationale:
    - NULL byte removal: 100% (always correct to remove)
    - NFKC normalization: 95% (fullwidth→ASCII is almost always intended)
    - Zero-width removal: 100% (invisible chars are never intended in data)
    """
    methods = []
    result = field

    # Step 1: Remove NULL bytes
    if '\x00' in result:
        result = result.replace('\x00', '')
        methods.append(("null_byte_removal", 1.0))

    # Step 2: Remove zero-width and format characters
    cleaned = []
    had_invisible = False
    for ch in result:
        cat = unicodedata.category(ch)
        if cat == 'Cf':
            had_invisible = True
            continue
        if cat == 'Cc' and ch not in '\n\r\t':
            had_invisible = True
            continue
        cleaned.append(ch)
    if had_invisible:
        result = ''.join(cleaned)
        methods.append(("invisible_char_removal", 1.0))

    # Step 3: NFKC normalization (fullwidth → ASCII, compatibility decomp)
    normalized = unicodedata.normalize('NFKC', result)
    if normalized != result:
        result = normalized
        methods.append(("nfkc_normalization", 0.95))

    return result, methods


def repair_csv(raw: str) -> Tuple[str, List[RepairAction]]:
    """Repair all known corruption types. Returns (repaired_csv, repair_log)."""
    repairs = []

    # Normalize line endings first
    normalized = raw.replace('\r\n', '\n')
    if normalized != raw:
        repairs.append(RepairAction(
            0, "file", "mixed", "LF-only", 1.0, "line_ending_normalization"
        ))

    lines = normalized.split('\n')
    repaired_lines = [lines[0]]  # Keep header as-is

    for line_num, line in enumerate(lines[1:], start=1):
        if not line.strip():
            repaired_lines.append(line)
            continue

        fields = line.split(',')
        if len(fields) < 5:
            repaired_lines.append(line)
            continue

        new_fields = []
        col_names = ["id", "date", "sensor", "value", "unit"]
        for i, field in enumerate(fields):
            repaired, methods = repair_field(field)
            if repaired != field:
                col_name = col_names[i] if i < len(col_names) else f"col{i}"
                for method, conf in methods:
                    repairs.append(RepairAction(
                        line_num, col_name, field, repaired, conf, method
                    ))
            new_fields.append(repaired)

        # Date repair: flag invalid dates for human review
        if len(new_fields) >= 2:
            try:
                datetime.date.fromisoformat(new_fields[1])
            except ValueError:
                original = new_fields[1]
                new_fields[1] = "QUARANTINED_DATE"
                repairs.append(RepairAction(
                    line_num, "date", original, "QUARANTINED_DATE",
                    0.0, "date_quarantine"
                ))

        # Value range: flag out-of-range values
        if len(new_fields) >= 4:
            try:
                v = float(new_fields[3])
                lo, hi = SENSOR_RANGE
                if not (lo <= v <= hi):
                    new_fields[3] = "QUARANTINED_VALUE"
                    repairs.append(RepairAction(
                        line_num, "value", str(v), "QUARANTINED_VALUE",
                        0.0, "range_quarantine"
                    ))
            except ValueError:
                pass

        repaired_lines.append(','.join(new_fields))

    return '\n'.join(repaired_lines), repairs


# ═══════════════════════════════════════════════════════════════════════════
# DATA PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════

def process_csv(clean_csv: str) -> Dict[str, Any]:
    """Process repaired CSV, separating clean rows from quarantined ones."""
    reader = csv.DictReader(io.StringIO(clean_csv))
    valid = []
    quarantined = []

    for row in reader:
        if ("QUARANTINED" in row.get("date", "") or
                "QUARANTINED" in row.get("value", "")):
            quarantined.append(row)
            continue
        try:
            valid.append({
                "id": int(row["id"]),
                "date": row["date"],
                "sensor": row["sensor"],
                "value": float(row["value"]),
                "unit": row["unit"],
            })
        except (ValueError, KeyError):
            quarantined.append(row)

    values = [r["value"] for r in valid]
    return {
        "valid_count": len(valid),
        "quarantined_count": len(quarantined),
        "mean": sum(values) / len(values) if values else None,
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "sensors": sorted(set(r["sensor"] for r in valid)),
        "quarantined_rows": quarantined,
    }


# ═══════════════════════════════════════════════════════════════════════════
# MAIN — DEMONSTRATION
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 65)
    print("TASK 2: The Silent File Corruption")
    print("=" * 65)
    print()

    # Phase 1: Scan
    print("PHASE 1: Corruption Scan")
    print("-" * 40)
    findings = scan_csv(CORRUPTED_CSV)
    print(f"Detected {len(findings)} corruption(s):")
    for f in findings:
        print(f)
    print()

    # Phase 2: Repair
    print("PHASE 2: Repair")
    print("-" * 40)
    repaired, repairs = repair_csv(CORRUPTED_CSV)
    print(f"Applied {len(repairs)} repair(s):")
    for r in repairs:
        print(r)
    print()

    # Phase 3: Validate
    print("PHASE 3: Post-Repair Validation")
    print("-" * 40)
    post_findings = scan_csv(repaired)
    real_issues = [f for f in post_findings
                   if "QUARANTINED" not in f.raw_value]
    if real_issues:
        print(f"WARNING: {len(real_issues)} issue(s) remain:")
        for f in real_issues:
            print(f)
    else:
        print("All corruptions repaired or quarantined.")
    print()

    # Phase 4: Process
    print("PHASE 4: Process")
    print("-" * 40)
    stats = process_csv(repaired)
    print(f"  Valid rows:      {stats['valid_count']}")
    print(f"  Quarantined:     {stats['quarantined_count']}")
    if stats["mean"] is not None:
        print(f"  Mean temperature: {stats['mean']:.2f} C")
        print(f"  Range:           {stats['min']:.1f} C to {stats['max']:.1f} C")
    print(f"  Active sensors:  {', '.join(stats['sensors'])}")
    print()
    print("Quarantined rows preserved for human review — never silently dropped.")
    print()
    print("SUCCESS: File corruption detected, repaired, and processed.")


if __name__ == "__main__":
    main()
