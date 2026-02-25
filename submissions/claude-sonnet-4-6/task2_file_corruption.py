#!/usr/bin/env python3
"""
Task 2: The Silent File Corruption
====================================
Problem: A data file has been subtly corrupted in a way that is not immediately
obvious. We must identify the corruption, repair the file, and process it.

Corruption types demonstrated:
1. NULL bytes embedded mid-line (silent binary contamination)
2. Unicode homoglyphs replacing ASCII digits (e.g., '١' for '1', '２' for '2')
3. Invisible Unicode characters (zero-width space, BOM mid-line)
4. Numeric field with out-of-range "plausible" value (data integrity issue)
5. Off-by-one date (2025-02-30 — February 30th doesn't exist)

Diagnosis approach:
- Check for non-ASCII bytes in supposedly ASCII fields
- Verify numeric ranges against domain constraints
- Validate date fields for calendar correctness
- Detect invisible Unicode codepoints
- Cross-check row checksums if available

Workaround:
- Multi-pass sanitizer that identifies and logs each corruption
- Repair with documented substitutions (auditable fix log)
- Re-validate after repair before processing
"""

import csv
import io
import unicodedata
import datetime
import re

# ─── Corrupted Dataset ───────────────────────────────────────────────────────

# This is what the "data file" contains — subtle corruptions embedded
CORRUPTED_CSV = (
    "id,date,sensor,value,unit\n"
    "1,2026-01-15,temp_A,23.4,celsius\n"
    "2,2026-01-16,temp_B,2\u0032.1,celsius\n"       # homoglyph: '２' (fullwidth 2)
    "3,2026-01-17,temp_C,21\x00.8,celsius\n"         # NULL byte mid-field
    "4,2026-01-\u0031\u0038,temp_D,22.5,celsius\n"   # invisible homoglyph in date
    "5,2026-02-30,temp_E,24.0,celsius\n"             # Feb 30 doesn't exist
    "6,2026-01-20,temp_F,9999.9,celsius\n"           # out-of-range value
    "7,2026-01-21,temp_G,\u200b22.3,celsius\n"       # zero-width space before value
    "8,2026-01-22,temp_H,23.1,celsius\n"             # clean row
)

VALID_SENSOR_RANGE = (-50.0, 60.0)  # Celsius: physically plausible

# ─── Corruption Detector ─────────────────────────────────────────────────────

def detect_invisible_unicode(s: str) -> list:
    """Find invisible/confusable Unicode characters."""
    invisible = []
    for i, ch in enumerate(s):
        if ch != ' ' and unicodedata.category(ch) in ('Cf', 'Cc', 'Cs'):
            invisible.append((i, ch, unicodedata.name(ch, f'U+{ord(ch):04X}')))
        elif ord(ch) > 127:
            # Check for homoglyphs (characters that look like ASCII)
            name = unicodedata.name(ch, '')
            if any(word in name for word in ('DIGIT', 'FULL', 'LATIN SMALL', 'LATIN CAPITAL')):
                invisible.append((i, ch, name))
    return invisible

def scan_for_corruption(raw_csv: str) -> list:
    """Multi-layer scan returning list of (row, col, description, original_value)."""
    findings = []
    lines = raw_csv.split('\n')

    for line_num, line in enumerate(lines[1:], start=1):  # Skip header
        if not line.strip():
            continue
        fields = line.split(',')
        if len(fields) < 5:
            continue

        row_id, date_str, sensor, value_str, unit = fields[:5]

        # Check 1: NULL bytes anywhere in line
        if '\x00' in line:
            findings.append((line_num, 'line', 'NULL byte embedded', repr(line)))

        # Check 2: Invisible/homoglyph Unicode in each field
        for col_name, field in [('date', date_str), ('value', value_str), ('id', row_id)]:
            issues = detect_invisible_unicode(field)
            if issues:
                for pos, ch, name in issues:
                    findings.append((line_num, col_name,
                                     f'Non-ASCII char at pos {pos}: {name}',
                                     repr(field)))

        # Check 3: Date validity
        clean_date = ''.join(ch for ch in date_str if ord(ch) < 128)
        try:
            datetime.date.fromisoformat(clean_date)
        except ValueError as e:
            findings.append((line_num, 'date', f'Invalid date: {e}', date_str))

        # Check 4: Numeric range
        clean_value = ''.join(ch for ch in value_str if ord(ch) < 128).replace('\x00', '')
        try:
            v = float(clean_value)
            lo, hi = VALID_SENSOR_RANGE
            if not (lo <= v <= hi):
                findings.append((line_num, 'value',
                                  f'Out of range [{lo},{hi}]: {v}', value_str))
        except ValueError:
            findings.append((line_num, 'value', f'Cannot parse as float', value_str))

    return findings


# ─── Corruption Repairer ─────────────────────────────────────────────────────

def repair_field(field: str) -> str:
    """Remove null bytes, normalize Unicode homoglyphs to ASCII."""
    # Remove null bytes
    field = field.replace('\x00', '')
    # Remove invisible Unicode (zero-width, format chars)
    field = ''.join(ch for ch in field
                    if unicodedata.category(ch) not in ('Cf', 'Cc', 'Cs'))
    # Normalize Unicode digits/letters to ASCII equivalents
    normalized = unicodedata.normalize('NFKC', field)
    return normalized

def repair_csv(raw_csv: str, fix_log: list) -> str:
    """Repair the CSV, logging every fix applied."""
    lines = raw_csv.split('\n')
    repaired_lines = [lines[0]]  # Keep header

    for line_num, line in enumerate(lines[1:], start=1):
        if not line.strip():
            repaired_lines.append(line)
            continue

        fields = line.split(',')
        new_fields = []
        for i, field in enumerate(fields):
            repaired = repair_field(field)
            if repaired != field:
                fix_log.append(f"  Row {line_num}, col {i}: {repr(field)} → {repr(repaired)}")
            new_fields.append(repaired)

        # Date repair: if date is invalid, mark as INVALID for human review
        if len(new_fields) >= 2:
            try:
                datetime.date.fromisoformat(new_fields[1])
            except ValueError:
                original = new_fields[1]
                new_fields[1] = 'INVALID_DATE'
                fix_log.append(f"  Row {line_num}, date: {repr(original)} → 'INVALID_DATE' (flagged for review)")

        # Value range repair: clamp and flag
        if len(new_fields) >= 4:
            try:
                v = float(new_fields[3])
                lo, hi = VALID_SENSOR_RANGE
                if not (lo <= v <= hi):
                    original = v
                    new_fields[3] = 'INVALID_VALUE'
                    fix_log.append(f"  Row {line_num}, value: {original} → 'INVALID_VALUE' (out of range {lo}..{hi})")
            except ValueError:
                pass

        repaired_lines.append(','.join(new_fields))

    return '\n'.join(repaired_lines)


# ─── Processor ───────────────────────────────────────────────────────────────

def process_csv(clean_csv: str) -> dict:
    """Process the repaired CSV: compute per-sensor statistics."""
    reader = csv.DictReader(io.StringIO(clean_csv))
    valid_rows = []
    skipped = []

    for row in reader:
        if row['date'] == 'INVALID_DATE' or row['value'] == 'INVALID_VALUE':
            skipped.append(row)
            continue
        try:
            valid_rows.append({
                'id': int(row['id']),
                'date': row['date'],
                'sensor': row['sensor'],
                'value': float(row['value']),
                'unit': row['unit']
            })
        except (ValueError, KeyError):
            skipped.append(row)

    values = [r['value'] for r in valid_rows]
    return {
        'valid_rows': len(valid_rows),
        'skipped_rows': len(skipped),
        'mean_value': sum(values) / len(values) if values else None,
        'min_value': min(values) if values else None,
        'max_value': max(values) if values else None,
        'sensors': [r['sensor'] for r in valid_rows],
    }


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("TASK 2: The Silent File Corruption — Workaround Demo")
    print("=" * 60)
    print()

    # PHASE 1: Scan
    print("PHASE 1: Corruption Scan")
    print("-" * 40)
    findings = scan_for_corruption(CORRUPTED_CSV)
    if findings:
        print(f"Found {len(findings)} corruption(s):")
        for row, col, desc, val in findings:
            print(f"  Row {row}, field '{col}': {desc}")
            print(f"    Raw value: {val}")
    else:
        print("  No corruptions detected (unexpected)")
    print()

    # PHASE 2: Repair
    print("PHASE 2: Repair")
    print("-" * 40)
    fix_log = []
    repaired_csv = repair_csv(CORRUPTED_CSV, fix_log)
    if fix_log:
        print(f"Applied {len(fix_log)} fix(es):")
        for fix in fix_log:
            print(fix)
    print()

    # PHASE 3: Validate repaired file
    print("PHASE 3: Post-Repair Validation")
    print("-" * 40)
    post_findings = scan_for_corruption(repaired_csv)
    # Filter out the expected INVALID_ markers (those are intentional placeholders)
    real_issues = [f for f in post_findings
                   if 'INVALID_' not in str(f[3])]
    if real_issues:
        print(f"⚠️  {len(real_issues)} issue(s) remain after repair:")
        for f in real_issues:
            print(f"  {f}")
    else:
        print("✅ Repaired file passes validation")
    print()

    # PHASE 4: Process
    print("PHASE 4: Process Repaired Data")
    print("-" * 40)
    stats = process_csv(repaired_csv)
    print(f"  Valid rows processed: {stats['valid_rows']}")
    print(f"  Skipped (flagged):    {stats['skipped_rows']}")
    print(f"  Mean temperature:     {stats['mean_value']:.2f}°C")
    print(f"  Range:                {stats['min_value']:.1f}°C – {stats['max_value']:.1f}°C")
    print(f"  Sensors:              {', '.join(stats['sensors'])}")
    print()
    print("✅ Processing complete. Corrupted rows were flagged, not silently dropped.")

if __name__ == "__main__":
    main()
