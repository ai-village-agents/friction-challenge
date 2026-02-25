#!/usr/bin/env python3
"""
Task 2: The Silent File Corruption
===================================
Problem: Data file has subtle corruption (NULL bytes, homoglyphs, invisible chars, 
invalid dates, out-of-range values) that isn't obvious. Must detect, repair, and process.

Corruption types:
- NULL bytes (0x00) embedded in strings
- Unicode homoglyphs (Cyrillic А replacing Latin A)
- Invisible Unicode control chars (zero-width joiners)
- Invalid calendar dates (2026-02-30)
- Out-of-range values (9999.9°C for temperature)

Workaround: 4-phase pipeline - Scan → Repair → Validate → Process
"""

import json
import unicodedata
import re
from datetime import datetime
from io import StringIO


def create_corrupted_file():
    """Creates a CSV-like data file with various corruptions."""
    corrupted_data = [
        "sensor_id,timestamp,temperature,humidity",
        "SENSOR001\x002A,2026-02-25T10:30:00,23.5,45",  # NULL byte
        "SENSOR002,2026-02-25T10:31:00,24.1,46",
        "SЕNSOR003,2026-02-25T10:32:00,22.9,44",  # Cyrillic Е (U+0415)
        "SENSOR004,2026‌02-25T10:33:00,23.7,47",  # Zero-width joiner U+200C
        "SENSOR005,2026-02-30T10:34:00,25.0,48",  # Invalid date (Feb 30)
        "SENSOR006,2026-02-25T10:35:00,9999.9,50",  # Out-of-range temp
        "SENSOR007,2026-02-25T10:36:00,23.2,49",
        "SENSOR008,2026-02-25T10:37:00,23.8,43",
    ]
    return "\n".join(corrupted_data)


def scan_corruption(lines):
    """Identify corruption patterns in data."""
    issues = {}
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        
        # Check for NULL bytes
        if '\x00' in line:
            issues[i] = issues.get(i, []) + ["null_byte"]
        
        # Check for invisible Unicode characters
        for j, char in enumerate(line):
            cat = unicodedata.category(char)
            if cat.startswith('C'):  # Control/format category
                issues[i] = issues.get(i, []) + ["control_char"]
                break
        
        # Check for homoglyphs (normalize and compare)
        normalized = unicodedata.normalize('NFKC', line)
        if normalized != line:
            issues[i] = issues.get(i, []) + ["homoglyph"]
    
    return issues


def repair_corruption(line):
    """Repair known corruption patterns."""
    # Remove NULL bytes
    line = line.replace('\x00', '')
    
    # Normalize Unicode (converts Cyrillic to Latin, removes invisible chars)
    line = unicodedata.normalize('NFKC', line)
    
    # Remove zero-width characters
    line = re.sub(r'[\u200B-\u200D\u2060\uFEFF]', '', line)
    
    return line.strip()


def validate_row(row_dict, row_num):
    """Validate individual row for semantic errors."""
    issues = []
    
    # Validate sensor_id format
    if 'sensor_id' in row_dict:
        if not re.match(r'^SENSOR\d{3}$', row_dict['sensor_id']):
            issues.append(f"invalid_sensor_id: {row_dict['sensor_id']}")
    
    # Validate timestamp format
    if 'timestamp' in row_dict:
        try:
            datetime.fromisoformat(row_dict['timestamp'].replace('Z', '+00:00'))
        except ValueError:
            issues.append(f"invalid_timestamp: {row_dict['timestamp']}")
    
    # Validate temperature range (-50°C to 60°C for sensors)
    if 'temperature' in row_dict:
        try:
            temp = float(row_dict['temperature'])
            if temp < -50 or temp > 60:
                issues.append(f"out_of_range_temperature: {temp}°C")
        except ValueError:
            issues.append(f"unparseable_temperature: {row_dict['temperature']}")
    
    # Validate humidity range (0-100%)
    if 'humidity' in row_dict:
        try:
            humidity = float(row_dict['humidity'])
            if humidity < 0 or humidity > 100:
                issues.append(f"out_of_range_humidity: {humidity}%")
        except ValueError:
            issues.append(f"unparseable_humidity: {row_dict['humidity']}")
    
    return issues


def process_file(content):
    """Process corrupted file with repair pipeline."""
    lines = content.split('\n')
    
    # Phase 1: Scan for corruption
    print("Phase 1: Scanning for corruption...")
    corruption_map = scan_corruption(lines)
    if corruption_map:
        print(f"  Found issues in {len(corruption_map)} lines: {corruption_map}")
    else:
        print("  No obvious corruption detected")
    
    # Phase 2: Repair
    print("\nPhase 2: Repairing...")
    repaired_lines = [repair_corruption(line) for line in lines]
    
    # Phase 3: Re-validate and extract rows
    print("\nPhase 3: Validating and extracting rows...")
    header = repaired_lines[0].split(',')
    rows = []
    issues_by_row = {}
    
    for i, line in enumerate(repaired_lines[1:], start=1):
        if not line.strip():
            continue
        
        fields = line.split(',')
        if len(fields) != len(header):
            print(f"  Row {i}: Field count mismatch ({len(fields)} vs {len(header)})")
            continue
        
        row_dict = dict(zip(header, fields))
        issues = validate_row(row_dict, i)
        
        if issues:
            issues_by_row[i] = issues
            print(f"  Row {i}: {'; '.join(issues)}")
        else:
            rows.append(row_dict)
            print(f"  Row {i}: ✓ Valid")
    
    # Phase 4: Summary
    print(f"\nPhase 4: Processing summary")
    print(f"  Total rows: {len(repaired_lines) - 1}")
    print(f"  Valid rows: {len(rows)}")
    print(f"  Invalid rows: {len(issues_by_row)}")
    print(f"  Success rate: {100.0 * len(rows) / max(1, len(repaired_lines) - 1):.1f}%")
    
    return rows, issues_by_row


if __name__ == "__main__":
    print("Task 2: The Silent File Corruption")
    print("=" * 60)
    
    content = create_corrupted_file()
    print("Created corrupted file with multiple corruption types\n")
    
    rows, issues = process_file(content)
    
    print("\n" + "=" * 60)
    print(f"Successfully processed {len(rows)} valid rows:")
    for row in rows:
        print(f"  {row}")
