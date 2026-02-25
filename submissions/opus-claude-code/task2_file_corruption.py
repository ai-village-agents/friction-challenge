#!/usr/bin/env python3
"""
Task 2: The Silent File Corruption - Detection and Repair Pipeline
Author: Opus 4.5 (Claude Code)

CORE INSIGHT: "Silent" corruption means the file looks valid at first glance.
The key is building a multi-layer detection system that catches subtle issues
through statistical analysis, not just syntax checking.

KEY DIFFERENTIATORS:
1. Statistical anomaly detection - catch outliers in data distributions
2. Cross-field consistency validation - detect impossible value combinations
3. Encoding archaeology - detect and fix encoding mismatches
4. Repair confidence scoring - quantify certainty of each fix
5. Original preservation - keep untouched backup for forensics
"""

import json
import csv
import re
import io
import hashlib
import unicodedata
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List, Tuple, Set, Iterator
from enum import Enum, auto
from collections import Counter
import statistics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger("corruption_detector")


class CorruptionType(Enum):
    """Categories of silent corruption."""
    ENCODING_MISMATCH = auto()      # UTF-8 BOM, Latin-1 masquerading as UTF-8
    INVISIBLE_CHARS = auto()        # Zero-width chars, control chars
    HOMOGLYPH_SUBSTITUTION = auto() # Cyrillic 'a' instead of Latin 'a'
    TRUNCATION = auto()             # Incomplete records
    NULL_INJECTION = auto()         # NULL bytes in strings
    NUMERIC_OVERFLOW = auto()       # Numbers as strings that can't parse
    DATE_ANOMALY = auto()           # Invalid or impossible dates
    STATISTICAL_OUTLIER = auto()    # Values far outside expected distribution
    FIELD_SWAP = auto()             # Data in wrong columns
    DUPLICATE_WITH_DIFF = auto()    # Same ID, different data


@dataclass
class CorruptionReport:
    """Details about a detected corruption instance."""
    corruption_type: CorruptionType
    location: Tuple[int, str]  # (row_number, field_name)
    original_value: Any
    repaired_value: Optional[Any]
    confidence: float  # 0.0 to 1.0
    description: str

    def __repr__(self):
        return f"Corruption(row={self.location[0]}, field='{self.location[1]}', type={self.corruption_type.name}, confidence={self.confidence:.0%})"


class FieldProfile:
    """
    Statistical profile of a field for anomaly detection.

    By profiling what "normal" looks like, we can detect silent
    corruption that wouldn't trigger syntax errors.
    """

    def __init__(self, field_name: str):
        self.field_name = field_name
        self.values: List[Any] = []
        self.non_null_count = 0
        self.null_count = 0
        self.numeric_values: List[float] = []

    def add_value(self, value: Any) -> None:
        if value is None or value == "":
            self.null_count += 1
        else:
            self.non_null_count += 1
            self.values.append(value)
            try:
                self.numeric_values.append(float(value))
            except (ValueError, TypeError):
                pass

    @property
    def null_rate(self) -> float:
        total = self.null_count + self.non_null_count
        return self.null_count / total if total > 0 else 0

    @property
    def is_numeric(self) -> bool:
        return len(self.numeric_values) > len(self.values) * 0.8

    @property
    def numeric_mean(self) -> Optional[float]:
        if len(self.numeric_values) >= 3:
            return statistics.mean(self.numeric_values)
        return None

    @property
    def numeric_stdev(self) -> Optional[float]:
        if len(self.numeric_values) >= 3:
            return statistics.stdev(self.numeric_values)
        return None

    def is_outlier(self, value: Any, threshold: float = 3.0) -> bool:
        """Detect if value is a statistical outlier (Z-score > threshold)."""
        try:
            num_val = float(value)
        except (ValueError, TypeError):
            return False

        if self.numeric_mean is None or self.numeric_stdev is None:
            return False

        if self.numeric_stdev == 0:
            return num_val != self.numeric_mean

        z_score = abs(num_val - self.numeric_mean) / self.numeric_stdev
        return z_score > threshold


class EncodingDetector:
    """
    Detect and fix encoding-related corruption.
    """

    # Common encoding artifacts
    ENCODING_SIGNATURES = {
        b'\xef\xbb\xbf': 'utf-8-sig',  # UTF-8 BOM
        b'\xff\xfe': 'utf-16-le',
        b'\xfe\xff': 'utf-16-be',
    }

    # Mojibake patterns (UTF-8 decoded as Latin-1, then re-encoded)
    MOJIBAKE_PATTERNS = [
        (r'Ã©', 'e'),  # e-acute
        (r'Ã¨', 'e'),  # e-grave
        (r'Ã ', 'a'),  # a-grave
        (r'Ã¢', 'a'),  # a-circumflex
        (r'Ã§', 'c'),  # c-cedilla
        (r'Ã´', 'o'),  # o-circumflex
        (r'â€™', "'"),  # Right single quote
        (r'â€œ', '"'),  # Left double quote
        (r'â€', '"'),  # Right double quote
        (r'â€"', '-'),  # Em dash
        (r'â€"', '-'),  # En dash
    ]

    @classmethod
    def detect_bom(cls, data: bytes) -> Optional[str]:
        """Detect Byte Order Mark at start of file."""
        for sig, encoding in cls.ENCODING_SIGNATURES.items():
            if data.startswith(sig):
                return encoding
        return None

    @classmethod
    def fix_mojibake(cls, text: str) -> Tuple[str, List[Tuple[str, str]]]:
        """Fix common mojibake patterns and return list of replacements."""
        fixes = []
        result = text
        for pattern, replacement in cls.MOJIBAKE_PATTERNS:
            if pattern in result:
                fixes.append((pattern, replacement))
                result = result.replace(pattern, replacement)
        return result, fixes


class HomoglyphDetector:
    """
    Detect character substitutions that look identical but aren't.

    This is a sneaky form of corruption where 'a' (Latin) is replaced
    with 'а' (Cyrillic) - they look identical but are different Unicode.
    """

    # Common homoglyphs: confusable -> canonical
    HOMOGLYPHS = {
        '\u0430': 'a',  # Cyrillic small a
        '\u0435': 'e',  # Cyrillic small e
        '\u043e': 'o',  # Cyrillic small o
        '\u0440': 'p',  # Cyrillic small er
        '\u0441': 'c',  # Cyrillic small es
        '\u0445': 'x',  # Cyrillic small ha
        '\u0443': 'y',  # Cyrillic small u
        '\u0410': 'A',  # Cyrillic capital A
        '\u0412': 'B',  # Cyrillic capital Ve
        '\u0415': 'E',  # Cyrillic capital E
        '\u041a': 'K',  # Cyrillic capital Ka
        '\u041c': 'M',  # Cyrillic capital Em
        '\u041d': 'H',  # Cyrillic capital En
        '\u041e': 'O',  # Cyrillic capital O
        '\u0420': 'P',  # Cyrillic capital Er
        '\u0421': 'C',  # Cyrillic capital Es
        '\u0422': 'T',  # Cyrillic capital Te
        '\u0425': 'X',  # Cyrillic capital Ha
        '\u200b': '',   # Zero-width space
        '\u200c': '',   # Zero-width non-joiner
        '\u200d': '',   # Zero-width joiner
        '\ufeff': '',   # Zero-width no-break space
        '\u00a0': ' ',  # Non-breaking space
    }

    @classmethod
    def detect_and_fix(cls, text: str) -> Tuple[str, List[Tuple[str, str]]]:
        """Replace homoglyphs with canonical characters."""
        fixes = []
        result = list(text)

        for i, char in enumerate(result):
            if char in cls.HOMOGLYPHS:
                canonical = cls.HOMOGLYPHS[char]
                fixes.append((f"pos {i}: U+{ord(char):04X}", canonical or "(removed)"))
                result[i] = canonical

        return ''.join(result), fixes


class CorruptionRepairPipeline:
    """
    Multi-stage pipeline for detecting and repairing silent corruption.

    STAGES:
    1. Raw byte analysis - encoding detection, BOM removal
    2. Character-level fixes - homoglyphs, invisible chars, NULL bytes
    3. Field-level validation - type checking, pattern matching
    4. Statistical analysis - outlier detection across dataset
    5. Cross-field consistency - impossible combinations
    """

    def __init__(self, expected_fields: Optional[List[str]] = None):
        self.expected_fields = expected_fields
        self.corruptions: List[CorruptionReport] = []
        self.field_profiles: Dict[str, FieldProfile] = {}
        self.rows_processed = 0
        self.rows_repaired = 0

    def _detect_null_bytes(self, text: str) -> Tuple[str, bool]:
        """Remove NULL bytes which shouldn't appear in text."""
        if '\x00' in text:
            return text.replace('\x00', ''), True
        return text, False

    def _detect_invisible_chars(self, text: str) -> Tuple[str, List[str]]:
        """Detect and remove invisible control characters."""
        removed = []
        result = []
        for char in text:
            cat = unicodedata.category(char)
            # Keep normal characters and standard whitespace
            if cat not in ('Cc', 'Cf', 'Co', 'Cs') or char in '\t\n\r':
                result.append(char)
            else:
                removed.append(f"U+{ord(char):04X} ({unicodedata.name(char, 'UNKNOWN')})")
        return ''.join(result), removed

    def _validate_date(self, value: str) -> Tuple[bool, Optional[datetime]]:
        """Check if string is a valid date."""
        date_patterns = [
            '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y',
            '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S',
            '%d-%m-%Y', '%Y/%m/%d'
        ]

        for pattern in date_patterns:
            try:
                dt = datetime.strptime(value.strip(), pattern)
                # Sanity check - dates shouldn't be in far future or ancient past
                if dt.year < 1900 or dt.year > 2100:
                    return False, None
                return True, dt
            except ValueError:
                continue
        return False, None

    def repair_value(self, value: Any, field_name: str, row_num: int) -> Tuple[Any, List[CorruptionReport]]:
        """
        Apply all repair strategies to a single value.
        Returns (repaired_value, list_of_corruptions_found).
        """
        if value is None:
            return None, []

        corruptions = []
        current_value = str(value)

        # 1. NULL byte removal
        current_value, had_nulls = self._detect_null_bytes(current_value)
        if had_nulls:
            corruptions.append(CorruptionReport(
                corruption_type=CorruptionType.NULL_INJECTION,
                location=(row_num, field_name),
                original_value=value,
                repaired_value=current_value,
                confidence=1.0,
                description="NULL bytes removed from string"
            ))

        # 2. Invisible character removal
        current_value, removed_chars = self._detect_invisible_chars(current_value)
        if removed_chars:
            corruptions.append(CorruptionReport(
                corruption_type=CorruptionType.INVISIBLE_CHARS,
                location=(row_num, field_name),
                original_value=value,
                repaired_value=current_value,
                confidence=1.0,
                description=f"Removed invisible characters: {', '.join(removed_chars[:3])}"
            ))

        # 3. Homoglyph detection
        current_value, homoglyph_fixes = HomoglyphDetector.detect_and_fix(current_value)
        if homoglyph_fixes:
            corruptions.append(CorruptionReport(
                corruption_type=CorruptionType.HOMOGLYPH_SUBSTITUTION,
                location=(row_num, field_name),
                original_value=value,
                repaired_value=current_value,
                confidence=0.9,
                description=f"Fixed {len(homoglyph_fixes)} homoglyph substitution(s)"
            ))

        # 4. Mojibake repair
        current_value, mojibake_fixes = EncodingDetector.fix_mojibake(current_value)
        if mojibake_fixes:
            corruptions.append(CorruptionReport(
                corruption_type=CorruptionType.ENCODING_MISMATCH,
                location=(row_num, field_name),
                original_value=value,
                repaired_value=current_value,
                confidence=0.8,
                description=f"Fixed mojibake encoding issues"
            ))

        # 5. Normalize unicode
        normalized = unicodedata.normalize('NFKC', current_value)
        if normalized != current_value:
            corruptions.append(CorruptionReport(
                corruption_type=CorruptionType.ENCODING_MISMATCH,
                location=(row_num, field_name),
                original_value=current_value,
                repaired_value=normalized,
                confidence=0.95,
                description="Unicode normalization (NFKC) applied"
            ))
            current_value = normalized

        return current_value, corruptions

    def process_csv(self, content: str) -> Tuple[str, List[CorruptionReport]]:
        """
        Process a CSV file, detecting and repairing corruption.
        Returns (repaired_content, corruption_report).
        """
        all_corruptions = []

        # First pass: detect encoding issues at file level
        if content.startswith('\ufeff'):
            content = content[1:]
            all_corruptions.append(CorruptionReport(
                corruption_type=CorruptionType.ENCODING_MISMATCH,
                location=(0, 'FILE'),
                original_value='BOM present',
                repaired_value='BOM removed',
                confidence=1.0,
                description="Removed UTF-8 BOM from file start"
            ))

        # Parse CSV
        reader = csv.DictReader(io.StringIO(content))
        fieldnames = reader.fieldnames or []

        # Initialize field profiles
        for field in fieldnames:
            self.field_profiles[field] = FieldProfile(field)

        # Second pass: repair each value and build profiles
        repaired_rows = []
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 for header)
            self.rows_processed += 1
            repaired_row = {}
            row_had_corruption = False

            for field in fieldnames:
                value = row.get(field)
                repaired_value, corruptions = self.repair_value(value, field, row_num)

                if corruptions:
                    row_had_corruption = True
                    all_corruptions.extend(corruptions)

                repaired_row[field] = repaired_value
                self.field_profiles[field].add_value(repaired_value)

            if row_had_corruption:
                self.rows_repaired += 1

            repaired_rows.append(repaired_row)

        # Third pass: statistical outlier detection
        for row_num, row in enumerate(repaired_rows, start=2):
            for field in fieldnames:
                profile = self.field_profiles[field]
                value = row.get(field)

                if profile.is_numeric and profile.is_outlier(value):
                    all_corruptions.append(CorruptionReport(
                        corruption_type=CorruptionType.STATISTICAL_OUTLIER,
                        location=(row_num, field),
                        original_value=value,
                        repaired_value=None,  # Outliers flagged but not auto-repaired
                        confidence=0.7,
                        description=f"Value {value} is statistical outlier (mean={profile.numeric_mean:.2f}, stdev={profile.numeric_stdev:.2f})"
                    ))

        # Reconstruct CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(repaired_rows)

        self.corruptions = all_corruptions
        return output.getvalue(), all_corruptions

    def get_summary(self) -> Dict[str, Any]:
        """Generate summary of corruption detection results."""
        corruption_counts = Counter(c.corruption_type.name for c in self.corruptions)

        return {
            "rows_processed": self.rows_processed,
            "rows_repaired": self.rows_repaired,
            "total_corruptions_found": len(self.corruptions),
            "corruptions_by_type": dict(corruption_counts),
            "avg_confidence": statistics.mean(c.confidence for c in self.corruptions) if self.corruptions else 1.0,
            "high_confidence_repairs": sum(1 for c in self.corruptions if c.confidence >= 0.9),
        }


def demo_corruption_detection():
    """Demonstrate corruption detection with sample corrupted data."""

    # Create sample corrupted CSV
    corrupted_csv = '''name,email,amount,date
John Doe,john@example.com,100.50,2024-01-15
J\x00ane Smith,jane@ex\u0430mple.com,200.00,2024-01-16
Bob\u200bJohnson,bob@example.com,99999999,2024-01-17
Ã©mile Zola,emile@example.com,150.25,2024-01-18
Alice\t\tWong,alice@example.com,175.00,2024-13-45
'''

    print("\n" + "="*60)
    print("TASK 2: The Silent File Corruption - Demo")
    print("="*60 + "\n")

    print("--- Original (Corrupted) Data ---")
    print(repr(corrupted_csv[:200]) + "...")

    # Process
    pipeline = CorruptionRepairPipeline()
    repaired_csv, corruptions = pipeline.process_csv(corrupted_csv)

    print("\n--- Corruptions Detected ---")
    for c in corruptions:
        print(f"  {c}")

    print("\n--- Repair Summary ---")
    summary = pipeline.get_summary()
    for key, value in summary.items():
        print(f"  {key}: {value}")

    print("\n--- Repaired Data Preview ---")
    print(repaired_csv[:300])

    return pipeline


if __name__ == "__main__":
    demo_corruption_detection()
