#!/usr/bin/env python3
"""
Task 2: The Silent File Corruption - Detection and Repair Pipeline
Author: Claude Opus 4.5

PHILOSOPHY: Corruption is subtle because it's designed to slip past naive validation.
The key insight is that we must validate at MULTIPLE LEVELS:
1. Byte-level (null bytes, control characters)
2. Encoding-level (homoglyphs, normalization)
3. Semantic-level (valid dates, reasonable ranges)
4. Structural-level (row consistency, field counts)

This implementation uses a 4-PHASE PIPELINE:
  SCAN -> REPAIR -> VALIDATE -> PROCESS

Each phase is independent and observable, allowing diagnosis of WHERE
corruption was detected and HOW it was resolved.
"""

import csv
import io
import re
import unicodedata
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple, Set, Callable
from enum import Enum, auto
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class CorruptionType(Enum):
    """Categories of corruption we can detect and repair."""
    NULL_BYTES = auto()           # \x00 bytes embedded in text
    INVISIBLE_CHARS = auto()      # Zero-width chars, direction overrides
    HOMOGLYPHS = auto()          # Cyrillic 'а' instead of Latin 'a'
    INVALID_DATE = auto()        # Feb 30, month 13, etc.
    OUT_OF_RANGE = auto()        # Age=999, price=-50
    ENCODING_ERROR = auto()      # Invalid UTF-8 sequences
    FIELD_COUNT_MISMATCH = auto() # Wrong number of columns
    TRAILING_WHITESPACE = auto()  # Subtle but can break exact matching


@dataclass
class CorruptionReport:
    """Detailed report of a corruption instance."""
    row_number: int
    column: Optional[str]
    corruption_type: CorruptionType
    original_value: str
    repaired_value: Optional[str]
    confidence: float  # 0.0 to 1.0 - how confident we are in the repair
    repairable: bool


@dataclass
class ProcessingResult:
    """Complete result of file processing."""
    success: bool
    rows_processed: int
    rows_quarantined: int
    corruptions_found: List[CorruptionReport]
    corruptions_repaired: int
    clean_data: List[Dict[str, Any]]
    quarantine_data: List[Dict[str, Any]]
    warnings: List[str]


# =============================================================================
# CORRUPTION SIMULATORS - Models realistic corruption scenarios
# =============================================================================

class CorruptedFileGenerator:
    """
    Generate test files with realistic corruption patterns.
    
    These corruptions model real-world scenarios:
    - Database exports with encoding issues
    - Copy-paste from documents with invisible formatting
    - OCR errors introducing homoglyphs
    - Manual data entry errors (invalid dates)
    - System glitches inserting null bytes
    """
    
    @staticmethod
    def generate_sample_csv() -> str:
        """Generate a corrupted CSV for testing."""
        # Clean header
        lines = ["id,name,date,amount,status"]
        
        # Row 1: Clean
        lines.append("1,Alice Johnson,2024-03-15,150.00,active")
        
        # Row 2: NULL byte in name
        lines.append("2,Bob\x00Smith,2024-03-16,200.50,active")
        
        # Row 3: Homoglyph - Cyrillic 'а' (U+0430) instead of Latin 'a'
        lines.append("3,С\u0430rl Davis,2024-03-17,175.25,pending")  # Cyrillic С and а
        
        # Row 4: Invalid date (Feb 30)
        lines.append("4,Diana Evans,2024-02-30,300.00,active")
        
        # Row 5: Invisible characters (zero-width space U+200B)
        lines.append("5,Eve\u200bFrank,2024-03-18,125.75,completed")
        
        # Row 6: Out of range value
        lines.append("6,Frank Green,2024-03-19,-500.00,active")
        
        # Row 7: Unicode direction override (dangerous!)
        lines.append("7,Grace\u202eHill,2024-03-20,450.00,pending")
        
        # Row 8: Invalid date (month 13)
        lines.append("8,Henry Irving,2024-13-01,275.00,active")
        
        # Row 9: Trailing whitespace (subtle)
        lines.append("9,Ivy James,2024-03-21,180.00,active   ")
        
        # Row 10: Clean
        lines.append("10,Jack King,2024-03-22,220.00,completed")
        
        return "\n".join(lines)


# =============================================================================
# DETECTION ENGINE - Multi-layer corruption scanner
# =============================================================================

class CorruptionDetector:
    """
    Multi-layer corruption detection engine.
    
    WHY multi-layer: Each corruption type requires different detection logic.
    A null byte scanner won't catch homoglyphs, and a date validator won't
    catch invisible characters. We need ALL checks running together.
    """
    
    # Characters that should never appear in normal text data
    DANGEROUS_CHARS = {
        '\x00',         # NULL byte
        '\u200b',       # Zero-width space
        '\u200c',       # Zero-width non-joiner
        '\u200d',       # Zero-width joiner
        '\ufeff',       # BOM / zero-width no-break space
        '\u202a',       # Left-to-right embedding
        '\u202b',       # Right-to-left embedding
        '\u202c',       # Pop directional formatting
        '\u202d',       # Left-to-right override
        '\u202e',       # Right-to-left override
        '\u2060',       # Word joiner
        '\u2061',       # Function application (invisible)
        '\u2062',       # Invisible times
        '\u2063',       # Invisible separator
        '\u2064',       # Invisible plus
    }
    
    # Common homoglyph mappings (confusable characters)
    HOMOGLYPH_MAP = {
        '\u0430': 'a',  # Cyrillic а -> Latin a
        '\u0435': 'e',  # Cyrillic е -> Latin e
        '\u043e': 'o',  # Cyrillic о -> Latin o
        '\u0440': 'p',  # Cyrillic р -> Latin p
        '\u0441': 'c',  # Cyrillic с -> Latin c
        '\u0443': 'y',  # Cyrillic у -> Latin y
        '\u0445': 'x',  # Cyrillic х -> Latin x
        '\u0410': 'A',  # Cyrillic А -> Latin A
        '\u0412': 'B',  # Cyrillic В -> Latin B
        '\u0415': 'E',  # Cyrillic Е -> Latin E
        '\u041a': 'K',  # Cyrillic К -> Latin K
        '\u041c': 'M',  # Cyrillic М -> Latin M
        '\u041d': 'H',  # Cyrillic Н -> Latin H
        '\u041e': 'O',  # Cyrillic О -> Latin O
        '\u0420': 'P',  # Cyrillic Р -> Latin P
        '\u0421': 'C',  # Cyrillic С -> Latin C
        '\u0422': 'T',  # Cyrillic Т -> Latin T
        '\u0425': 'X',  # Cyrillic Х -> Latin X
    }
    
    @classmethod
    def scan_value(cls, value: str, column: str, row_num: int) -> List[CorruptionReport]:
        """Scan a single value for all corruption types."""
        reports = []
        
        # Check for NULL bytes
        if '\x00' in value:
            reports.append(CorruptionReport(
                row_number=row_num,
                column=column,
                corruption_type=CorruptionType.NULL_BYTES,
                original_value=repr(value),
                repaired_value=value.replace('\x00', ''),
                confidence=1.0,
                repairable=True
            ))
        
        # Check for invisible/dangerous characters
        for char in value:
            if char in cls.DANGEROUS_CHARS:
                clean = ''.join(c for c in value if c not in cls.DANGEROUS_CHARS)
                reports.append(CorruptionReport(
                    row_number=row_num,
                    column=column,
                    corruption_type=CorruptionType.INVISIBLE_CHARS,
                    original_value=repr(value),
                    repaired_value=clean,
                    confidence=1.0,
                    repairable=True
                ))
                break  # One report per value for invisible chars
        
        # Check for homoglyphs
        has_homoglyph = any(c in cls.HOMOGLYPH_MAP for c in value)
        if has_homoglyph:
            repaired = ''.join(cls.HOMOGLYPH_MAP.get(c, c) for c in value)
            reports.append(CorruptionReport(
                row_number=row_num,
                column=column,
                corruption_type=CorruptionType.HOMOGLYPHS,
                original_value=repr(value),
                repaired_value=repaired,
                confidence=0.9,  # Slightly lower - might be intentional
                repairable=True
            ))
        
        # Check for trailing/leading whitespace
        stripped = value.strip()
        if stripped != value:
            reports.append(CorruptionReport(
                row_number=row_num,
                column=column,
                corruption_type=CorruptionType.TRAILING_WHITESPACE,
                original_value=repr(value),
                repaired_value=stripped,
                confidence=0.95,
                repairable=True
            ))
        
        return reports
    
    @classmethod
    def validate_date(cls, value: str, row_num: int, column: str) -> Optional[CorruptionReport]:
        """
        Validate date format and semantic correctness.
        
        WHY semantic validation: "2024-02-30" is syntactically valid ISO format
        but semantically impossible. Naive parsers might accept it.
        """
        # Try to parse as ISO date
        date_pattern = r'^(\d{4})-(\d{2})-(\d{2})$'
        match = re.match(date_pattern, value.strip())
        
        if not match:
            return None  # Not a date field
        
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
        
        # Check semantic validity
        if month < 1 or month > 12:
            return CorruptionReport(
                row_number=row_num,
                column=column,
                corruption_type=CorruptionType.INVALID_DATE,
                original_value=value,
                repaired_value=None,
                confidence=1.0,
                repairable=False  # Can't guess correct month
            )
        
        # Days in each month (handling leap years)
        is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        days_in_month = [31, 29 if is_leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        
        if day < 1 or day > days_in_month[month - 1]:
            # Try to repair obvious cases
            repaired = None
            if day > days_in_month[month - 1]:
                # Cap to max day of month
                repaired = f"{year:04d}-{month:02d}-{days_in_month[month-1]:02d}"
            
            return CorruptionReport(
                row_number=row_num,
                column=column,
                corruption_type=CorruptionType.INVALID_DATE,
                original_value=value,
                repaired_value=repaired,
                confidence=0.7 if repaired else 0.0,
                repairable=repaired is not None
            )
        
        return None
    
    @classmethod
    def validate_numeric(cls, value: str, row_num: int, column: str,
                        min_val: Optional[float] = None,
                        max_val: Optional[float] = None) -> Optional[CorruptionReport]:
        """Validate numeric values are within acceptable range."""
        try:
            num = float(value)
        except ValueError:
            return None  # Not a numeric field
        
        # Check for obviously wrong values
        if min_val is not None and num < min_val:
            return CorruptionReport(
                row_number=row_num,
                column=column,
                corruption_type=CorruptionType.OUT_OF_RANGE,
                original_value=value,
                repaired_value=None,  # Can't guess correct value
                confidence=1.0,
                repairable=False
            )
        
        if max_val is not None and num > max_val:
            return CorruptionReport(
                row_number=row_num,
                column=column,
                corruption_type=CorruptionType.OUT_OF_RANGE,
                original_value=value,
                repaired_value=None,
                confidence=1.0,
                repairable=False
            )
        
        return None


# =============================================================================
# REPAIR ENGINE - Applies fixes based on detection results
# =============================================================================

class RepairEngine:
    """
    Applies repairs to corrupted data.
    
    PHILOSOPHY: Not all corruptions are repairable. We maintain a clear
    distinction between:
    1. Confidently repairable (null bytes, invisible chars)
    2. Probably repairable (homoglyphs, whitespace)
    3. Not repairable (invalid dates, out of range)
    
    Unrepairable rows go to quarantine for human review.
    """
    
    @staticmethod
    def apply_repairs(value: str, reports: List[CorruptionReport]) -> Tuple[str, bool]:
        """
        Apply repairs from corruption reports.
        Returns (repaired_value, all_repaired).
        """
        result = value
        all_repaired = True
        
        for report in reports:
            if report.repairable and report.repaired_value is not None:
                # Apply repair - note we apply in sequence
                result = report.repaired_value
            else:
                all_repaired = False
        
        return result, all_repaired
    
    @staticmethod
    def normalize_unicode(value: str) -> str:
        """
        Apply NFKC normalization to catch remaining homoglyphs.
        
        WHY NFKC: It's the most aggressive normalization form.
        - NFC: Canonical composition
        - NFD: Canonical decomposition  
        - NFKC: Compatibility composition (maps ① to 1, ｆｕｌｌ to full)
        - NFKD: Compatibility decomposition
        
        NFKC catches the most sneaky substitutions.
        """
        return unicodedata.normalize('NFKC', value)


# =============================================================================
# PROCESSING PIPELINE - Orchestrates scan -> repair -> validate -> process
# =============================================================================

class FileProcessor:
    """
    Complete file processing pipeline with corruption handling.
    
    The 4-phase approach:
    1. SCAN: Detect all corruptions without modifying data
    2. REPAIR: Apply fixes to repairable issues
    3. VALIDATE: Re-check repaired data for remaining issues
    4. PROCESS: Output clean data, quarantine problematic rows
    """
    
    def __init__(self, numeric_ranges: Optional[Dict[str, Tuple[float, float]]] = None):
        """
        Initialize processor.
        
        Args:
            numeric_ranges: Dict mapping column name to (min, max) tuple
        """
        self.numeric_ranges = numeric_ranges or {'amount': (0, 100000)}
        self.detector = CorruptionDetector()
        self.repairer = RepairEngine()
    
    def process(self, csv_content: str) -> ProcessingResult:
        """Process CSV content through the full pipeline."""
        
        all_reports: List[CorruptionReport] = []
        clean_rows: List[Dict[str, Any]] = []
        quarantine_rows: List[Dict[str, Any]] = []
        warnings: List[str] = []
        repairs_applied = 0
        
        # Parse CSV
        reader = csv.DictReader(io.StringIO(csv_content))
        headers = reader.fieldnames or []
        
        logger.info(f"Processing CSV with columns: {headers}")
        
        for row_num, row in enumerate(reader, start=2):  # Row 1 is header
            row_reports: List[CorruptionReport] = []
            row_repairable = True
            repaired_row = dict(row)
            
            # PHASE 1: SCAN each field
            for column, value in row.items():
                if value is None:
                    continue
                    
                # General text corruption checks
                field_reports = self.detector.scan_value(value, column, row_num)
                row_reports.extend(field_reports)
                
                # Date validation (for date columns)
                if 'date' in column.lower():
                    date_report = self.detector.validate_date(value, row_num, column)
                    if date_report:
                        row_reports.append(date_report)
                
                # Numeric range validation
                if column in self.numeric_ranges:
                    min_val, max_val = self.numeric_ranges[column]
                    range_report = self.detector.validate_numeric(
                        value, row_num, column, min_val, max_val
                    )
                    if range_report:
                        row_reports.append(range_report)
            
            # PHASE 2: REPAIR
            for column, value in row.items():
                if value is None:
                    continue
                
                # Get reports for this column
                column_reports = [r for r in row_reports if r.column == column]
                
                if column_reports:
                    repaired_value, fully_repaired = self.repairer.apply_repairs(
                        value, column_reports
                    )
                    # Also normalize
                    repaired_value = self.repairer.normalize_unicode(repaired_value)
                    repaired_row[column] = repaired_value
                    
                    if fully_repaired:
                        repairs_applied += len([r for r in column_reports if r.repairable])
                    else:
                        row_repairable = False
            
            # PHASE 3: VALIDATE - check if any unrepairable issues remain
            has_unrepairable = any(not r.repairable for r in row_reports)
            
            # PHASE 4: ROUTE - clean data or quarantine
            all_reports.extend(row_reports)
            
            if has_unrepairable:
                repaired_row['_quarantine_reason'] = [
                    f"{r.corruption_type.name}: {r.original_value}" 
                    for r in row_reports if not r.repairable
                ]
                quarantine_rows.append(repaired_row)
                logger.warning(f"Row {row_num}: Quarantined - unrepairable corruption")
            else:
                clean_rows.append(repaired_row)
                if row_reports:
                    logger.info(f"Row {row_num}: Repaired {len(row_reports)} corruption(s)")
        
        return ProcessingResult(
            success=True,
            rows_processed=len(clean_rows) + len(quarantine_rows),
            rows_quarantined=len(quarantine_rows),
            corruptions_found=all_reports,
            corruptions_repaired=repairs_applied,
            clean_data=clean_rows,
            quarantine_data=quarantine_rows,
            warnings=warnings
        )


# =============================================================================
# MAIN - Demonstration
# =============================================================================

def main():
    """Demonstrate the corruption detection and repair pipeline."""
    
    print("=" * 70)
    print("TASK 2: THE SILENT FILE CORRUPTION")
    print("Demonstrating multi-layer corruption detection and repair pipeline")
    print("=" * 70)
    print()
    
    # Generate corrupted test data
    csv_content = CorruptedFileGenerator.generate_sample_csv()
    
    print("INPUT DATA (with hidden corruptions):")
    print("-" * 70)
    for i, line in enumerate(csv_content.split('\n')):
        # Show repr to reveal hidden chars
        if i == 0:
            print(f"  {line}")
        else:
            print(f"  {line:50s} | repr: {repr(line)[:50]}...")
    print()
    
    # Process the file
    processor = FileProcessor(
        numeric_ranges={'amount': (0, 10000)}  # Amounts must be 0-10000
    )
    
    result = processor.process(csv_content)
    
    print("=" * 70)
    print("PROCESSING RESULTS")
    print("=" * 70)
    print(f"Rows processed: {result.rows_processed}")
    print(f"Corruptions found: {len(result.corruptions_found)}")
    print(f"Corruptions repaired: {result.corruptions_repaired}")
    print(f"Rows quarantined: {result.rows_quarantined}")
    print()
    
    print("CORRUPTION REPORT:")
    print("-" * 70)
    for report in result.corruptions_found:
        status = "✓ REPAIRED" if report.repairable else "✗ QUARANTINE"
        print(f"  Row {report.row_number}, {report.column}:")
        print(f"    Type: {report.corruption_type.name}")
        print(f"    Original: {report.original_value[:50]}")
        if report.repaired_value:
            print(f"    Repaired: {report.repaired_value[:50]}")
        print(f"    Confidence: {report.confidence:.0%}")
        print(f"    Status: {status}")
        print()
    
    print("CLEAN DATA:")
    print("-" * 70)
    for row in result.clean_data:
        print(f"  {row}")
    print()
    
    if result.quarantine_data:
        print("QUARANTINED DATA:")
        print("-" * 70)
        for row in result.quarantine_data:
            reason = row.pop('_quarantine_reason', 'Unknown')
            print(f"  {row}")
            print(f"    Reason: {reason}")
        print()


if __name__ == "__main__":
    main()
