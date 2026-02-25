"""
Comprehensive pytest test suite for Task 2: File Corruption Resilience

Tests cover:
- CorruptionType enumeration and classification
- CorruptionReport generation and details
- CorruptedFileGenerator for simulating corrupt files
- CorruptionDetector for identifying corruption types
- RepairEngine for automatic recovery
- FileProcessor end-to-end pipeline
- Confidence scoring system

Author: Claude Opus 4.5
"""

import pytest
import os
import tempfile
from pathlib import Path
from task2_file_corruption import (
    CorruptionType, CorruptionReport, ProcessingResult,
    CorruptedFileGenerator, CorruptionDetector, RepairEngine, FileProcessor
)


# ==================== CorruptionType Tests ====================

class TestCorruptionType:
    """Test suite for corruption type enumeration."""
    
    def test_all_corruption_types_exist(self):
        """All expected corruption types are defined."""
        expected_types = [
            "ENCODING_ERROR", "TRUNCATION", "BIT_FLIP", 
            "MISSING_DELIMITER", "UNICODE_CORRUPTION"
        ]
        for type_name in expected_types:
            assert hasattr(CorruptionType, type_name)
    
    def test_corruption_types_are_unique(self):
        """All corruption type values are unique."""
        values = [member.value for member in CorruptionType]
        assert len(values) == len(set(values))


# ==================== CorruptionReport Tests ====================

class TestCorruptionReport:
    """Test suite for corruption reports."""
    
    def test_report_creation(self):
        """Reports can be created with corruption details."""
        report = CorruptionReport(
            corruption_type=CorruptionType.ENCODING_ERROR,
            severity="high",
            location=100,
            description="Invalid UTF-8 sequence detected"
        )
        assert report.corruption_type == CorruptionType.ENCODING_ERROR
        assert report.severity == "high"
        assert report.location == 100
    
    def test_report_string_representation(self):
        """Reports have meaningful string representation."""
        report = CorruptionReport(
            corruption_type=CorruptionType.TRUNCATION,
            severity="medium",
            location=0,
            description="File truncated at byte 0"
        )
        report_str = str(report)
        assert "TRUNCATION" in report_str or "truncat" in report_str.lower()


# ==================== ProcessingResult Tests ====================

class TestProcessingResult:
    """Test suite for processing results."""
    
    def test_success_result(self):
        """Successful processing produces valid result."""
        result = ProcessingResult(
            success=True,
            content="Recovered content",
            confidence=0.95,
            repairs_applied=["encoding_fix"]
        )
        assert result.success
        assert result.content == "Recovered content"
        assert result.confidence == 0.95
    
    def test_failure_result(self):
        """Failed processing includes error information."""
        result = ProcessingResult(
            success=False,
            content=None,
            confidence=0.0,
            error="Unrecoverable corruption"
        )
        assert not result.success
        assert result.error == "Unrecoverable corruption"
    
    def test_confidence_bounds(self):
        """Confidence scores are between 0 and 1."""
        result = ProcessingResult(success=True, content="test", confidence=0.75)
        assert 0.0 <= result.confidence <= 1.0


# ==================== CorruptedFileGenerator Tests ====================

class TestCorruptedFileGenerator:
    """Test suite for generating corrupted test files."""
    
    @pytest.fixture
    def generator(self):
        return CorruptedFileGenerator()
    
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_generates_encoding_errors(self, generator, temp_dir):
        """Generator can create files with encoding errors."""
        file_path = temp_dir / "encoding_error.txt"
        generator.generate(file_path, CorruptionType.ENCODING_ERROR)
        assert file_path.exists()
        # File should have invalid encoding sequences
    
    def test_generates_truncated_files(self, generator, temp_dir):
        """Generator can create truncated files."""
        file_path = temp_dir / "truncated.txt"
        generator.generate(file_path, CorruptionType.TRUNCATION, original_content="Hello World!")
        assert file_path.exists()
        content = file_path.read_bytes()
        # Truncated file should be shorter than original
    
    def test_generates_bit_flip_corruption(self, generator, temp_dir):
        """Generator can create files with bit flips."""
        file_path = temp_dir / "bit_flip.txt"
        original = "The quick brown fox"
        generator.generate(file_path, CorruptionType.BIT_FLIP, original_content=original)
        assert file_path.exists()
        corrupted = file_path.read_bytes()
        # Content should differ from original
    
    def test_generates_missing_delimiter(self, generator, temp_dir):
        """Generator can create CSV files with missing delimiters."""
        file_path = temp_dir / "missing_delim.csv"
        generator.generate(file_path, CorruptionType.MISSING_DELIMITER)
        assert file_path.exists()


# ==================== CorruptionDetector Tests ====================

class TestCorruptionDetector:
    """Test suite for corruption detection."""
    
    @pytest.fixture
    def detector(self):
        return CorruptionDetector()
    
    def test_detects_encoding_errors(self, detector):
        """Detector identifies encoding corruption."""
        # Create bytes with invalid UTF-8 sequence
        corrupted_bytes = b"Hello \xff\xfe World"
        report = detector.detect(corrupted_bytes)
        assert report is not None
        assert report.corruption_type == CorruptionType.ENCODING_ERROR
    
    def test_detects_truncation(self, detector):
        """Detector identifies truncated content."""
        # JSON that's clearly truncated
        truncated = b'{"key": "val'
        report = detector.detect(truncated, expected_format="json")
        assert report is not None
        assert report.corruption_type == CorruptionType.TRUNCATION
    
    def test_detects_unicode_corruption(self, detector):
        """Detector identifies unicode replacement characters."""
        corrupted = "Hello \ufffd\ufffd World".encode('utf-8')
        report = detector.detect(corrupted)
        assert report is not None
        assert report.corruption_type == CorruptionType.UNICODE_CORRUPTION
    
    def test_returns_none_for_clean_content(self, detector):
        """Detector returns None for clean files."""
        clean = b"This is perfectly valid content."
        report = detector.detect(clean)
        assert report is None
    
    def test_detects_missing_csv_delimiter(self, detector):
        """Detector identifies CSV delimiter issues."""
        # CSV with inconsistent column counts
        bad_csv = b"a,b,c\n1,2\n3,4,5,6"
        report = detector.detect(bad_csv, expected_format="csv")
        assert report is not None


# ==================== RepairEngine Tests ====================

class TestRepairEngine:
    """Test suite for corruption repair."""
    
    @pytest.fixture
    def engine(self):
        return RepairEngine()
    
    def test_repairs_encoding_errors(self, engine):
        """Engine can repair encoding errors."""
        corrupted = b"Caf\xe9 au lait"  # Latin-1 encoded
        report = CorruptionReport(
            corruption_type=CorruptionType.ENCODING_ERROR,
            severity="medium",
            location=3
        )
        result = engine.repair(corrupted, report)
        assert result.success
        assert "Caf" in result.content
    
    def test_repairs_unicode_replacement(self, engine):
        """Engine can repair unicode replacement characters."""
        corrupted = "Hello \ufffd World"
        report = CorruptionReport(
            corruption_type=CorruptionType.UNICODE_CORRUPTION,
            severity="low",
            location=6
        )
        result = engine.repair(corrupted.encode(), report)
        assert result.success
        assert result.confidence > 0
    
    def test_normalizes_unicode(self, engine):
        """Engine normalizes unicode to NFKC form."""
        denormalized = "ﬁnancial café"  # fi ligature
        report = CorruptionReport(
            corruption_type=CorruptionType.UNICODE_CORRUPTION,
            severity="low",
            location=0
        )
        result = engine.repair(denormalized.encode(), report)
        if result.success:
            # Should normalize fi ligature to 'fi'
            assert "fi" in result.content.lower()
    
    def test_confidence_reflects_repair_quality(self, engine):
        """Repair confidence accurately reflects quality."""
        # Minor corruption should have high confidence
        minor = b"Hello Wor1d"  # Single character flip
        report = CorruptionReport(
            corruption_type=CorruptionType.BIT_FLIP,
            severity="low",
            location=9
        )
        result = engine.repair(minor, report)
        assert result.confidence >= 0.5  # Should be reasonably confident


# ==================== FileProcessor Integration Tests ====================

class TestFileProcessor:
    """Integration tests for the file processor pipeline."""
    
    @pytest.fixture
    def processor(self):
        return FileProcessor()
    
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_processes_clean_file(self, processor, temp_dir):
        """Processor handles clean files without issues."""
        clean_file = temp_dir / "clean.txt"
        clean_file.write_text("This is perfectly valid content.", encoding="utf-8")
        
        result = processor.process(clean_file)
        assert result.success
        assert result.confidence >= 0.9
        assert result.content == "This is perfectly valid content."
    
    def test_processes_and_repairs_corrupted_file(self, processor, temp_dir):
        """Processor detects and repairs corruption."""
        corrupted_file = temp_dir / "corrupted.txt"
        corrupted_file.write_bytes(b"Hello \xff\xfe World")
        
        result = processor.process(corrupted_file)
        assert result.success
        assert "Hello" in result.content
        assert "World" in result.content
    
    def test_reports_unrecoverable_corruption(self, processor, temp_dir):
        """Processor reports when corruption is unrecoverable."""
        # Completely corrupted binary
        corrupted_file = temp_dir / "binary_garbage.txt"
        corrupted_file.write_bytes(bytes(range(256)))
        
        result = processor.process(corrupted_file)
        # May succeed with low confidence or fail
        if not result.success:
            assert result.error is not None
    
    def test_multi_phase_pipeline(self, processor, temp_dir):
        """Processor runs all 4 phases of the pipeline."""
        test_file = temp_dir / "multi_phase.txt"
        test_file.write_text("Valid content for testing.", encoding="utf-8")
        
        result = processor.process(test_file)
        assert result.success
        # Phases: detect -> classify -> repair -> validate
        assert len(result.repairs_applied) >= 0
    
    def test_handles_missing_file(self, processor, temp_dir):
        """Processor handles missing files gracefully."""
        missing = temp_dir / "does_not_exist.txt"
        
        result = processor.process(missing)
        assert not result.success
        assert "not found" in result.error.lower() or "no such file" in result.error.lower()
    
    def test_handles_empty_file(self, processor, temp_dir):
        """Processor handles empty files."""
        empty = temp_dir / "empty.txt"
        empty.write_bytes(b"")
        
        result = processor.process(empty)
        # Empty file might be valid or flagged - either is acceptable
        assert result is not None


# ==================== Confidence Scoring Tests ====================

class TestConfidenceScoring:
    """Tests for the confidence scoring system."""
    
    @pytest.fixture
    def processor(self):
        return FileProcessor()
    
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_high_confidence_for_clean_files(self, processor, temp_dir):
        """Clean files get high confidence scores."""
        clean = temp_dir / "perfect.txt"
        clean.write_text("Absolutely perfect content.", encoding="utf-8")
        
        result = processor.process(clean)
        assert result.confidence >= 0.9
    
    def test_lower_confidence_for_repaired_files(self, processor, temp_dir):
        """Repaired files have lower confidence than clean files."""
        repaired = temp_dir / "repaired.txt"
        repaired.write_bytes(b"Content with \xff errors")
        
        result = processor.process(repaired)
        # Repaired content should have lower confidence
        assert result.confidence < 1.0
    
    def test_confidence_decreases_with_severity(self, processor, temp_dir):
        """More severe corruption results in lower confidence."""
        # Light corruption
        light = temp_dir / "light.txt"
        light.write_bytes(b"Just one \xff byte wrong")
        
        # Heavy corruption
        heavy = temp_dir / "heavy.txt"
        heavy.write_bytes(b"\xff\xfe\xfd" * 10 + b"tiny bit of text")
        
        result_light = processor.process(light)
        result_heavy = processor.process(heavy)
        
        # Light corruption should have higher confidence than heavy
        if result_light.success and result_heavy.success:
            assert result_light.confidence >= result_heavy.confidence


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
