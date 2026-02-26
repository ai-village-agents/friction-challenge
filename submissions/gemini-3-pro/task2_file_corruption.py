import json
import os
import random
import unicodedata


class CorruptionSanitizer:
    """Detects and repairs common file corruptions in CSV-like text."""

    ZERO_WIDTH_CHARS = {"\u200b", "\u200c", "\u200d", "\ufeff", "\u2060"}
    CONTROL_WHITELIST = {"\n", "\r", "\t"}

    def __init__(self, path):
        self.path = path

    def _read_bytes(self):
        with open(self.path, "rb") as handle:
            return handle.read()

    def _is_control_char(self, char):
        return unicodedata.category(char) == "Cc" and char not in self.CONTROL_WHITELIST

    def _strip_zero_width(self, text):
        for zw in self.ZERO_WIDTH_CHARS:
            text = text.replace(zw, "")
        return text

    def _strip_control_chars(self, text):
        return "".join(ch for ch in text if not self._is_control_char(ch))

    def _find_indices(self, text, predicate):
        return [idx for idx, ch in enumerate(text) if predicate(ch)]

    def _sample_indices(self, indices, limit=25):
        return indices[:limit]

    def _csv_shape(self, text):
        rows = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            columns = [col.strip() for col in stripped.split(",")]
            rows.append(len(columns))
        if not rows:
            return {"rows": 0, "columns": None, "consistent": False}
        consistent = len(set(rows)) == 1
        expected_columns = rows[0] if consistent else None
        return {"rows": len(rows), "columns": expected_columns, "consistent": consistent}

    def _confidence(self, integrity_score, anomaly_density, csv_preserved, utf8_errors, total_length):
        score = integrity_score
        if csv_preserved:
            score += 0.15
        if anomaly_density > 0.05:
            score -= 0.1
        if anomaly_density > 0.15:
            score -= 0.1
        if utf8_errors == 0:
            score += 0.05
        if total_length == 0:
            score = min(score, 0.5)
        return max(0.0, min(1.0, score))

    def sanitize(self, persist_cleaned=False):
        raw = self._read_bytes()
        lossy_text = raw.decode("utf-8", errors="replace")

        null_positions = [idx for idx, byte in enumerate(raw) if byte == 0]
        zero_width_positions = self._find_indices(lossy_text, lambda ch: ch in self.ZERO_WIDTH_CHARS)
        control_positions = self._find_indices(lossy_text, self._is_control_char)
        utf8_error_positions = self._find_indices(lossy_text, lambda ch: ch == "\ufffd")

        anomalies_total = (
            len(null_positions)
            + len(zero_width_positions)
            + len(control_positions)
            + len(utf8_error_positions)
        )
        anomaly_density = anomalies_total / float(max(len(lossy_text), 1))
        integrity_score = max(0.0, min(1.0, 1.0 - anomaly_density))

        cleaned_bytes = raw.replace(b"\x00", b"")
        cleaned_text = cleaned_bytes.decode("utf-8", errors="ignore")
        cleaned_text = self._strip_zero_width(cleaned_text)
        cleaned_text = self._strip_control_chars(cleaned_text)

        csv_before = self._csv_shape(lossy_text)
        csv_after = self._csv_shape(cleaned_text)
        csv_preserved = csv_after["consistent"] and (
            csv_before["columns"] in (None, csv_after["columns"])
        )

        confidence_score = self._confidence(
            integrity_score,
            anomaly_density,
            csv_preserved,
            len(utf8_error_positions),
            len(cleaned_text),
        )

        dropped_bytes = max(
            0,
            len(raw.replace(b"\x00", b"")) - len(cleaned_text.encode("utf-8")),
        )

        cleaned_path = None
        if persist_cleaned:
            cleaned_path = f"{self.path}.cleaned"
            with open(cleaned_path, "w", encoding="utf-8", newline="") as handle:
                handle.write(cleaned_text)

        report = {
            "file": self.path,
            "size_bytes": len(raw),
            "anomalies": {
                "null_bytes": {
                    "count": len(null_positions),
                    "positions_sample": self._sample_indices(null_positions),
                },
                "zero_width_chars": {
                    "count": len(zero_width_positions),
                    "positions_sample": self._sample_indices(zero_width_positions),
                },
                "control_chars": {
                    "count": len(control_positions),
                    "positions_sample": self._sample_indices(control_positions),
                },
                "utf8_decode_errors": {
                    "count": len(utf8_error_positions),
                    "positions_sample": self._sample_indices(utf8_error_positions),
                },
            },
            "anomaly_density": anomaly_density,
            "integrity_score": round(integrity_score, 4),
            "confidence_score": round(confidence_score, 4),
            "csv_structure": {
                "before": csv_before,
                "after": csv_after,
                "preserved_after_repair": csv_preserved,
            },
            "repairs": {
                "removed_null_bytes": len(null_positions),
                "removed_zero_width_chars": len(zero_width_positions),
                "removed_control_chars": len(control_positions),
                "dropped_invalid_utf8_bytes": dropped_bytes,
                "cleaned_output_path": cleaned_path,
            },
            "cleaned_preview": cleaned_text[:200],
        }
        return report


def generate_nasty_corrupted_file(path):
    random.seed()
    rows = ["id,name,score"]
    for idx in range(1, 11):
        rows.append(f"{idx},User {idx},{random.randint(50, 100)}")
    baseline = "\n".join(rows) + "\n"
    baseline_bytes = baseline.encode("utf-8")

    payloads = [
        b"\x00",
        "\u200b".encode("utf-8"),
        "\ufeff".encode("utf-8"),
        b"\x0b",
        b"\x1f",
        b"\x7f",
        b"\xff\xfe",
        b"\xed\xa0\x80",
    ]

    corrupted = bytearray(baseline_bytes)
    for _ in range(random.randint(8, 16)):
        payload = random.choice(payloads)
        position = random.randint(0, len(corrupted))
        corrupted[position:position] = payload

    # Occasionally drop a comma to simulate column drift.
    if random.random() < 0.5 and b"," in corrupted:
        comma_positions = [idx for idx, byte in enumerate(corrupted) if byte == ord(",")]
        if comma_positions:
            corrupted.pop(random.choice(comma_positions))

    with open(path, "wb") as handle:
        handle.write(bytes(corrupted))
    return path


if __name__ == "__main__":
    base_dir = os.path.dirname(__file__)
    target_file = os.path.join(base_dir, "task2_corrupted_sample.csv")

    generate_nasty_corrupted_file(target_file)
    sanitizer = CorruptionSanitizer(target_file)
    report = sanitizer.sanitize(persist_cleaned=True)

    print(json.dumps(report, indent=2))
