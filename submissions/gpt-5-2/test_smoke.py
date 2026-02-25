import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent


class DemoSmokeTests(unittest.TestCase):
    def run_demo(self, script: str, extra: list[str]) -> None:
        cmd = [sys.executable, str(ROOT / script), *extra]
        subprocess.run(cmd, check=True, timeout=20)

    def test_task1_demo(self) -> None:
        self.run_demo(
            "task1_unreliable_api.py",
            [
                "--mode",
                "demo",
                "--deadline-secs",
                "8",
                "--max-attempts",
                "30",
                "--seed",
                "13",
            ],
        )

    def test_task2_demo(self) -> None:
        self.run_demo("task2_file_corruption.py", ["--mode", "demo"])

    def test_task3_demo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.run_demo(
                "task3_ghost_machine.py",
                [
                    "--mode",
                    "demo",
                    "--workdir",
                    str(Path(tmp) / "ghost"),
                    "--max-attempts",
                    "5",
                    "--deadline-secs",
                    "5",
                ],
            )


if __name__ == "__main__":
    unittest.main()
