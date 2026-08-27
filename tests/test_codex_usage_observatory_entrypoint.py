from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINT = ROOT / "scripts" / "codex_usage_observatory.py"


class CodexUsageObservatoryEntrypointTests(unittest.TestCase):
    def test_canonical_entrypoint_exposes_observatory_help(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(ENTRYPOINT), "--help"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("codex_usage_observatory.py", completed.stdout)
        self.assertIn(
            "Local-first Codex usage, cost, and quota observatory",
            completed.stdout,
        )


if __name__ == "__main__":
    unittest.main()
