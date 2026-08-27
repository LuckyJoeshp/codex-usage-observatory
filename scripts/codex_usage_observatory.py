#!/usr/bin/env python3
"""Canonical command-line entry point for Codex Usage Observatory."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.cliproxy_usage_meter import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
