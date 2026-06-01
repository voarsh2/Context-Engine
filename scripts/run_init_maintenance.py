#!/usr/bin/env python3
"""Run the init maintenance script sequence under the shared watcher lock."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.watch_index_core.init_maintenance import run_init_maintenance_once


def main() -> int:
    return 0 if run_init_maintenance_once() else 1


if __name__ == "__main__":
    raise SystemExit(main())
