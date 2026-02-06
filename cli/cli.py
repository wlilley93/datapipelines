#!/usr/bin/env python3
"""
CLI Data Orchestrator - Production Edition

NOTE:
This file is intentionally small now. The orchestration UI and logic live in the
`orchestrator/` package to keep responsibilities separated and diffs manageable.
"""
from __future__ import annotations

import sys
import os
from pathlib import Path

# Add the parent directory to Python path to allow imports from sibling directories
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from orchestrator.app import main


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
