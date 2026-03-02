"""Run a CDS API smoke test against ERA5 dataset.

Usage:
  python scripts/test_cdsapi.py
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.cds_client import run_cds_smoke_test


if __name__ == "__main__":
    result = run_cds_smoke_test()
    print(json.dumps(result, ensure_ascii=False, indent=2))
