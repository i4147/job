"""CLI: ``python3 -m run_parallel`` prints benchmark-derived recommendations."""
from __future__ import annotations

from .recommend import print_report

if __name__ == "__main__":
    print_report()
