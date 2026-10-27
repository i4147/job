"""Standalone, memory-friendly parallel helper for Termux / Android."""

from .run_parallel_lite import (
    LiteParallel,
    LiteResult,
    Outcome,
    hash_file,
    hash_folder,
    iter_file_digests,
    iter_files,
    peak_rss_kb,
)

__all__ = [
    "LiteParallel",
    "LiteResult",
    "Outcome",
    "iter_files",
    "hash_file",
    "iter_file_digests",
    "hash_folder",
    "peak_rss_kb",
]
