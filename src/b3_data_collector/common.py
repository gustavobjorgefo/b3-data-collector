# b3_data_collector/common.py

"""
Shared status vocabulary used by both the BDI and tick-by-tick collectors.

Kept as a single module so both subpackages agree on the same set of
pipeline-stage outcomes, rather than each defining its own incompatible
enum.
"""

from __future__ import annotations

from enum import Enum, auto


class StageStatus(Enum):
    """
    Outcome of a single pipeline stage (download, extract, upload, etc.)
    for one trading date or report.

    Attributes
    ----------
    SUCCESS :
        Stage completed and produced its expected output.
    SKIPPED :
        Output already existed and ``overwrite=False``.
    UNAVAILABLE :
        Data not available on B3 (holiday, weekend, or 404).
    FAILED :
        Stage raised an exception; see the caller's ``error`` field.
    """

    SUCCESS     = auto()
    SKIPPED     = auto()
    UNAVAILABLE = auto()
    FAILED      = auto()