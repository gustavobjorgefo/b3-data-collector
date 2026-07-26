# src/b3_data_collector/reader/__init__.py

"""
Read-access layer for data already collected to S3 by the BDI and
tick-by-tick pipelines.

Public API
----------
read_bdi_report
    Read one or more trading dates of a BDI report CSV from S3 into a
    single DataFrame. See ``api.py`` for full parameter documentation.
read_tick_by_tick
    Read one or more trading dates of processed tick-by-tick Parquet
    data from S3 into a single DataFrame.

Examples
--------
>>> from b3_data_collector.reader import read_bdi_report
>>> df = read_bdi_report("BTBLoanBalance", dates=("2026-06-01", "2026-06-30"))

>>> from b3_data_collector.reader import read_tick_by_tick
>>> from b3_data_collector.tick_by_tick import FeedType
>>> df = read_tick_by_tick(FeedType.RV, dates="2026-06-30")
"""

from __future__ import annotations

from .api import read_bdi_report, read_tick_by_tick

__all__: list[str] = ["read_bdi_report", "read_tick_by_tick"]