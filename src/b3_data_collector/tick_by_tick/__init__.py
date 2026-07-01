# src\b3_data_collector\tick_by_tick\__init__.py

"""
B3 tick-data ingestion package.

Public API
----------
run_pipeline
    Execute the B3 intraday tick-data ingestion pipeline for one or more
    trading dates and a specific feed type. See ``pipeline.py`` for full
    parameter documentation.

FeedType
    Feed selector enum. Use ``FeedType.RV`` for equities and
    ``FeedType.DERIV`` for derivatives.

Examples
--------
Run both feeds for a single date:

>>> from b3_data_collector.tick_by_tick import run_pipeline, FeedType
>>> run_pipeline("2026-06-26", feed=FeedType.RV)
>>> run_pipeline("2026-06-26", feed=FeedType.DERIV)

Backfill a date range for the derivatives feed:

>>> run_pipeline(("2026-01-01", "2026-06-26"), feed=FeedType.DERIV)
"""

from __future__ import annotations

from ._feed import FeedType
from .pipeline import run_pipeline

__all__: list[str] = ["FeedType", "run_pipeline"]