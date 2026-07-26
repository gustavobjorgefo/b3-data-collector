# src/b3_data_collector/reader/api.py

"""
Public reader API — turns S3-collected data into pandas DataFrames.

BDI reports are parsed via the registry in ``bdi/_parsers.py`` (one
parser per report, since layouts vary); tick-by-tick data is read
directly, since both feeds share a single canonical schema and require
no per-report parsing.

Date input contract
--------------------
Identical to the collector pipelines, resolved via ``common.resolve_dates``:

- Single date  : ``"2026-06-30"`` or ``date(2026, 6, 30)``
- Explicit list: ``["2026-06-29", "2026-06-30"]``
- Date range   : ``("2026-06-01", "2026-06-30")`` — expanded to business days

Dates with no corresponding object in S3 are skipped with a warning
rather than raising — a partial result across a wide range is more
useful than an all-or-nothing failure.
"""

from __future__ import annotations

import io
import logging
from datetime import date

import pandas as pd

from ..bdi._parsers import read_bdi_report_file
from ..common import resolve_dates
from ..tick_by_tick import FeedType
from ._client import fetch_bdi_report_bytes, fetch_tick_by_tick_bytes

logger = logging.getLogger(__name__)

DateInput = str | date | list[str | date] | tuple[str | date, str | date]


# --- Public API ---

def read_bdi_report(api_name: str, dates: DateInput) -> pd.DataFrame:
    """
    Read one or more trading dates of a BDI report already collected to S3.

    Parameters
    ----------
    api_name : str
        BDI API name of the report (e.g. ``"BTBLoanBalance"``), as defined
        in ``b3_data_collector.bdi._catalog``. A parser must be registered
        for this report in ``b3_data_collector.bdi._parsers``.
    dates : str | date | list[str | date] | tuple[str | date, str | date]
        Trading dates to read:

        - Single date  : ``"2026-06-30"`` or ``date(2026, 6, 30)``
        - Explicit list: ``["2026-06-29", "2026-06-30"]``
        - Date range   : ``("2026-06-01", "2026-06-30")``

    Returns
    -------
    pd.DataFrame
        Concatenated rows across all requested dates, in date order.
        Empty if none of the requested dates has data in S3.

    Raises
    ------
    KeyError
        If ``api_name`` is not a known report in the catalog.
    NotImplementedError
        If no parser is registered yet for ``api_name``.
    """
    frames: list[pd.DataFrame] = []

    for trade_date in resolve_dates(dates):
        try:
            content = fetch_bdi_report_bytes(api_name=api_name, trade_date=trade_date)
        except FileNotFoundError:
            logger.warning(
                "No '%s' report in S3 for %s — skipping.", api_name, trade_date
            )
            continue

        frames.append(read_bdi_report_file(content, report_name=api_name))

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def read_tick_by_tick(feed: FeedType, dates: DateInput) -> pd.DataFrame:
    """
    Read one or more trading dates of processed tick-by-tick data from S3.

    Parameters
    ----------
    feed : FeedType
        Feed selector (``FeedType.RV`` or ``FeedType.DERIV``).
    dates : str | date | list[str | date] | tuple[str | date, str | date]
        Trading dates to read (same contract as ``read_bdi_report``).

    Returns
    -------
    pd.DataFrame
        Concatenated ticks across all requested dates, in date order.
        Empty if none of the requested dates has data in S3.
    """
    frames: list[pd.DataFrame] = []

    for trade_date in resolve_dates(dates):
        try:
            content = fetch_tick_by_tick_bytes(feed=feed, trade_date=trade_date)
        except FileNotFoundError:
            logger.warning(
                "[%s] No ticks Parquet in S3 for %s — skipping.",
                feed.config.label, trade_date,
            )
            continue

        frames.append(pd.read_parquet(io.BytesIO(content)))

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)