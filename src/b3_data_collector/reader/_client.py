# src/b3_data_collector/reader/_client.py

"""
S3 fetch helpers for the reader subpackage.

Builds the exact same partition keys used at upload time by the BDI
uploader (``bdi/_uploader.py``) and the tick-by-tick partitioner
(``tick_by_tick/_partitioner.py``) — reusing those key-building functions
directly rather than re-deriving the partition scheme here, which would
duplicate it a third time.

Delegates the actual S3 GET to ``common.download_bytes_from_s3``.
"""

from __future__ import annotations

from datetime import date

from ..bdi._catalog import CATALOG_BY_NAME
from ..bdi._uploader import _build_s3_key
from ..common import download_bytes_from_s3
from ..config import settings
from ..tick_by_tick import FeedType
from ..tick_by_tick._partitioner import _build_s3_key_ticks


def fetch_bdi_report_bytes(api_name: str, trade_date: date) -> bytes:
    """
    Download a single BDI report CSV from S3.

    Parameters
    ----------
    api_name : str
        BDI API name of the report (e.g. ``"BTBLoanBalance"``). Must be
        a known entry in ``b3_data_collector.bdi._catalog.CATALOG``.
    trade_date : date
        Trading date to fetch.

    Returns
    -------
    bytes
        Raw CSV bytes, exactly as uploaded by the BDI pipeline.

    Raises
    ------
    KeyError
        If ``api_name`` is not a known report in the catalog.
    FileNotFoundError
        If no object exists in S3 for this report and date (not yet
        collected, or unavailable from B3 on that date).
    """
    try:
        report = CATALOG_BY_NAME[api_name]
    except KeyError:
        raise KeyError(
            f"Unknown BDI report '{api_name}'. See "
            "b3_data_collector.bdi._catalog.CATALOG for valid api_name values."
        ) from None

    key = _build_s3_key(report.section, api_name, trade_date)
    return download_bytes_from_s3(bucket=settings.AWS_S3_BUCKET_B3, key=key)


def fetch_tick_by_tick_bytes(feed: FeedType, trade_date: date) -> bytes:
    """
    Download a single processed ticks Parquet from S3.

    Parameters
    ----------
    feed : FeedType
        Feed selector (``FeedType.RV`` or ``FeedType.DERIV``).
    trade_date : date
        Trading date to fetch.

    Returns
    -------
    bytes
        Raw Parquet bytes, exactly as uploaded by the partitioner.

    Raises
    ------
    FileNotFoundError
        If no object exists in S3 for this feed and date.
    """
    key = _build_s3_key_ticks(feed.config.s3_prefix_ticks, trade_date)
    return download_bytes_from_s3(bucket=settings.AWS_S3_BUCKET_B3, key=key)