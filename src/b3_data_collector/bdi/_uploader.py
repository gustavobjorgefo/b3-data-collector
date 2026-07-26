# b3_data_collector/bdi/_uploader.py

"""
S3 uploader for BDI report CSVs.

Builds the Hive-style partitioned S3 key for a given report and trading
date, and delegates the actual upload to the shared helpers in
``common.py``. Key structure is BDI-specific and therefore lives here,
not in ``common.py``.

Key format
----------
b3/bdi/reports/{section}/{api_name}/year={YYYY}/{YYYY-MM-DD}.csv
"""

from __future__ import annotations

from datetime import date
from typing import Final

from ..common import StageStatus, upload_bytes_to_s3
from ..config import settings

# --- Module constants ---

_S3_KEY_PREFIX : Final[str] = "b3/bdi/reports/"
_CONTENT_TYPE  : Final[str] = "text/csv"


# --- Internal helpers ---

def _build_s3_key(section: str, api_name: str, trade_date: date) -> str:
    return (
        f"{_S3_KEY_PREFIX}"
        f"{section}/"
        f"{api_name}/"
        f"year={trade_date:%Y}/"
        f"{trade_date}.csv"
    )


# --- Public API ---

def upload_csv(
    content    : bytes,
    section    : str,
    api_name   : str,
    trade_date : date,
    overwrite  : bool = False,
) -> StageStatus:
    """
    Upload a single BDI report CSV to S3 under its Hive-partitioned key.

    Parameters
    ----------
    content : bytes
        Raw CSV bytes as returned by ``fetch_report_csv``.
    section : str
        Top-level report section (e.g. ``"renda_variavel"``).
    api_name : str
        BDI API name of the report (e.g. ``"DailyAverageStocks"``).
    trade_date : date
        Trading date the report refers to.
    overwrite : bool, optional
        If ``True``, re-uploads even when the object already exists.
        Default is ``False``.

    Returns
    -------
    StageStatus
        ``SUCCESS``, ``SKIPPED``, or ``FAILED``.
    """
    s3_key = _build_s3_key(section, api_name, trade_date)

    return upload_bytes_to_s3(
        content      = content,
        bucket       = settings.AWS_S3_BUCKET_B3,
        key          = s3_key,
        content_type = _CONTENT_TYPE,
        overwrite    = overwrite,
    )