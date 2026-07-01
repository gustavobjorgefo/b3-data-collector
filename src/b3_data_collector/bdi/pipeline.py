# src\b3_data_collector\bdi\pipeline.py

"""
BDI reports ingestion pipeline.

Orchestrates the two-step process for every enabled report in the catalog:

    Step 1 — Fetch  : POST to the BDI export endpoint; receive CSV bytes.
    Step 2 — Upload : stream bytes directly to S3 (no local file written).

The pipeline iterates over all enabled ``ReportDefinition`` entries in
``_catalog.ENABLED_REPORTS`` for each requested trading date.

Date input contract
-------------------
Identical to the tick-by-tick pipeline:

- Single date  : ``"2026-06-26"`` or ``date(2026, 6, 26)``
- Explicit list: ``["2026-06-26", "2026-06-27"]``
- Date range   : ``("2026-05-29", "2026-06-27")`` — expanded to business days

This module contains no logging configuration — callers are responsible
for configuring the logging stack.
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Final

import requests

from ._catalog import ENABLED_REPORTS
from ._client import fetch_report_csv
from ._models import BdiPipelineResult, ReportResult, ReportStatus
from ._uploader import upload_csv
from ..common import StageStatus

logger = logging.getLogger(__name__)

_WEEKEND_CUTOFF: Final[int] = 5


# --- Internal helpers ---

def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _business_days_in_range(start: date, end: date) -> list[date]:
    """Return all Mon–Fri dates between ``start`` and ``end``, inclusive."""
    total = (end - start).days + 1
    return [
        start + timedelta(days=i)
        for i in range(total)
        if (start + timedelta(days=i)).weekday() < _WEEKEND_CUTOFF
    ]


def _resolve_dates(
    dates: str | date | list[str | date] | tuple[str | date, str | date],
) -> list[date]:
    """
    Resolve any accepted date input into a sorted list of unique dates.

    Parameters
    ----------
    dates : str | date | list | tuple
        Trading dates in any accepted form.

    Returns
    -------
    list[date]

    Raises
    ------
    TypeError
        If ``dates`` is not one of the accepted types.
    ValueError
        If a tuple does not have exactly 2 elements.
    """
    if isinstance(dates, (str, date)):
        return [_parse_date(dates)]

    if isinstance(dates, tuple):
        if len(dates) != 2:
            raise ValueError(
                f"A tuple input must have exactly 2 elements (start, end). "
                f"Got {len(dates)}."
            )
        return _business_days_in_range(_parse_date(dates[0]), _parse_date(dates[1]))

    if isinstance(dates, list):
        return sorted({_parse_date(d) for d in dates})

    raise TypeError(
        f"Unsupported dates type: {type(dates).__name__}. "
        "Expected str, date, list, or tuple."
    )


def _run_single_report(
    api_name   : str,
    section    : str,
    trade_date : date,
    overwrite  : bool,
) -> ReportResult:
    """
    Fetch and upload a single report for one trading date.

    Parameters
    ----------
    api_name : str
        BDI API name of the report.
    section : str
        Section used in the S3 key path.
    trade_date : date
        Trading date to process.
    overwrite : bool
        Whether to overwrite existing S3 objects.

    Returns
    -------
    ReportResult
    """
    result = ReportResult(report_name=api_name, trade_date=trade_date)

    # --- Step 1: Fetch CSV from BDI API ---
    try:
        content = fetch_report_csv(api_name=api_name, trade_date=trade_date)
    except requests.HTTPError as exc:
        logger.error("HTTP error fetching '%s' for %s: %s", api_name, trade_date, exc)
        result.status = ReportStatus.FAILED
        result.error  = f"HTTP error: {exc}"
        return result
    except requests.RequestException as exc:
        logger.error("Network error fetching '%s' for %s: %s", api_name, trade_date, exc)
        result.status = ReportStatus.FAILED
        result.error  = f"Network error: {exc}"
        return result

    if content is None:
        result.status = ReportStatus.UNAVAILABLE
        return result

    # --- Step 2: Upload to S3 ---
    s3_status = upload_csv(
        content    = content,
        section    = section,
        api_name   = api_name,
        trade_date = trade_date,
        overwrite  = overwrite,
    )

    # Map StageStatus → ReportStatus (same semantics, different enum).
    result.status = {
        StageStatus.SUCCESS : ReportStatus.SUCCESS,
        StageStatus.SKIPPED : ReportStatus.SKIPPED,
        StageStatus.FAILED  : ReportStatus.FAILED,
    }.get(s3_status, ReportStatus.FAILED)

    if result.status is ReportStatus.FAILED:
        result.error = "S3 upload failed — see log for details."

    return result


# --- Public API ---

def run_bdi_pipeline(
    dates     : str | date | list[str | date] | tuple[str | date, str | date],
    *,
    overwrite : bool = False,
) -> BdiPipelineResult:
    """
    Execute the BDI reports ingestion pipeline.

    Downloads and uploads to S3 every enabled report in the catalog for
    each requested trading date. Reports unavailable on B3 for a given
    date (holiday, weekend, not yet published) are recorded as
    ``UNAVAILABLE`` and the pipeline continues.

    Parameters
    ----------
    dates : str | date | list[str | date] | tuple[str | date, str | date]
        Trading dates to process:

        - Single date  : ``"2026-06-26"`` or ``date(2026, 6, 26)``
        - Explicit list: ``["2026-06-26", "2026-06-27"]``
        - Date range   : ``("2026-05-29", "2026-06-27")``
    overwrite : bool, optional
        If ``True``, re-downloads and re-uploads even when the S3 object
        already exists. Default is ``False``.

    Returns
    -------
    BdiPipelineResult
        Structured result with per-report outcomes and aggregate statistics.
    """
    date_list       = _resolve_dates(dates)
    pipeline_result = BdiPipelineResult(started_at=time.monotonic())
    enabled_count   = len(ENABLED_REPORTS)

    logger.info(
        "BDI pipeline starting — %d date(s) × %d reports = %d requests",
        len(date_list), enabled_count, len(date_list) * enabled_count,
    )
    logger.info(
        "Date range: %s → %s  |  overwrite=%s",
        date_list[0], date_list[-1], overwrite,
    )

    for current_date in date_list:
        logger.info("--- Processing %s (%d reports) ---", current_date, enabled_count)

        for report in ENABLED_REPORTS:
            report_result = _run_single_report(
                api_name   = report.api_name,
                section    = report.section,
                trade_date = current_date,
                overwrite  = overwrite,
            )
            pipeline_result.results.append(report_result)
            logger.info(report_result.summary_line)

    pipeline_result.finished_at = time.monotonic()
    logger.info("\n%s", pipeline_result.summary)

    return pipeline_result