# src\b3_data_collector\tick_by_tick\pipeline.py

"""
B3 intraday tick-data ingestion pipeline.

Orchestrates the four-stage data ingestion process for B3 tick data,
supporting both the equities (RV) and derivatives (DERIV) feeds via the
``FeedType`` parameter:

    Stage 1 — Download  : fetches the raw ZIP from B3's distribution endpoint
                          and uploads it to S3 as an immutable archive.
    Stage 2 — Extract   : parses the ZIP, normalises types, renames columns
                          to canonical English schema, saves daily Parquet.
    Stage 3 — Partition : constructs the timestamp column, selects output
                          columns, validates and saves the tick-level Parquet.
    Stage 4 — Ticks S3  : uploads the tick-level Parquet to S3, Hive-partitioned
                          by year (folded into Stage 3's ``partition_to_ticks``).

Date input contract
-------------------
- Single date  : ``"2025-11-11"`` or ``date(2025, 11, 11)``
- Explicit list: ``["2025-11-11", "2025-11-12"]``
- Date range   : ``("2025-11-01", "2025-11-30")`` — expanded to business days

The distinction between list and range is type-based: ``tuple`` always
implies a range; ``list`` always implies explicit dates.

This module contains no logging configuration — callers are responsible for
configuring the logging stack via ``logging.basicConfig`` or equivalent.
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Final

from ._downloader import download_zip
from ._extractor import extract_to_raw_parquet
from ._feed import FeedType
from ._models import DateResult, PipelineResult, StageStatus
from ._partitioner import partition_to_ticks

logger = logging.getLogger(__name__)

# --- Module constants ---

_WEEKEND_CUTOFF: Final[int] = 5  # weekday() returns 0–4 for Mon–Fri


# --- Internal helpers ---

def _parse_date(value: str | date) -> date:
    """Coerce a ``str`` or ``date`` to a ``date`` object."""
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _business_days_in_range(start: date, end: date) -> list[date]:
    """
    Return all weekdays (Mon–Fri) between ``start`` and ``end``, inclusive.

    Parameters
    ----------
    start : date
        Range start, inclusive.
    end : date
        Range end, inclusive.

    Returns
    -------
    list[date]
    """
    total = (end - start).days + 1
    return [
        start + timedelta(days=offset)
        for offset in range(total)
        if (start + timedelta(days=offset)).weekday() < _WEEKEND_CUTOFF
    ]


def _resolve_dates(
    dates: str | date | list[str | date] | tuple[str | date, str | date],
) -> list[date]:
    """
    Resolve any accepted date input format into a sorted list of unique dates.

    Parameters
    ----------
    dates : str | date | list | tuple
        Trading dates in any accepted form.

    Returns
    -------
    list[date]
        Sorted, deduplicated list of ``date`` objects.

    Raises
    ------
    TypeError
        If ``dates`` is not one of the accepted types.
    ValueError
        If a tuple is provided with a length other than 2.
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


def _run_single_date(
    trade_date   : date,
    feed         : FeedType,
    *,
    download     : bool = True,
    extract      : bool = True,
    partition    : bool = True,
    upload_to_s3 : bool = True,
    overwrite    : bool = False,
) -> DateResult:
    """
    Execute the full pipeline for a single trading date and feed type.

    Parameters
    ----------
    trade_date : date
        Trading date to process.
    feed : FeedType
        Feed selector (``FeedType.RV`` or ``FeedType.DERIV``).
    download : bool
        Enable the download stage.
    extract : bool
        Enable the extraction stage.
    partition : bool
        Enable the partitioning stage.
    upload_to_s3 : bool
        Enable S3 upload during the download stage and the ticks upload
        during the partitioning stage.
    overwrite : bool
        Re-run stages even when output already exists.

    Returns
    -------
    DateResult
    """
    result = DateResult(trade_date=trade_date)

    # --- Stage 1: Download ---
    if download:
        try:
            _, dl_status, s3_status = download_zip(
                trade_date   = trade_date,
                feed         = feed,
                upload_to_s3 = upload_to_s3,
            )
            result.download  = dl_status
            result.s3_upload = s3_status

            if dl_status is StageStatus.UNAVAILABLE:
                result.error = f"No data available on B3 for {trade_date}."
                return result

            if dl_status is StageStatus.FAILED:
                result.error = f"Download failed for {trade_date}."
                return result

        except Exception as exc:
            logger.exception(
                "Unexpected error in download stage for %s [%s]",
                trade_date, feed.config.label,
            )
            result.download = StageStatus.FAILED
            result.error    = f"Download exception: {exc}"
            return result

    # --- Stage 2: Extract ---
    if extract:
        try:
            result.extract = extract_to_raw_parquet(
                trade_date = trade_date,
                feed       = feed,
                overwrite  = overwrite,
            )
        except FileNotFoundError as exc:
            logger.error("Extract FileNotFoundError for %s: %s", trade_date, exc)
            result.extract = StageStatus.FAILED
            result.error   = str(exc)
            return result
        except (KeyError, ValueError) as exc:
            logger.error("Extract error for %s: %s", trade_date, exc)
            result.extract = StageStatus.FAILED
            result.error   = str(exc)
            return result
        except Exception as exc:
            logger.exception(
                "Unexpected error in extract stage for %s [%s]",
                trade_date, feed.config.label,
            )
            result.extract = StageStatus.FAILED
            result.error   = f"Extract exception: {exc}"
            return result

    # --- Stage 3 & 4: Partition + ticks S3 upload ---
    if partition:
        try:
            partition_status, ticks_s3_status, tick_count = partition_to_ticks(
                trade_date   = trade_date,
                feed         = feed,
                overwrite    = overwrite,
                upload_to_s3 = upload_to_s3,
            )
            result.partition       = partition_status
            result.ticks_s3_upload = ticks_s3_status
            result.tick_count      = tick_count
        except FileNotFoundError as exc:
            logger.error("Partition FileNotFoundError for %s: %s", trade_date, exc)
            result.partition = StageStatus.FAILED
            result.error     = str(exc)
        except ValueError as exc:
            logger.error("Partition validation error for %s: %s", trade_date, exc)
            result.partition = StageStatus.FAILED
            result.error     = str(exc)
        except Exception as exc:
            logger.exception(
                "Unexpected error in partition stage for %s [%s]",
                trade_date, feed.config.label,
            )
            result.partition = StageStatus.FAILED
            result.error     = f"Partition exception: {exc}"

    return result


# --- Public API ---

def run_pipeline(
    dates        : str | date | list[str | date] | tuple[str | date, str | date],
    feed         : FeedType = FeedType.RV,
    *,
    download     : bool = True,
    extract      : bool = True,
    partition    : bool = True,
    upload_to_s3 : bool = True,
    overwrite    : bool = False,
) -> PipelineResult:
    """
    Execute the B3 intraday tick-data ingestion pipeline.

    Runs all enabled stages for each date. Each stage can be toggled
    independently. Dates unavailable on B3 (holidays, weekends) are skipped
    gracefully — the pipeline continues to the next date without raising.

    Parameters
    ----------
    dates : str | date | list[str | date] | tuple[str | date, str | date]
        Trading dates to process:

        - Single date  : ``"2025-11-11"`` or ``date(2025, 11, 11)``
        - Explicit list: ``["2025-11-11", "2025-11-12"]``
        - Date range   : ``("2025-11-01", "2025-11-30")``
    feed : FeedType, optional
        Feed selector. Default is ``FeedType.RV`` (equities).
    download : bool, optional
        Enable the download stage. Default is ``True``.
    extract : bool, optional
        Enable the extraction stage. Default is ``True``.
    partition : bool, optional
        Enable the partitioning stage. Default is ``True``.
    upload_to_s3 : bool, optional
        Upload each ZIP to S3 after download, and each ticks Parquet to
        S3 after partitioning. Default is ``True``.
    overwrite : bool, optional
        Re-run all enabled stages even when output already exists.
        Default is ``False``.

    Returns
    -------
    PipelineResult
        Structured result containing per-date outcomes and aggregate
        statistics. Suitable for logging and e-mail notification.
    """
    date_list       = _resolve_dates(dates)
    pipeline_result = PipelineResult(started_at=time.monotonic())
    label           = feed.config.label

    logger.info(
        "[%s] Pipeline starting — %d date(s): %s → %s",
        label, len(date_list), date_list[0], date_list[-1],
    )
    logger.info(
        "[%s] Stages: download=%s  extract=%s  partition=%s  "
        "upload_to_s3=%s  overwrite=%s",
        label, download, extract, partition, upload_to_s3, overwrite,
    )

    for current_date in date_list:
        logger.info("[%s] --- Processing %s ---", label, current_date)

        date_result = _run_single_date(
            trade_date   = current_date,
            feed         = feed,
            download     = download,
            extract      = extract,
            partition    = partition,
            upload_to_s3 = upload_to_s3,
            overwrite    = overwrite,
        )
        pipeline_result.results.append(date_result)
        logger.info(date_result.summary_line)

    pipeline_result.finished_at = time.monotonic()
    logger.info("\n%s", pipeline_result.summary)

    return pipeline_result