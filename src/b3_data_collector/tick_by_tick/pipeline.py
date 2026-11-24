# src\b3_data_collector\tick_by_tick\pipeline.py

"""
B3 intraday tick-data ingestion pipeline.

Orchestrates the four-stage data ingestion process for B3 tick data,
supporting both the equities (RV) and derivatives (DERIV) feeds via the
``FeedType`` parameter:

    Stage 0 — Check     : unless ``overwrite=True`` or ``upload_to_s3=False``,
                          skip a date whose ZIP and ticks are both in S3.
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
from datetime import date

from botocore.exceptions import BotoCoreError, ClientError

from ..common import (
    StageStatus,
    build_s3_client,
    require_s3_bucket,
    resolve_dates,
    s3_object_exists,
)
from ..config import settings
from ._downloader import _build_s3_key_zip, download_zip
from ._extractor import extract_to_raw_parquet
from ._feed import FeedType
from ._models import DateResult, PipelineResult
from ._partitioner import _build_s3_key_ticks, partition_to_ticks

logger = logging.getLogger(__name__)


# --- Internal helpers ---

def _append_error(result: DateResult, message: str) -> None:
    """
    Add ``message`` to ``result.error`` without discarding earlier errors.

    A failed ZIP upload does not stop the later stages, so a single date
    can collect more than one error before the run ends.

    Parameters
    ----------
    result : DateResult
        Result being built for the current date.
    message : str
        Human-readable description of the failure.
    """
    result.error = message if result.error is None else f"{result.error}; {message}"


def _is_archived_in_s3(trade_date: date, feed: FeedType) -> bool:
    """
    Check whether both S3 artifacts of a date already exist.

    A date counts as archived only when the raw ZIP **and** the ticks
    Parquet are in S3: if either is missing, the pipeline must still run
    to produce and upload it.

    Parameters
    ----------
    trade_date : date
        Trading date to check.
    feed : FeedType
        Feed selector (``FeedType.RV`` or ``FeedType.DERIV``).

    Returns
    -------
    bool
        ``True`` if both objects exist.

    Raises
    ------
    botocore.exceptions.BotoCoreError
        For client-side or connection errors.
    botocore.exceptions.ClientError
        For any S3 error response other than a 404 (e.g. access denied).
    """
    cfg    = feed.config
    client = build_s3_client()
    bucket = settings.AWS_S3_BUCKET_B3
    return (
        s3_object_exists(client, bucket, _build_s3_key_zip(cfg, trade_date))
        and s3_object_exists(client, bucket, _build_s3_key_ticks(cfg.s3_prefix_ticks, trade_date))
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
        Re-run stages even when output already exists. When ``False`` and
        ``upload_to_s3`` is ``True``, a date already archived in S3 is
        skipped before any stage runs.

    Returns
    -------
    DateResult
    """
    result = DateResult(trade_date=trade_date)

    # --- Stage 0: Skip dates already archived ---
    # Local files are not a reliable "done" marker once retention deletes
    # them; S3 is. Two HEAD requests replace a ~50 MB download from B3.
    # Only when uploading: a local-only run (upload_to_s3=False) wants the
    # local files, whatever S3 holds.
    if upload_to_s3 and not overwrite:
        try:
            if _is_archived_in_s3(trade_date, feed):
                logger.info(
                    "[%s] Already in S3, skipping %s.", feed.config.label, trade_date
                )
                return result
        except (BotoCoreError, ClientError) as exc:
            # Not fatal: running the stages is still correct, and a real S3
            # problem will surface as a failed upload.
            logger.warning(
                "[%s] S3 check failed for %s, running the stages: %s",
                feed.config.label, trade_date, exc,
            )

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

            # Not fatal: later stages run locally, and the ZIP stays on disk
            # so a later run can upload it again.
            if s3_status is StageStatus.FAILED:
                _append_error(result, "S3 upload failed: ZIP archive")

            if dl_status is StageStatus.UNAVAILABLE:
                _append_error(result, f"No data available on B3 for {trade_date}.")
                return result

            if dl_status is StageStatus.FAILED:
                _append_error(result, f"Download failed for {trade_date}.")
                return result

        except Exception as exc:
            logger.exception(
                "Unexpected error in download stage for %s [%s]",
                trade_date, feed.config.label,
            )
            result.download = StageStatus.FAILED
            _append_error(result, f"Download exception: {exc}")
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
            _append_error(result, str(exc))
            return result
        except (KeyError, ValueError) as exc:
            logger.error("Extract error for %s: %s", trade_date, exc)
            result.extract = StageStatus.FAILED
            _append_error(result, str(exc))
            return result
        except Exception as exc:
            logger.exception(
                "Unexpected error in extract stage for %s [%s]",
                trade_date, feed.config.label,
            )
            result.extract = StageStatus.FAILED
            _append_error(result, f"Extract exception: {exc}")
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

            if ticks_s3_status is StageStatus.FAILED:
                _append_error(result, "S3 upload failed: ticks Parquet")
        except FileNotFoundError as exc:
            logger.error("Partition FileNotFoundError for %s: %s", trade_date, exc)
            result.partition = StageStatus.FAILED
            _append_error(result, str(exc))
        except ValueError as exc:
            logger.error("Partition validation error for %s: %s", trade_date, exc)
            result.partition = StageStatus.FAILED
            _append_error(result, str(exc))
        except Exception as exc:
            logger.exception(
                "Unexpected error in partition stage for %s [%s]",
                trade_date, feed.config.label,
            )
            result.partition = StageStatus.FAILED
            _append_error(result, f"Partition exception: {exc}")

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

    Raises
    ------
    ValueError
        If ``upload_to_s3`` is ``True`` and ``AWS_S3_BUCKET_B3`` is not set.
    """
    if upload_to_s3:
        require_s3_bucket(settings.AWS_S3_BUCKET_B3)

    date_list       = resolve_dates(dates)
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