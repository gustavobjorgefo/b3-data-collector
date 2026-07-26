# src\b3_data_collector\tick_by_tick\_partitioner.py

"""
B3 tick data partitioner — Stage 3.

Responsible for reading the normalised daily Parquet produced by the
extraction stage, constructing a combined timestamp column from the
``trade_date`` and ``time`` fields, selecting the canonical output columns,
validating the result, persisting it under the feed-specific ticks
directory, and uploading the final Parquet to S3.

Both the equities (RV) and derivatives (DERIV) feeds share the same
canonical output schema — the feed distinction is relevant only for
resolving the correct input/output paths and S3 prefix via
``FeedType.config``.

The raw Parquet produced by the extraction stage is intentionally not
uploaded to S3 — it is a disposable intermediate artifact, reproducible
at any time from the archived ZIP. Only this stage's output (the
canonical, timestamped ticks Parquet) is persisted to S3, under a
Hive-style partition:

    b3/tick_by_tick/{rv|deriv}/ticks/year={YYYY}/{YYYY-MM-DD}.parquet

All input columns are expected to carry canonical English names as
produced by ``_extractor.py``.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Final

import pandas as pd

from ..common import StageStatus, upload_file_to_s3
from ..config import settings
from ..paths import PATHS_B3
from ._feed import FeedType

logger = logging.getLogger(__name__)


# --- Module constants ---

_OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "symbol",
    "trade_date",
    "timestamp",
    "price",
    "quantity",
    "update_action",
    "session_type",
    "channel_type",
    "trade_id",
    "buyer_broker",
    "seller_broker",
)

# Warn if more than this fraction of timestamp values could not be parsed.
_MAX_NAT_RATIO: Final[float] = 0.01


# --- Internal helpers ---

def _validate_output(df: pd.DataFrame, trade_date: date, label: str) -> None:
    """
    Run post-write sanity checks on the partitioned DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame that was written to disk.
    trade_date : date
        Trading date, used for log context.
    label : str
        Feed label for log messages.

    Raises
    ------
    ValueError
        If the DataFrame is empty.
    """
    if df.empty:
        raise ValueError(
            f"[{label}] Partitioned output for {trade_date} contains zero rows."
        )

    nat_count = df["timestamp"].isna().sum()
    nat_ratio = nat_count / len(df)
    if nat_ratio > _MAX_NAT_RATIO:
        logger.warning(
            "[%s] High NaT ratio in timestamp column for %s: %.1f%% (%d rows). "
            "Check raw time field integrity.",
            label, trade_date, nat_ratio * 100, nat_count,
        )


def _build_s3_key_ticks(s3_prefix_ticks: str, trade_date: date) -> str:
    """
    Build the Hive-style S3 key for a processed ticks Parquet file.

    Parameters
    ----------
    s3_prefix_ticks : str
        Feed-specific prefix, from ``FeedConfig.s3_prefix_ticks``.
    trade_date : date
        Trading date the file refers to.

    Returns
    -------
    str
        Full S3 key, e.g. ``"b3/tick_by_tick/rv/ticks/year=2026/2026-06-26.parquet"``.
    """
    return f"{s3_prefix_ticks}year={trade_date:%Y}/{trade_date}.parquet"


# --- Public API ---

def partition_to_ticks(
    trade_date   : date,
    feed         : FeedType,
    overwrite    : bool = False,
    upload_to_s3 : bool = True,
) -> tuple[StageStatus, StageStatus, int | None]:
    """
    Build a tick-level Parquet from the normalised daily raw Parquet.

    Constructs a combined ``timestamp`` column from ``trade_date`` and
    ``time``, selects the canonical output columns, validates the result,
    writes to the feed-specific ticks directory, and uploads the result
    to S3 (unless ``upload_to_s3=False``).

    Parameters
    ----------
    trade_date : date
        Trading date to partition.
    feed : FeedType
        Feed selector (``FeedType.RV`` or ``FeedType.DERIV``).
    overwrite : bool, optional
        If ``True``, removes the existing local file before writing and
        re-uploads to S3 even if the object already exists.
        Default is ``False``.
    upload_to_s3 : bool, optional
        If ``True``, upload the resulting Parquet to S3 after writing it
        locally. Default is ``True``.

    Returns
    -------
    tuple[StageStatus, StageStatus, int | None]
        ``(partition_status, s3_status, tick_count)``

        - ``partition_status`` reflects the local write.
        - ``s3_status`` is ``StageStatus.SKIPPED`` when ``upload_to_s3``
          is ``False`` or when partitioning itself did not run.
        - ``tick_count`` is the number of rows written, or ``None`` if
          the stage did not complete successfully.

    Raises
    ------
    FileNotFoundError
        If the expected raw Parquet does not exist.
    ValueError
        If the partitioned output contains zero rows.
    """
    cfg         = feed.config
    raw_file    = PATHS_B3[cfg.paths_key_raw]   / f"{trade_date}.parquet"
    output_file = PATHS_B3[cfg.paths_key_ticks] / f"{trade_date}.parquet"

    s3_status = StageStatus.SKIPPED

    if output_file.exists() and not overwrite:
        logger.info(
            "[%s] Skipping partition for %s — tick file exists.",
            cfg.label, trade_date,
        )
        if upload_to_s3:
            s3_status = upload_file_to_s3(
                local_path = output_file,
                bucket     = settings.AWS_S3_BUCKET_B3,
                key        = _build_s3_key_ticks(cfg.s3_prefix_ticks, trade_date),
            )
        return StageStatus.SKIPPED, s3_status, None

    if not raw_file.exists():
        raise FileNotFoundError(
            f"[{cfg.label}] Raw Parquet not found: {raw_file}"
        )

    logger.info("[%s] Loading raw Parquet for %s", cfg.label, trade_date)
    df = pd.read_parquet(raw_file)

    # --- Timestamp construction ---
    df["timestamp"] = pd.to_datetime(
        df["trade_date"].astype(str) + " " + df["time"].astype(str),
        errors="coerce",
    )

    df = df[list(_OUTPUT_COLUMNS)]

    # --- Validation ---
    _validate_output(df, trade_date, cfg.label)

    # --- Persist locally ---
    if overwrite and output_file.exists():
        logger.info(
            "[%s] Removing existing tick file for overwrite: %s",
            cfg.label, output_file.name,
        )
        output_file.unlink()

    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_file, index=False)

    tick_count = len(df)
    logger.info(
        "[%s] Partition complete for %s — %d ticks → %s",
        cfg.label, trade_date, tick_count, output_file,
    )

    # --- Upload to S3 ---
    if upload_to_s3:
        s3_status = upload_file_to_s3(
            local_path = output_file,
            bucket     = settings.AWS_S3_BUCKET_B3,
            key        = _build_s3_key_ticks(cfg.s3_prefix_ticks, trade_date),
            overwrite  = overwrite,
        )

    return StageStatus.SUCCESS, s3_status, tick_count