# src\b3_data_collector\tick_by_tick\_partitioner.py

"""
B3 tick data partitioner — Stage 3.

Responsible for reading the raw daily Parquet produced by the extraction
stage, building the time-zone-aware ``timestamp`` column from
``trade_date`` and ``time_raw``, shaping the result to the tick contract
(``_contract.py``), recording the file's provenance in the Parquet footer,
persisting it under the feed-specific ticks directory, and uploading the
final Parquet to S3.

Both the equities (RV) and derivatives (DERIV) feeds share the same
contract — the feed distinction is relevant only for resolving the
correct input/output paths and S3 prefix via ``FeedType.config``.

Contract rules applied here:

- Rows keep the TXT order; nothing is sorted or dropped.
- The row count must equal the TXT data line count recorded by the
  extractor, or the date fails with ``ContractViolationError``.
- A ``time_raw`` that is not a valid ``HHMMSSmmm`` time yields a null
  ``timestamp``. The row is kept and counted in ``timestamp_null_count``;
  rejecting it is the consumer's call.

The raw Parquet produced by the extraction stage is intentionally not
uploaded to S3 — it is a disposable intermediate artifact, reproducible
at any time from the archived ZIP. It is deleted as soon as the ticks
Parquet has been written. Only this stage's output (the
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
import pyarrow as pa
import pyarrow.parquet as pq

from ..common import StageStatus, upload_file_to_s3
from ..config import settings
from ..paths import PATHS_B3
from ._contract import (
    OUTPUT_SCHEMA,
    TIMEZONE,
    ContractViolationError,
    SourceProvenance,
    TicksProvenance,
    with_provenance,
)
from ._feed import FeedType

logger = logging.getLogger(__name__)


# --- Module constants ---

_TIME_RAW_PATTERN    : Final[str] = r"\d{9}"  # HHMMSSmmm, B3 local time
_TIMESTAMP_FORMAT    : Final[str] = "%Y-%m-%d %H:%M:%S.%f"
_MAX_REPORTED_VALUES : Final[int] = 5


# --- Internal helpers ---

def _validate_output(
    df         : pd.DataFrame,
    source     : SourceProvenance,
    trade_date : date,
    label      : str,
) -> None:
    """
    Check the partitioned DataFrame before it is written.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame about to be written.
    source : SourceProvenance
        Provenance recorded by the extractor.
    trade_date : date
        Trading date, used for messages.
    label : str
        Feed label for messages.

    Raises
    ------
    ValueError
        If the DataFrame is empty.
    ContractViolationError
        If its row count differs from the TXT data line count.
    """
    if df.empty:
        raise ValueError(
            f"[{label}] Partitioned output for {trade_date} contains zero rows."
        )
    if len(df) != source.source_txt_row_count:
        raise ContractViolationError(
            f"[{label}] Partitioned output for {trade_date} has {len(df)} rows, "
            f"but '{source.source_txt_name}' has {source.source_txt_row_count}."
        )


def _build_timestamp(trade_date: pd.Series, time_raw: pd.Series) -> pd.Series:
    """
    Combine the trade date and the raw B3 time into a time-zone-aware timestamp.

    B3 publishes ``HoraFechamento`` in local time (UTC-3) as ``HHMMSSmmm``.
    The format is explicit, never inferred, and anything that is not
    exactly nine digits forming a valid time becomes null.

    Parameters
    ----------
    trade_date : pd.Series
        ``YYYY-MM-DD`` text, already validated by the extractor.
    time_raw : pd.Series
        Time text exactly as found in the TXT.

    Returns
    -------
    pd.Series
        Timestamps localised to the contract time zone.
    """
    is_well_formed = time_raw.str.fullmatch(_TIME_RAW_PATTERN)
    # Separators pin each field to two digits: without them "%H" also
    # accepts one digit, and "250000000" would parse as 02:50:00.
    combined = (
        trade_date + " "
        + time_raw.str.slice(0, 2) + ":" + time_raw.str.slice(2, 4) + ":"
        + time_raw.str.slice(4, 6) + "." + time_raw.str.slice(6, 9)
    ).where(is_well_formed)
    naive = pd.to_datetime(combined, format=_TIMESTAMP_FORMAT, errors="coerce")
    return naive.dt.tz_localize(TIMEZONE)


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
    Build the contract ticks Parquet from the raw daily Parquet.

    Builds the ``timestamp`` column from ``trade_date`` and ``time_raw``,
    shapes the result to the tick contract, validates it, writes it with
    its provenance to the feed-specific ticks directory, deletes the now
    redundant raw Parquet, and uploads the result to S3 (unless
    ``upload_to_s3=False``).

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
    ContractViolationError
        If the raw Parquet has no provenance, or the row count differs
        from the TXT data line count.
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
    raw_table = pq.read_table(raw_file)
    source    = SourceProvenance.from_schema(raw_table.schema)
    df        = raw_table.to_pandas()

    # --- Timestamp construction ---
    df["timestamp"] = _build_timestamp(df["trade_date"], df["time_raw"])

    # --- Validation ---
    _validate_output(df, source, trade_date, cfg.label)

    timestamp_null_count = int(df["timestamp"].isna().sum())
    if timestamp_null_count:
        unreadable = df.loc[df["timestamp"].isna(), "time_raw"].unique()
        logger.warning(
            "[%s] %d trade(s) on %s have an unreadable time; kept with a null "
            "timestamp. First values: %s",
            cfg.label, timestamp_null_count, trade_date,
            list(unreadable[:_MAX_REPORTED_VALUES]),
        )

    provenance = TicksProvenance.build(
        source               = source,
        row_count            = len(df),
        timestamp_null_count = timestamp_null_count,
    )
    table = with_provenance(
        pa.Table.from_pandas(df[OUTPUT_SCHEMA.names], schema=OUTPUT_SCHEMA, preserve_index=False),
        provenance,
    )

    # --- Persist locally ---
    if overwrite and output_file.exists():
        logger.info(
            "[%s] Removing existing tick file for overwrite: %s",
            cfg.label, output_file.name,
        )
        output_file.unlink()

    output_file.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_file)

    tick_count = len(df)
    logger.info(
        "[%s] Partition complete for %s — %d ticks → %s",
        cfg.label, trade_date, tick_count, output_file,
    )

    raw_file.unlink()
    logger.info("[%s] Removed raw Parquet: %s", cfg.label, raw_file.name)

    # --- Upload to S3 ---
    if upload_to_s3:
        s3_status = upload_file_to_s3(
            local_path = output_file,
            bucket     = settings.AWS_S3_BUCKET_B3,
            key        = _build_s3_key_ticks(cfg.s3_prefix_ticks, trade_date),
            overwrite  = overwrite,
        )

    return StageStatus.SUCCESS, s3_status, tick_count