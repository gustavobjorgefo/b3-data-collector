# src\b3_data_collector\tick_by_tick\backfill.py

"""
Rebuild the ticks Parquet in S3 from the ZIPs archived in S3.

Used when the tick contract changes: every archived ZIP is the immutable
source, so every ticks file can be rewritten with the current code. For
each feed and date:

1. Download the archived ZIP from S3 into the local downloads directory.
2. Run the pipeline locally with ``download=False`` and ``overwrite=True``:
   extract, partition, and upload the ticks Parquet over the S3 object.
3. Delete the local ZIP and ticks (unless ``--keep-local``), so disk use
   stays at one day at a time.
4. Append one row to a CSV report, written as it goes so an interrupted
   run keeps its progress.

A date whose ticks in S3 were already written by this exact commit (clean
checkout) is skipped, so an interrupted run can simply be started again.
``--force`` rewrites everything.

At the end, a verification pass reads only the footer of every ticks
Parquet in S3 and checks it against the contract: schema, schema version,
row count against the TXT, and a ticks file for every archived ZIP.
``--verify-only`` runs just that pass.

Never touches the archived ZIPs. Do not run it while the daily pipeline
is running: both would write the same local files.

Usage, from the repository root with the virtual environment active::

    python -m b3_data_collector.tick_by_tick.backfill --dry-run
    python -m b3_data_collector.tick_by_tick.backfill --feed DERIV --dates 2026-11-13
    python -m b3_data_collector.tick_by_tick.backfill
    python -m b3_data_collector.tick_by_tick.backfill --verify-only
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import sys
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime
from pathlib import Path
from typing import Final, TextIO

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from ..common import build_s3_client, download_file_from_s3, require_s3_bucket
from ..config import settings
from ..paths import LOGS_DIR, PATHS_B3
from ._contract import METADATA_KEY, OUTPUT_SCHEMA, SCHEMA_VERSION, collector_commit
from ._downloader import _build_s3_key_zip
from ._feed import FeedType
from ._partitioner import _build_s3_key_ticks
from ._retention import _parse_trade_date, _template_to_strptime
from .pipeline import run_pipeline

logger = logging.getLogger(__name__)


# --- Module constants ---

_PARQUET_MAGIC       : Final[bytes] = b"PAR1"
_PARQUET_TAIL_LENGTH : Final[int]   = 8  # 4-byte footer length + magic
_TICKS_NAME_FORMAT   : Final[str]   = "%Y-%m-%d.parquet"
_STATUS_OK           : Final[str]   = "OK"
_STATUS_FAILED       : Final[str]   = "FAILED"
_STATUS_SKIPPED      : Final[str]   = "SKIPPED"


# --- Report ---

@dataclass
class BackfillRow:
    """
    Outcome of the backfill for one feed and date; one row of the report.

    Parameters
    ----------
    feed : str
        Feed name.
    trade_date : str
        Trading date, ``YYYY-MM-DD``.
    status : str
        ``OK``, ``FAILED`` or ``SKIPPED`` (already written by this commit).
    row_count : int | None
        Rows in the new ticks Parquet.
    timestamp_null_count : int | None
        Rows with an unreadable time.
    other_session_row_count : int | None
        Rows whose ``trade_date`` differs from ``reference_date``.
    seconds : float
        Wall time spent on the date.
    error : str
        Failure description, empty on success.
    """

    feed                    : str
    trade_date              : str
    status                  : str
    row_count               : int | None = None
    timestamp_null_count    : int | None = None
    other_session_row_count : int | None = None
    seconds                 : float      = 0.0
    error                   : str        = ""


class _ReportWriter:
    """CSV report flushed after every row, so a crash loses nothing."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path  : Path   = path
        self._file : TextIO = path.open("w", newline="", encoding="utf-8")
        csv.writer(self._file).writerow([field.name for field in fields(BackfillRow)])

    def write(self, row: BackfillRow) -> None:
        csv.writer(self._file).writerow(asdict(row).values())
        self._file.flush()

    def close(self) -> None:
        self._file.close()


# --- S3 helpers ---

def _list_keys(client: BaseClient, bucket: str, prefix: str) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    return [
        item["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
    ]


def list_archived_dates(client: BaseClient, bucket: str, feed: FeedType) -> list[date]:
    """
    List the trading dates that have an archived ZIP in S3.

    Parameters
    ----------
    client : BaseClient
        S3 client.
    bucket : str
        Bucket holding the archive.
    feed : FeedType
        Feed to list.

    Returns
    -------
    list[date]
        Sorted dates. Keys whose name does not follow the feed's ZIP
        template are ignored.
    """
    cfg = feed.config
    name_format = _template_to_strptime(cfg.zip_name_template)
    dates = {
        parsed
        for key in _list_keys(client, bucket, cfg.s3_prefix)
        if (parsed := _parse_trade_date(key.rsplit("/", 1)[-1], name_format)) is not None
    }
    return sorted(dates)


def _read_remote_schema(client: BaseClient, bucket: str, key: str) -> pa.Schema | None:
    """
    Read a Parquet schema from S3 by downloading only the file footer.

    Returns
    -------
    pa.Schema | None
        The schema with its metadata, or ``None`` if the object does not
        exist.

    Raises
    ------
    ValueError
        If the object is not a Parquet file.
    """
    def tail(length: int) -> bytes:
        response = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=-{length}")
        return response["Body"].read()

    try:
        last_bytes = tail(_PARQUET_TAIL_LENGTH)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return None
        raise
    if last_bytes[4:] != _PARQUET_MAGIC:
        raise ValueError(f"s3://{bucket}/{key} is not a Parquet file.")

    footer_length = int.from_bytes(last_bytes[:4], "little")
    footer = tail(footer_length + _PARQUET_TAIL_LENGTH)
    # pyarrow only needs the leading magic and the footer to read the schema.
    return pq.read_schema(io.BytesIO(_PARQUET_MAGIC + footer))


def _provenance_of(schema: pa.Schema | None) -> dict[str, object] | None:
    if schema is None or METADATA_KEY not in (schema.metadata or {}):
        return None
    return json.loads(schema.metadata[METADATA_KEY])


# --- Per-date backfill ---

def _written_by_this_commit(provenance: dict[str, object] | None) -> bool:
    # A dirty checkout is not a reproducible build: never trust it to skip.
    commit = collector_commit()
    return (
        provenance is not None
        and commit is not None
        and not commit.endswith("+dirty")
        and provenance.get("collector_commit") == commit
        and provenance.get("schema_version") == SCHEMA_VERSION
    )


def _local_files(feed: FeedType, trade_date: date) -> tuple[Path, Path, Path]:
    cfg = feed.config
    return (
        PATHS_B3[cfg.paths_key_downloads] / cfg.zip_name_template.format(date=trade_date),
        PATHS_B3[cfg.paths_key_raw] / f"{trade_date}.parquet",
        PATHS_B3[cfg.paths_key_ticks] / f"{trade_date}.parquet",
    )


def _backfill_date(feed: FeedType, trade_date: date, bucket: str, keep_local: bool) -> BackfillRow:
    """
    Rebuild and upload the ticks Parquet of one date.

    Never raises: any failure is returned in the row, so one bad date does
    not stop the run.
    """
    started = time.monotonic()
    row = BackfillRow(feed=feed.name, trade_date=f"{trade_date}", status=_STATUS_FAILED)
    zip_path, raw_path, ticks_path = _local_files(feed, trade_date)

    try:
        download_file_from_s3(bucket, _build_s3_key_zip(feed.config, trade_date), zip_path)
        result = run_pipeline(
            trade_date, feed, download=False, upload_to_s3=True, overwrite=True
        ).results[0]

        if not result.succeeded:
            row.error = result.error or "unknown pipeline failure"
            return row

        provenance = _provenance_of(pq.read_schema(ticks_path))
        dates = pq.read_table(ticks_path, columns=["trade_date", "reference_date"])
        row.status = _STATUS_OK
        row.row_count = provenance["row_count"]
        row.timestamp_null_count = provenance["timestamp_null_count"]
        differs = pc.not_equal(dates["trade_date"], dates["reference_date"])
        row.other_session_row_count = pc.sum(pc.cast(differs, pa.int64())).as_py() or 0
    # Any failure belongs to this date only: report it and go on, like the
    # pipeline does per stage.
    except Exception as exc:
        logger.exception("[%s] Backfill failed for %s", feed.name, trade_date)
        row.error = f"{type(exc).__name__}: {exc}"
    finally:
        if not keep_local:
            for path in (zip_path, raw_path, ticks_path):
                path.unlink(missing_ok=True)
        row.seconds = round(time.monotonic() - started, 1)

    return row


# --- Verification ---

def _verify_ticks_file(
    client     : BaseClient,
    bucket     : str,
    feed       : FeedType,
    trade_date : date,
) -> str | None:
    key = _build_s3_key_ticks(feed.config.s3_prefix_ticks, trade_date)
    schema = _read_remote_schema(client, bucket, key)
    if schema is None:
        return "no ticks Parquet"
    provenance = _provenance_of(schema)
    if provenance is None:
        return "pre-contract file (no provenance)"
    if provenance.get("schema_version") != SCHEMA_VERSION:
        return f"schema_version {provenance.get('schema_version')}"
    if not schema.remove_metadata().equals(OUTPUT_SCHEMA):
        return "schema differs from the contract"
    if provenance.get("row_count") != provenance.get("source_txt_row_count"):
        return (
            f"row_count {provenance.get('row_count')} != "
            f"source_txt_row_count {provenance.get('source_txt_row_count')}"
        )
    return None


def verify(feeds: Sequence[FeedType]) -> list[str]:
    """
    Check every ticks Parquet in S3 against the contract.

    Reads only the Parquet footers. Every date with an archived ZIP must
    have a ticks file with the current schema version, the contract
    schema, and as many rows as its TXT. Ticks files without a ZIP are
    reported too.

    Parameters
    ----------
    feeds : Sequence[FeedType]
        Feeds to verify.

    Returns
    -------
    list[str]
        One message per problem; empty when everything conforms.
    """
    client = build_s3_client()
    bucket = require_s3_bucket(settings.AWS_S3_BUCKET_B3)
    problems: list[str] = []

    for feed in feeds:
        archived = list_archived_dates(client, bucket, feed)
        ticks_dates = {
            parsed
            for key in _list_keys(client, bucket, feed.config.s3_prefix_ticks)
            if (parsed := _parse_trade_date(key.rsplit("/", 1)[-1], _TICKS_NAME_FORMAT))
        }
        for trade_date in sorted(ticks_dates - set(archived)):
            problems.append(f"{feed.name} {trade_date}: ticks Parquet without archived ZIP")
        for trade_date in archived:
            problem = _verify_ticks_file(client, bucket, feed, trade_date)
            if problem is not None:
                problems.append(f"{feed.name} {trade_date}: {problem}")

        logger.info(
            "[%s] Verified %d archived date(s); %d ticks file(s) in S3.",
            feed.name, len(archived), len(ticks_dates),
        )

    return problems


# --- Public API ---

def run_backfill(
    feeds       : Sequence[FeedType],
    report_path : Path,
    dates       : Sequence[date] | None = None,
    *,
    dry_run     : bool = False,
    keep_local  : bool = False,
    force       : bool = False,
) -> list[BackfillRow]:
    """
    Rebuild the ticks Parquet in S3 for the archived dates of ``feeds``.

    Parameters
    ----------
    feeds : Sequence[FeedType]
        Feeds to backfill, processed in order.
    report_path : Path
        CSV report, one row per date, written as the run goes.
    dates : Sequence[date] | None, optional
        Only these dates. Default: every date with an archived ZIP.
        A requested date without an archived ZIP is reported as failed.
    dry_run : bool, optional
        Only report what would be done; nothing is downloaded or written
        except the report.
    keep_local : bool, optional
        Keep the local ZIP and ticks of each date. Default deletes them.
    force : bool, optional
        Rebuild dates already written by this commit as well.

    Returns
    -------
    list[BackfillRow]
        One row per feed and date, in processing order.
    """
    client = build_s3_client()
    bucket = require_s3_bucket(settings.AWS_S3_BUCKET_B3)
    report = _ReportWriter(report_path)
    rows: list[BackfillRow] = []

    try:
        for feed in feeds:
            archived = list_archived_dates(client, bucket, feed)
            wanted = sorted(set(dates)) if dates is not None else archived
            logger.info("[%s] %d date(s) to consider.", feed.name, len(wanted))

            for position, trade_date in enumerate(wanted, start=1):
                if trade_date not in archived:
                    row = BackfillRow(
                        feed=feed.name, trade_date=f"{trade_date}", status=_STATUS_FAILED,
                        error="no archived ZIP in S3",
                    )
                elif not force and _written_by_this_commit(_provenance_of(_read_remote_schema(
                    client, bucket, _build_s3_key_ticks(feed.config.s3_prefix_ticks, trade_date)
                ))):
                    row = BackfillRow(
                        feed=feed.name, trade_date=f"{trade_date}", status=_STATUS_SKIPPED,
                    )
                elif dry_run:
                    row = BackfillRow(
                        feed=feed.name, trade_date=f"{trade_date}", status="WOULD_RUN",
                    )
                else:
                    row = _backfill_date(feed, trade_date, bucket, keep_local)

                logger.info(
                    "[%s] %d/%d %s %s %s",
                    feed.name, position, len(wanted), trade_date, row.status, row.error,
                )
                report.write(row)
                rows.append(row)
    finally:
        report.close()

    return rows


# --- Entrypoint ---

def _configure_logging(log_file: Path) -> None:
    formatter = logging.Formatter(
        fmt     = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, "a", "utf-8")):
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)


def _parse_arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog        = "python -m b3_data_collector.tick_by_tick.backfill",
        description = "Rebuild the ticks Parquet in S3 from the archived ZIPs.",
    )
    parser.add_argument("--feed", choices=["ALL", *(feed.name for feed in FeedType)],
                        default="ALL")
    parser.add_argument("--dates", nargs="+", type=date.fromisoformat,
                        help="only these dates (YYYY-MM-DD); default: all archived")
    parser.add_argument("--dry-run", action="store_true",
                        help="list what would run; download and write nothing")
    parser.add_argument("--keep-local", action="store_true",
                        help="keep each date's local ZIP and ticks")
    parser.add_argument("--force", action="store_true",
                        help="also rebuild dates already written by this commit")
    parser.add_argument("--verify-only", action="store_true",
                        help="only check the ticks in S3 against the contract")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """
    Command-line entrypoint; see the module docstring for usage.

    Returns
    -------
    int
        ``0`` when every date succeeded (or was skipped) and verification
        found no problem, ``1`` otherwise.
    """
    arguments = _parse_arguments(argv)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    _configure_logging(LOGS_DIR / "backfill_tick_by_tick.log")

    feeds = list(FeedType) if arguments.feed == "ALL" else [FeedType[arguments.feed]]
    logger.info("Backfill commit: %s", collector_commit())

    failed: list[BackfillRow] = []
    if not arguments.verify_only:
        report_path = LOGS_DIR / f"backfill_tick_by_tick_{stamp}.csv"
        rows = run_backfill(
            feeds, report_path, arguments.dates,
            dry_run=arguments.dry_run, keep_local=arguments.keep_local, force=arguments.force,
        )
        failed = [row for row in rows if row.status == _STATUS_FAILED]
        counts = {status: sum(row.status == status for row in rows) for status in
                  sorted({row.status for row in rows})}
        logger.info("Backfill finished: %s. Report: %s", counts, report_path)
        for row in failed:
            logger.error("FAILED %s %s: %s", row.feed, row.trade_date, row.error)
        if arguments.dry_run:
            return 1 if failed else 0

    problems = verify(feeds)
    for problem in problems:
        logger.error("VERIFY %s", problem)
    logger.info("Verification: %d problem(s).", len(problems))

    return 1 if failed or problems else 0


if __name__ == "__main__":
    sys.exit(main())
