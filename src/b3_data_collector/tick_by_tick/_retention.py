# src\b3_data_collector\tick_by_tick\_retention.py

"""
Local retention policy for tick-by-tick files.

Deletes local files whose trading date is older than the retention window,
so ``data/tick_by_tick/`` holds only recent days instead of growing forever.
S3 is the long-term store; the local window exists so a failed upload can
be retried without downloading the ZIP from B3 again.

Rules per artifact:

- ZIP and ticks Parquet: deleted only once confirmed in S3. A file missing
  from S3 is kept and logged, so a later run can still upload it.
- Raw Parquet: deleted without an S3 check — it is never uploaded and can
  be rebuilt from the ZIP. Normally the partitioner already removed it;
  this only catches leftovers from a failed partition.

The trading date is parsed from the file name using the same templates the
pipeline writes with. Files that do not match are never deleted.

Housekeeping must never break the daily run: S3 and filesystem errors are
logged and counted, and the affected files are kept. Does not download,
extract or upload anything.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Final

from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from ..common import build_s3_client, s3_object_exists, today_b3
from ..config import settings
from ..paths import PATHS_B3
from ._feed import FeedType
from ._partitioner import _build_s3_key_ticks

logger = logging.getLogger(__name__)

# --- Module constants ---

_PARQUET_NAME_FORMAT : Final[str]             = "%Y-%m-%d.parquet"
_DATE_PLACEHOLDER    : Final[re.Pattern[str]] = re.compile(r"\{date:([^}]*)\}")


# --- Result model ---

@dataclass
class RetentionResult:
    """
    Outcome of one retention pass for a single feed.

    Parameters
    ----------
    label : str
        Feed label for log messages.
    cutoff : date
        Files with a trading date strictly before this date were eligible.
    deleted : int
        Files removed from disk.
    kept_missing_in_s3 : int
        Eligible files kept because they are not in S3 yet.
    ignored : int
        Files whose name does not match the expected pattern.
    errors : int
        Files kept because an S3 check or the deletion itself failed.
    freed_bytes : int
        Total size of the deleted files.
    """

    label              : str
    cutoff             : date
    deleted            : int = 0
    kept_missing_in_s3 : int = 0
    ignored            : int = 0
    errors             : int = 0
    freed_bytes        : int = 0

    @property
    def summary_line(self) -> str:
        return (
            f"[{self.label}] Retention (before {self.cutoff}): "
            f"deleted={self.deleted}  freed={self.freed_bytes / 1024**2:,.1f} MB  "
            f"kept_missing_in_s3={self.kept_missing_in_s3}  "
            f"ignored={self.ignored}  errors={self.errors}"
        )


# --- Internal helpers ---

def _template_to_strptime(template: str) -> str:
    """
    Turn a ``FeedConfig`` file-name template into a ``strptime`` format.

    ``"{date:%Y%m%d}_NEGOCIOSAVISTA.zip"`` → ``"%Y%m%d_NEGOCIOSAVISTA.zip"``.

    Parameters
    ----------
    template : str
        Template with a single ``{date:<strftime directives>}`` placeholder.

    Returns
    -------
    str
        Format string accepted by ``datetime.strptime``.
    """
    return _DATE_PLACEHOLDER.sub(r"\1", template)


def _parse_trade_date(file_name: str, name_format: str) -> date | None:
    """
    Parse the trading date from a file name.

    Parameters
    ----------
    file_name : str
        Name of the local file, without directories.
    name_format : str
        ``strptime`` format the name must match exactly.

    Returns
    -------
    date | None
        Trading date encoded in the name, or ``None`` if it does not match.
    """
    try:
        return datetime.strptime(file_name, name_format).date()
    except ValueError:
        return None


def _delete(path: Path, result: RetentionResult) -> None:
    """
    Delete ``path`` and record it, keeping the file if the OS refuses.

    Parameters
    ----------
    path : Path
        File to remove.
    result : RetentionResult
        Counters to update.
    """
    size = path.stat().st_size
    try:
        path.unlink()
    except OSError as exc:
        # On Windows a file open in another process cannot be deleted.
        logger.warning("[%s] Could not delete %s: %s", result.label, path.name, exc)
        result.errors += 1
        return
    result.deleted     += 1
    result.freed_bytes += size


def _prune_directory(
    directory   : Path,
    name_format : str,
    cutoff      : date,
    result      : RetentionResult,
    s3_key      : Callable[[date, str], str] | None = None,
    client      : BaseClient | None = None,
) -> None:
    """
    Apply the retention rules to every file in one local directory.

    Parameters
    ----------
    directory : Path
        Local directory to scan.
    name_format : str
        ``strptime`` format of the file names in ``directory``.
    cutoff : date
        Files with a trading date strictly before this date are eligible.
    result : RetentionResult
        Counters to update.
    s3_key : Callable[[date, str], str] | None, optional
        Builds the S3 key from ``(trade_date, file_name)``. ``None`` deletes
        eligible files without checking S3.
    client : BaseClient | None, optional
        S3 client, required when ``s3_key`` is given.

    Raises
    ------
    ValueError
        If only one of ``s3_key`` and ``client`` is given.
    """
    # Guard against deleting S3-backed files without actually checking S3.
    if (s3_key is None) != (client is None):
        raise ValueError("s3_key and client must be given together.")

    if not directory.is_dir():
        return

    for path in sorted(directory.iterdir()):
        if not path.is_file():
            continue

        trade_date = _parse_trade_date(path.name, name_format)
        if trade_date is None:
            logger.info("[%s] Ignoring unrecognised file: %s", result.label, path.name)
            result.ignored += 1
            continue

        if trade_date >= cutoff:
            continue

        if s3_key is not None:
            key = s3_key(trade_date, path.name)
            try:
                in_s3 = s3_object_exists(client, settings.AWS_S3_BUCKET_B3, key)
            except (BotoCoreError, ClientError) as exc:
                logger.warning(
                    "[%s] S3 check failed for %s, keeping it: %s",
                    result.label, path.name, exc,
                )
                result.errors += 1
                continue

            if not in_s3:
                logger.warning(
                    "[%s] Not in S3, keeping for a later upload: %s",
                    result.label, path.name,
                )
                result.kept_missing_in_s3 += 1
                continue

        _delete(path, result)


# --- Public API ---

def prune_local_files(
    feed           : FeedType,
    retention_days : int,
    today          : date | None = None,
) -> RetentionResult:
    """
    Delete local files of ``feed`` older than the retention window.

    Parameters
    ----------
    feed : FeedType
        Feed selector (``FeedType.RV`` or ``FeedType.DERIV``).
    retention_days : int
        Number of calendar days to keep. Files with a trading date strictly
        before ``today - retention_days`` are eligible for deletion.
    today : date | None, optional
        Reference date. Default is ``today_b3()``; tests pass a fixed date.

    Returns
    -------
    RetentionResult
        Counters describing what was deleted, kept and ignored.

    Raises
    ------
    ValueError
        If ``retention_days`` is negative.
    """
    if retention_days < 0:
        raise ValueError(f"retention_days must be >= 0, got {retention_days}.")

    cfg    = feed.config
    cutoff = (today or today_b3()) - timedelta(days=retention_days)
    result = RetentionResult(label=cfg.label, cutoff=cutoff)

    # Raw files need no S3 check, so they are pruned even if S3 is unreachable.
    _prune_directory(
        directory   = PATHS_B3[cfg.paths_key_raw],
        name_format = _PARQUET_NAME_FORMAT,
        cutoff      = cutoff,
        result      = result,
    )

    try:
        client = build_s3_client()
    except (BotoCoreError, ClientError) as exc:
        logger.warning(
            "[%s] S3 client unavailable, skipping S3-backed files: %s", cfg.label, exc
        )
        result.errors += 1
        return result

    _prune_directory(
        directory   = PATHS_B3[cfg.paths_key_downloads],
        name_format = _template_to_strptime(cfg.zip_name_template),
        cutoff      = cutoff,
        result      = result,
        s3_key      = lambda _trade_date, name: cfg.s3_prefix + name,
        client      = client,
    )
    _prune_directory(
        directory   = PATHS_B3[cfg.paths_key_ticks],
        name_format = _PARQUET_NAME_FORMAT,
        cutoff      = cutoff,
        result      = result,
        s3_key      = lambda trade_date, _name: _build_s3_key_ticks(
            cfg.s3_prefix_ticks, trade_date
        ),
        client      = client,
    )

    logger.info(result.summary_line)
    return result
