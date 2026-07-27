# b3_data_collector/common.py

"""
Shared status vocabulary, date-resolution helpers, and S3 access helpers
used across the BDI, tick-by-tick, and reader subpackages.

Kept as a single module so all subpackages agree on the same set of
pipeline-stage outcomes, the same date input contract, and reuse the
same S3 client and upload/download logic — rather than each defining
its own incompatible copy.
"""

from __future__ import annotations

import io
import logging
from datetime import date, datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import Final
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .config import settings

logger = logging.getLogger(__name__)

# --- Module constants ---

_WEEKEND_CUTOFF: Final[int] = 5  # weekday() returns 0–4 for Mon–Fri

B3_TIMEZONE: Final = ZoneInfo("America/Sao_Paulo")


class StageStatus(Enum):
    """
    Outcome of a single pipeline stage (download, extract, upload, etc.)
    for one trading date or report.

    Attributes
    ----------
    SUCCESS :
        Stage completed and produced its expected output.
    SKIPPED :
        Output already existed and ``overwrite=False``.
    UNAVAILABLE :
        Data not available on B3 (holiday, weekend, or 404).
    FAILED :
        Stage raised an exception; see the caller's ``error`` field.
    """

    SUCCESS     = auto()
    SKIPPED     = auto()
    UNAVAILABLE = auto()
    FAILED      = auto()


# --- Timezone-aware "today" ---

def today_b3() -> date:
    """
    Return today's date in B3's local timezone (America/Sao_Paulo).

    Prefer this over ``date.today()`` for anything computing "today" or
    "yesterday" relative to a B3 trading session — ``date.today()`` uses
    the timezone of the machine running the code, which drifts from B3's
    actual trading day near midnight BRT if that machine runs in a
    different timezone (e.g. a server or CI runner in UTC).
    """
    return datetime.now(tz=B3_TIMEZONE).date()


# --- Date resolution ---

def parse_date(value: str | date) -> date:
    """Coerce a ``str`` (``YYYY-MM-DD``) or ``date`` to a ``date`` object."""
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def business_days_in_range(start: date, end: date) -> list[date]:
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


def resolve_dates(
    dates: str | date | list[str | date] | tuple[str | date, str | date],
) -> list[date]:
    """
    Resolve any accepted date input format into a sorted list of unique dates.

    Shared date input contract used by the BDI pipeline, the tick-by-tick
    pipeline, and the reader:

    - Single date  : ``"2026-06-26"`` or ``date(2026, 6, 26)``
    - Explicit list: ``["2026-06-26", "2026-06-27"]``
    - Date range   : ``("2026-05-29", "2026-06-27")`` — expanded to business days

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
        return [parse_date(dates)]

    if isinstance(dates, tuple):
        if len(dates) != 2:
            raise ValueError(
                f"A tuple input must have exactly 2 elements (start, end). "
                f"Got {len(dates)}."
            )
        return business_days_in_range(parse_date(dates[0]), parse_date(dates[1]))

    if isinstance(dates, list):
        return sorted({parse_date(d) for d in dates})

    raise TypeError(
        f"Unsupported dates type: {type(dates).__name__}. "
        "Expected str, date, list, or tuple."
    )


# --- S3 access ---

def build_s3_client() -> "boto3.client":
    """
    Build a boto3 S3 client from the module-level settings.

    Returns
    -------
    boto3.client
        Configured S3 client for the ``b3-data-collector`` bucket.
    """
    return boto3.client(
        "s3",
        region_name           = settings.AWS_S3_REGION,
        aws_access_key_id     = settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY,
    )


def s3_object_exists(client: "boto3.client", bucket: str, key: str) -> bool:
    """
    Check whether an object exists in S3 without downloading it.

    Parameters
    ----------
    client : boto3.client
        S3 client, as returned by ``build_s3_client``.
    bucket : str
        Target bucket name.
    key : str
        Object key to check.

    Returns
    -------
    bool
        ``True`` if the object exists, ``False`` if it returns a 404.

    Raises
    ------
    botocore.exceptions.ClientError
        For any error response other than a 404 (e.g. access denied).
    """
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "404":
            raise
        return False


def upload_file_to_s3(
    local_path : Path,
    bucket     : str,
    key        : str,
    overwrite  : bool = False,
) -> StageStatus:
    """
    Upload a local file to S3, skipping if it already exists.

    Parameters
    ----------
    local_path : Path
        Absolute path to the file to upload.
    bucket : str
        Target bucket name.
    key : str
        Destination S3 key.
    overwrite : bool, optional
        If ``False`` (default), skips the upload when ``key`` already
        exists in ``bucket``.

    Returns
    -------
    StageStatus
        ``SUCCESS`` on upload, ``SKIPPED`` if the object already existed
        and ``overwrite=False``, or ``FAILED`` on error.
    """
    try:
        client = build_s3_client()

        if not overwrite and s3_object_exists(client, bucket, key):
            logger.info("S3 object already exists, skipping upload: %s", key)
            return StageStatus.SKIPPED

        client.upload_file(Filename=str(local_path), Bucket=bucket, Key=key)
        logger.info("Uploaded to S3: s3://%s/%s", bucket, key)
        return StageStatus.SUCCESS

    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed for '%s': %s", local_path.name, exc)
        return StageStatus.FAILED


def upload_bytes_to_s3(
    content      : bytes,
    bucket       : str,
    key          : str,
    content_type : str,
    overwrite    : bool = False,
) -> StageStatus:
    """
    Upload an in-memory byte string to S3, skipping if it already exists.

    Parameters
    ----------
    content : bytes
        Payload to upload.
    bucket : str
        Target bucket name.
    key : str
        Destination S3 key.
    content_type : str
        MIME type to set on the uploaded object (e.g. ``"text/csv"``).
    overwrite : bool, optional
        If ``False`` (default), skips the upload when ``key`` already
        exists in ``bucket``.

    Returns
    -------
    StageStatus
        ``SUCCESS`` on upload, ``SKIPPED`` if the object already existed
        and ``overwrite=False``, or ``FAILED`` on error.
    """
    try:
        client = build_s3_client()

        if not overwrite and s3_object_exists(client, bucket, key):
            logger.info("S3 object already exists, skipping upload: %s", key)
            return StageStatus.SKIPPED

        client.upload_fileobj(
            Fileobj   = io.BytesIO(content),
            Bucket    = bucket,
            Key       = key,
            ExtraArgs = {"ContentType": content_type},
        )
        logger.info("Uploaded to S3: s3://%s/%s", bucket, key)
        return StageStatus.SUCCESS

    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed for '%s': %s", key, exc)
        return StageStatus.FAILED


def download_bytes_from_s3(bucket: str, key: str) -> bytes:
    """
    Download an object from S3 into memory.

    Parameters
    ----------
    bucket : str
        Source bucket name.
    key : str
        Object key to download.

    Returns
    -------
    bytes
        Raw object content.

    Raises
    ------
    FileNotFoundError
        If ``key`` does not exist in ``bucket``.
    botocore.exceptions.ClientError
        For any other S3 error (e.g. access denied).
    """
    client = build_s3_client()

    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            raise FileNotFoundError(f"S3 object not found: s3://{bucket}/{key}") from exc
        raise

    return response["Body"].read()