# src\b3_data_collector\tick_by_tick\_downloader.py

"""
B3 raw tick-data downloader — Stage 1.

Responsible for:
    1. Fetching the daily ZIP from the B3 distribution endpoint for the
       requested feed type and persisting it to the feed-specific local
       downloads directory.
    2. Uploading the ZIP to S3 as an immutable historical archive
       (only when ``upload_to_s3=True``).

Does not parse, transform, or validate the ZIP contents — that
responsibility belongs to the extraction stage (_extractor.py).

Feed-specific configuration (URL, filename template, S3 prefix, local
directory key) is resolved entirely through ``FeedType.config`` —
this module contains no hardcoded per-feed constants.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Final

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

from ..config import settings
from ..paths import PATHS_B3
from ._feed import FeedType
from ._models import StageStatus

logger = logging.getLogger(__name__)

# --- Module constants ---

_VALID_CONTENT_TYPES : Final[tuple[str, ...]] = ("zip", "octet-stream")
_CHUNK_SIZE          : Final[int]             = 8_192


# --- Internal helpers ---

def _build_s3_client() -> "boto3.client":
    return boto3.client(
        "s3",
        region_name           = settings.AWS_S3_REGION,
        aws_access_key_id     = settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY,
    )


def _upload_to_s3(local_path: Path, s3_prefix: str) -> StageStatus:
    """
    Upload a local ZIP file to the configured S3 bucket.

    Parameters
    ----------
    local_path : Path
        Absolute path to the file to upload.
    s3_prefix : str
        S3 key prefix for the target feed (e.g. ``"b3/bdi/tick_by_tick_rv/"``).

    Returns
    -------
    StageStatus
        ``SUCCESS`` on successful upload, ``SKIPPED`` if the object already
        exists, or ``FAILED`` on error.
    """
    s3_key = s3_prefix + local_path.name

    try:
        client = _build_s3_client()

        # Avoid redundant uploads — check existence before transferring.
        try:
            client.head_object(Bucket=settings.AWS_S3_BUCKET_B3, Key=s3_key)
            logger.info("S3 object already exists, skipping upload: %s", s3_key)
            return StageStatus.SKIPPED
        except ClientError as exc:
            # 404 means the object does not exist yet — proceed with upload.
            if exc.response["Error"]["Code"] != "404":
                raise

        client.upload_file(
            Filename = str(local_path),
            Bucket   = settings.AWS_S3_BUCKET_B3,
            Key      = s3_key,
        )
        logger.info("Uploaded to S3: s3://%s/%s", settings.AWS_S3_BUCKET_B3, s3_key)
        return StageStatus.SUCCESS

    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed for %s: %s", local_path.name, exc)
        return StageStatus.FAILED


# --- Public API ---

def download_zip(
    trade_date   : date,
    feed         : FeedType,
    upload_to_s3 : bool = True,
    timeout      : int  = 30,
) -> tuple[Path | None, StageStatus, StageStatus]:
    """
    Download the daily B3 ZIP for a given trading date and feed type.

    If the file already exists locally it is not re-downloaded. The S3
    upload is attempted only when ``upload_to_s3=True`` and the file is
    present (whether freshly downloaded or already cached).

    Parameters
    ----------
    trade_date : date
        Trading day to download.
    feed : FeedType
        Feed selector (``FeedType.RV`` or ``FeedType.DERIV``).
    upload_to_s3 : bool, optional
        If ``True``, upload the ZIP to S3 after a successful download.
        Default is ``True``.
    timeout : int, optional
        HTTP request timeout in seconds. Default is ``30``.

    Returns
    -------
    tuple[Path | None, StageStatus, StageStatus]
        ``(local_path, download_status, s3_status)``

        - ``local_path`` is ``None`` when the file is unavailable on B3.
        - ``s3_status`` is ``StageStatus.SKIPPED`` when ``upload_to_s3``
          is ``False`` or when the download itself did not succeed.
    """
    cfg       = feed.config
    url       = cfg.url_template.format(date=f"{trade_date:%Y-%m-%d}")
    filename  = cfg.zip_name_template.format(date=trade_date)
    save_path = PATHS_B3[cfg.paths_key_downloads] / filename

    s3_status = StageStatus.SKIPPED

    # --- Already on disk ---
    if save_path.exists():
        logger.info("[%s] Already downloaded: %s", cfg.label, save_path.name)
        if upload_to_s3:
            s3_status = _upload_to_s3(save_path, cfg.s3_prefix)
        return save_path, StageStatus.SKIPPED, s3_status

    # --- HTTP download ---
    logger.info("[%s] Downloading B3 trades for %s", cfg.label, trade_date)

    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            if response.status_code == 404:
                logger.warning(
                    "[%s] No file available for %s (holiday or weekend).",
                    cfg.label, trade_date,
                )
                return None, StageStatus.UNAVAILABLE, StageStatus.SKIPPED

            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()
            if not any(valid in content_type for valid in _VALID_CONTENT_TYPES):
                raise ValueError(
                    f"[{cfg.label}] Unexpected Content-Type '{content_type}' "
                    f"for {trade_date}. Expected ZIP or octet-stream."
                )

            save_path.parent.mkdir(parents=True, exist_ok=True)

            with save_path.open("wb") as file_handle:
                for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
                    if chunk:
                        file_handle.write(chunk)

    except requests.RequestException as exc:
        logger.error(
            "[%s] Network error downloading ZIP for %s: %s", cfg.label, trade_date, exc
        )
        return None, StageStatus.FAILED, StageStatus.SKIPPED

    except ValueError as exc:
        logger.error("%s", exc)
        return None, StageStatus.FAILED, StageStatus.SKIPPED

    logger.info("[%s] Download complete: %s", cfg.label, save_path.name)

    # --- S3 upload ---
    if upload_to_s3:
        s3_status = _upload_to_s3(save_path, cfg.s3_prefix)

    return save_path, StageStatus.SUCCESS, s3_status