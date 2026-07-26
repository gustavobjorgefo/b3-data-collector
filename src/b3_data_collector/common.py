# b3_data_collector/common.py

"""
Shared status vocabulary and S3 access helpers used by both the BDI and
tick-by-tick collectors.

Kept as a single module so both subpackages agree on the same set of
pipeline-stage outcomes and reuse the same S3 client and upload logic,
rather than each defining its own incompatible copy.
"""

from __future__ import annotations

import io
import logging
from enum import Enum, auto
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .config import settings

logger = logging.getLogger(__name__)


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