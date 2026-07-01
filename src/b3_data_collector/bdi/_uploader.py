# b3_data_collector/bdi/_uploader.py

"""
S3 uploader for BDI report CSVs.
...
"""

from __future__ import annotations

import io
import logging
from datetime import date
from typing import Final

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from ..config import settings
from ..common import StageStatus

logger = logging.getLogger(__name__)

# --- Module constants ---

_S3_KEY_PREFIX  : Final[str] = "b3/bdi/reports/"
_CONTENT_TYPE   : Final[str] = "text/csv"


# --- Internal helpers ---

def _build_s3_client() -> "boto3.client":
    return boto3.client(
        "s3",
        region_name           = settings.AWS_S3_REGION,
        aws_access_key_id     = settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY,
    )


def _build_s3_key(section: str, api_name: str, trade_date: date) -> str:
    return (
        f"{_S3_KEY_PREFIX}"
        f"{section}/"
        f"{api_name}/"
        f"year={trade_date:%Y}/"
        f"{trade_date}.csv"
    )


# --- Public API ---

def upload_csv(
    content    : bytes,
    section    : str,
    api_name   : str,
    trade_date : date,
    overwrite  : bool = False,
) -> StageStatus:
    s3_key = _build_s3_key(section, api_name, trade_date)

    try:
        client = _build_s3_client()

        if not overwrite:
            try:
                client.head_object(Bucket=settings.AWS_S3_BUCKET_B3, Key=s3_key)
                logger.info("S3 object already exists, skipping: %s", s3_key)
                return StageStatus.SKIPPED
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "404":
                    raise

        client.upload_fileobj(
            Fileobj     = io.BytesIO(content),
            Bucket      = settings.AWS_S3_BUCKET_B3,
            Key         = s3_key,
            ExtraArgs   = {"ContentType": _CONTENT_TYPE},
        )
        logger.info(
            "Uploaded: s3://%s/%s", settings.AWS_S3_BUCKET_B3, s3_key
        )
        return StageStatus.SUCCESS

    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed for '%s': %s", s3_key, exc)
        return StageStatus.FAILED