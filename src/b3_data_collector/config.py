# src/b3_data_collector/config.py

"""
Runtime settings for b3-data-collector.

Loads environment variables from the ``.env`` file at the repository root
and exposes them through an immutable ``Settings`` instance.

The ``.env`` location is resolved from the repository root rather than
searched for, so the same file is used no matter where the repository is
cloned or which directory the process is started from. Variables already
set in the process environment take precedence over the file, letting a
scheduler or CI override individual values without editing it.

A missing ``.env`` is not an error: tests and CI run without one and
inject explicit ``Settings`` instances instead.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

from b3_data_collector.paths import ROOT_DIR

ENV_FILE: Final[Path] = ROOT_DIR / ".env"

load_dotenv(ENV_FILE, override=False)


@dataclass(frozen=True)
class Settings:
    """
    Immutable AWS settings read from the environment.

    Defaults are evaluated once, when this module is imported, after the
    ``.env`` file has been loaded.

    Parameters
    ----------
    AWS_ACCESS_KEY_ID : str
        AWS access key. Empty when not configured.
    AWS_SECRET_ACCESS_KEY : str
        AWS secret key. Empty when not configured.
    AWS_S3_REGION : str
        AWS region of the bucket. Default is ``"us-east-1"``.
    AWS_S3_BUCKET_B3 : str
        Name of the bucket that stores B3 data. Empty when not configured.
    """

    AWS_ACCESS_KEY_ID     : str = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY : str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_S3_REGION         : str = os.getenv("AWS_S3_REGION", "us-east-1")
    AWS_S3_BUCKET_B3      : str = os.getenv("AWS_S3_BUCKET_B3", "")


settings = Settings()
