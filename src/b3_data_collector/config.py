# src/b3_data_collector/config.py

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    AWS_ACCESS_KEY_ID     : str = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY : str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_S3_REGION          : str = os.getenv("AWS_S3_REGION", "us-east-1")
    AWS_S3_BUCKET_B3        : str = os.getenv("AWS_S3_BUCKET_B3", "")

settings = Settings()