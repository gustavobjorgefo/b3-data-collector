# tests/conftest.py

"""
Shared pytest fixtures for the b3-data-collector test suite.

Fixtures defined here are automatically available to every test module
under tests/, without needing an explicit import — this is a pytest
convention tied to the file name "conftest.py".
"""

from __future__ import annotations

from datetime import date

import pytest


# --- Common test dates ---

@pytest.fixture
def sample_trade_date() -> date:
    """A single, fixed trading date (a Friday) used across tests."""
    return date(2026, 6, 26)


@pytest.fixture
def sample_date_range() -> tuple[date, date]:
    """A short date range spanning one weekend, for business-day tests."""
    return date(2026, 6, 26), date(2026, 6, 29)  # Fri -> Mon


# --- Sample CSV/report content ---

@pytest.fixture
def sample_csv_bytes() -> bytes:
    """
    Minimal valid CSV content, mimicking a BDI report export.

    Not a real B3 report — just enough bytes to exercise the "successful
    fetch" path without hitting the network.
    """
    return (
        b"Column1;Column2;Column3\n"
        b"ValueA;123;2026-06-26\n"
        b"ValueB;456;2026-06-26\n"
    )


@pytest.fixture
def empty_csv_bytes() -> bytes:
    """Content too small to be a real report - exercises the 'unavailable' path."""
    return b""


# --- Fake settings (never real credentials) ---

@pytest.fixture
def fake_aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Set fake AWS environment variables for the duration of a test.

    Ensures tests never depend on (or accidentally use) real credentials
    from the developer's actual .env file.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_S3_REGION", "us-east-1")
    monkeypatch.setenv("AWS_S3_BUCKET_B3", "test-bucket")


# --- Temporary filesystem paths ---

@pytest.fixture
def tmp_data_dir(tmp_path):
    """
    A temporary directory standing in for the project's data/ folder.

    Using pytest's built-in tmp_path fixture ensures every test gets an
    isolated, automatically cleaned-up directory - never touches the
    real local data/ folder.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir