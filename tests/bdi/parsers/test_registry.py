# tests/bdi/parsers/test_registry.py

"""
Unit tests for bdi/parsers/_registry.py — decorator registration,
dispatch, and error paths. Uses a dummy parser, not a real report.
"""

from __future__ import annotations

import pandas as pd
import pytest

from b3_data_collector.bdi.parsers import _registry


@pytest.fixture(autouse=True)
def clean_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give each test an isolated, empty registry."""
    monkeypatch.setattr(_registry, "_READERS", {})


class TestRegisterParser:
    def test_registers_function_and_dispatch_finds_it(self):
        @_registry.register_parser("DummyReport")
        def read_dummy(source: bytes) -> pd.DataFrame:
            return pd.DataFrame({"a": [1]})

        result = _registry.read_bdi_report_file(b"irrelevant", report_name="DummyReport")

        assert result["a"].iloc[0] == 1

    def test_duplicate_registration_raises_value_error(self):
        @_registry.register_parser("DummyReport")
        def read_dummy(source: bytes) -> pd.DataFrame:
            return pd.DataFrame()

        with pytest.raises(ValueError, match="already registered"):
            @_registry.register_parser("DummyReport")
            def read_dummy_again(source: bytes) -> pd.DataFrame:
                return pd.DataFrame()


class TestReadBdiReportFile:
    def test_unregistered_report_raises_not_implemented_error(self):
        with pytest.raises(NotImplementedError, match="No parser registered"):
            _registry.read_bdi_report_file(b"irrelevant", report_name="Nonexistent")
