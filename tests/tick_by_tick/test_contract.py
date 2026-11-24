# tests/tick_by_tick/test_contract.py

"""
Unit tests for tick_by_tick/_contract.py.

The schema itself is checked against real output in test_partitioner.py;
this module covers the provenance helpers and the collector build lookup.
"""

from __future__ import annotations

import json
import re
import subprocess

import pyarrow as pa
import pytest

from b3_data_collector import __version__
from b3_data_collector.paths import ROOT_DIR
from b3_data_collector.tick_by_tick import _contract
from b3_data_collector.tick_by_tick._contract import (
    METADATA_KEY,
    OUTPUT_SCHEMA,
    SCHEMA_VERSION,
    ContractViolationError,
    SourceProvenance,
    TicksProvenance,
    with_provenance,
)


@pytest.fixture
def source() -> SourceProvenance:
    return SourceProvenance(
        feed                 = "DERIV",
        trade_date           = "2026-06-30",
        source_zip_name      = "30-06-2026_NEGOCIOSAVISTA_DRV.zip",
        source_zip_sha256    = "0" * 64,
        source_txt_name      = "30-06-2026_NEGOCIOSAVISTA_DRV.txt",
        source_txt_row_count = 3,
    )


class TestOutputSchema:
    def test_only_timestamp_and_brokers_are_nullable(self):
        nullable = [field.name for field in OUTPUT_SCHEMA if field.nullable]
        assert nullable == ["timestamp", "buyer_broker", "seller_broker"]

    def test_timestamp_carries_the_time_zone(self):
        assert OUTPUT_SCHEMA.field("timestamp").type == pa.timestamp("ms", tz="America/Sao_Paulo")


class TestProvenance:
    def test_round_trip_through_schema_metadata(self, source):
        table = with_provenance(pa.table({"column": [1]}), source)
        assert SourceProvenance.from_schema(table.schema) == source

    def test_existing_metadata_is_kept(self, source):
        table = pa.table({"column": [1]}).replace_schema_metadata({b"pandas": b"{}"})
        metadata = with_provenance(table, source).schema.metadata
        assert metadata[b"pandas"] == b"{}"
        assert METADATA_KEY in metadata

    def test_missing_metadata_fails(self):
        with pytest.raises(ContractViolationError, match="predates the tick contract"):
            SourceProvenance.from_schema(pa.schema([("column", pa.int64())]))

    @pytest.mark.parametrize("payload", [b"not json", json.dumps({"feed": "RV"}).encode()])
    def test_malformed_metadata_fails(self, payload):
        schema = pa.schema([("column", pa.int64())], metadata={METADATA_KEY: payload})
        with pytest.raises(ContractViolationError, match="malformed"):
            SourceProvenance.from_schema(schema)

    def test_build_completes_source(self, source, monkeypatch):
        monkeypatch.setattr(_contract, "collector_commit", lambda: "abc123")

        provenance = TicksProvenance.build(source, row_count=3, timestamp_null_count=1)

        assert provenance.source_txt_name == source.source_txt_name
        assert provenance.schema_version == SCHEMA_VERSION
        assert provenance.row_count == 3
        assert provenance.timestamp_null_count == 1
        assert provenance.timezone == "America/Sao_Paulo"
        assert provenance.collector_version == __version__
        assert provenance.collector_commit == "abc123"


class TestCollectorCommit:
    @pytest.fixture(autouse=True)
    def clear_cache(self):
        _contract.collector_commit.cache_clear()
        yield
        _contract.collector_commit.cache_clear()

    @staticmethod
    def _fake_git(head_code: int, diff_code: int):
        def run(*arguments: str) -> subprocess.CompletedProcess[str]:
            code = head_code if arguments[0] == "rev-parse" else diff_code
            return subprocess.CompletedProcess(arguments, code, stdout="abc123\n")
        return run

    def test_clean_checkout(self, monkeypatch):
        monkeypatch.setattr(_contract, "_run_git", self._fake_git(0, 0))
        assert _contract.collector_commit() == "abc123"

    def test_local_changes_are_flagged(self, monkeypatch):
        monkeypatch.setattr(_contract, "_run_git", self._fake_git(0, 1))
        assert _contract.collector_commit() == "abc123+dirty"

    def test_git_failure_gives_none(self, monkeypatch):
        monkeypatch.setattr(_contract, "_run_git", self._fake_git(128, 0))
        assert _contract.collector_commit() is None

    def test_git_missing_gives_none(self, monkeypatch):
        def missing(*arguments: str) -> subprocess.CompletedProcess[str]:
            raise FileNotFoundError("git")
        monkeypatch.setattr(_contract, "_run_git", missing)
        assert _contract.collector_commit() is None

    def test_outside_a_checkout_gives_none(self, monkeypatch, tmp_path):
        monkeypatch.setattr(_contract, "ROOT_DIR", tmp_path)
        assert _contract.collector_commit() is None


class TestContractDocument:
    def test_documents_every_column_in_order(self):
        document = ROOT_DIR / "docs" / "tick_by_tick_parquet_contract.md"
        section = document.read_text(encoding="utf-8").split("## Columns", 1)[1]
        section = section.split("\n## ", 1)[0]
        documented = re.findall(r"^\| `(\w+)` \|", section, flags=re.MULTILINE)
        assert documented == OUTPUT_SCHEMA.names
