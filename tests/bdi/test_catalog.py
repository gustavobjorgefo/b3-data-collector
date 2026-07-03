# tests/bdi/test_catalog.py

"""Unit tests for bdi/_catalog.py — report catalog integrity."""

from __future__ import annotations

from b3_data_collector.bdi._catalog import CATALOG, CATALOG_BY_NAME, ENABLED_REPORTS


class TestCatalog:
    def test_catalog_is_not_empty(self):
        assert len(CATALOG) > 0

    def test_all_api_names_are_unique(self):
        api_names = [report.api_name for report in CATALOG]
        assert len(api_names) == len(set(api_names))

    def test_catalog_by_name_lookup_matches_catalog(self):
        for report in CATALOG:
            assert CATALOG_BY_NAME[report.api_name] is report

    def test_enabled_reports_is_subset_of_catalog(self):
        assert set(ENABLED_REPORTS).issubset(set(CATALOG))

    def test_enabled_reports_are_all_flagged_enabled(self):
        for report in ENABLED_REPORTS:
            assert report.enabled is True

    def test_every_report_has_a_section(self):
        for report in CATALOG:
            assert report.section  # non-empty string