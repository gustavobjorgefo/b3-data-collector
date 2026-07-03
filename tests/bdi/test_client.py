# tests/bdi/test_client.py

"""Unit tests for bdi/_client.py — network mocked via requests-mock, no real B3 calls."""

from __future__ import annotations

import pytest
import requests

from b3_data_collector.bdi._client import _BDI_EXPORT_URL, fetch_report_csv


class TestFetchReportCsv:
    def test_successful_fetch_returns_bytes(
        self, requests_mock, sample_trade_date, sample_csv_bytes
    ):
        requests_mock.post(_BDI_EXPORT_URL, content=sample_csv_bytes)

        result = fetch_report_csv(api_name="DailyAverageStocks", trade_date=sample_trade_date)

        assert result == sample_csv_bytes

    def test_404_returns_none(self, requests_mock, sample_trade_date):
        requests_mock.post(_BDI_EXPORT_URL, status_code=404)

        result = fetch_report_csv(api_name="UnknownReport", trade_date=sample_trade_date)

        assert result is None

    def test_empty_content_returns_none(
        self, requests_mock, sample_trade_date, empty_csv_bytes
    ):
        requests_mock.post(_BDI_EXPORT_URL, content=empty_csv_bytes)

        result = fetch_report_csv(api_name="DailyAverageStocks", trade_date=sample_trade_date)

        assert result is None

    def test_server_error_raises_http_error(self, requests_mock, sample_trade_date):
        requests_mock.post(_BDI_EXPORT_URL, status_code=500)

        with pytest.raises(requests.HTTPError):
            fetch_report_csv(api_name="DailyAverageStocks", trade_date=sample_trade_date)

    def test_connection_error_propagates(self, requests_mock, sample_trade_date):
        requests_mock.post(_BDI_EXPORT_URL, exc=requests.exceptions.ConnectionError)

        with pytest.raises(requests.exceptions.RequestException):
            fetch_report_csv(api_name="DailyAverageStocks", trade_date=sample_trade_date)

    def test_sends_correct_payload(
        self, requests_mock, sample_trade_date, sample_csv_bytes
    ):
        """Confirms the POST body matches the B3 API contract exactly."""
        requests_mock.post(_BDI_EXPORT_URL, content=sample_csv_bytes)

        fetch_report_csv(api_name="DailyAverageStocks", trade_date=sample_trade_date)

        sent_json = requests_mock.last_request.json()
        assert sent_json["Name"] == "DailyAverageStocks"
        assert sent_json["Date"] == f"{sample_trade_date:%Y-%m-%d}"
        assert sent_json["FinalDate"] == f"{sample_trade_date:%Y-%m-%d}"