# tests/tick_by_tick/test_feed.py

"""Unit tests for tick_by_tick/_feed.py — FeedType configuration."""

from __future__ import annotations

from b3_data_collector.tick_by_tick._feed import FeedType


class TestFeedType:
    def test_rv_and_deriv_have_distinct_configs(self):
        assert FeedType.RV.config != FeedType.DERIV.config

    def test_rv_config_has_expected_label(self):
        assert "RV" in FeedType.RV.config.label or "equities" in FeedType.RV.config.label.lower()

    def test_each_feed_config_has_all_required_paths_keys(self):
        for feed in FeedType:
            cfg = feed.config
            assert cfg.paths_key_downloads
            assert cfg.paths_key_raw
            assert cfg.paths_key_ticks

    def test_url_template_contains_date_placeholder(self):
        for feed in FeedType:
            assert "{date}" in feed.config.url_template

    def test_each_feed_config_has_distinct_s3_prefixes(self):
        for feed in FeedType:
            cfg = feed.config
            assert cfg.s3_prefix
            assert cfg.s3_prefix_ticks
            # ZIP archive and processed ticks must never share a prefix —
            # otherwise uploads from the two stages could collide in S3.
            assert cfg.s3_prefix != cfg.s3_prefix_ticks