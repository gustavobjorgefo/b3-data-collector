# tests/test_config.py

"""
Unit tests for config.py — .env location and precedence rules.

``config`` reads the environment once, at import time, so each test
reloads the module against a temporary repository root. The fixture
restores the real root, the process environment and the module itself
afterwards, so no other test sees the temporary values.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType

import pytest

from b3_data_collector import config, paths

_BUCKET_VARIABLE = "AWS_S3_BUCKET_B3"


@pytest.fixture
def reload_config(monkeypatch) -> Iterator[Callable[[Path], ModuleType]]:
    """Return a function that reloads ``config`` with ``root`` as the repository root."""
    original_environment = dict(os.environ)
    original_root        = paths.ROOT_DIR

    def _reload(root: Path) -> ModuleType:
        monkeypatch.setattr(paths, "ROOT_DIR", root)
        return importlib.reload(config)

    yield _reload

    # load_dotenv writes straight into os.environ, outside monkeypatch's
    # bookkeeping, so the environment is restored by hand.
    os.environ.clear()
    os.environ.update(original_environment)
    paths.ROOT_DIR = original_root
    importlib.reload(config)


class TestEnvFileLocation:
    def test_env_file_is_at_repository_root(self):
        assert config.ENV_FILE == paths.ROOT_DIR / ".env"

    def test_repository_root_contains_pyproject(self):
        # Guards ROOT_DIR itself: if the package moves, parents[2] stops
        # pointing at the repository and the .env would silently be missed.
        assert (paths.ROOT_DIR / "pyproject.toml").is_file()


class TestEnvLoading:
    def test_reads_values_from_env_file(self, tmp_path, monkeypatch, reload_config):
        (tmp_path / ".env").write_text(f"{_BUCKET_VARIABLE}=from-file\n", encoding="utf-8")
        monkeypatch.delenv(_BUCKET_VARIABLE, raising=False)

        reloaded = reload_config(tmp_path)

        assert reloaded.settings.AWS_S3_BUCKET_B3 == "from-file"

    def test_process_environment_wins_over_env_file(
        self, tmp_path, monkeypatch, reload_config
    ):
        (tmp_path / ".env").write_text(f"{_BUCKET_VARIABLE}=from-file\n", encoding="utf-8")
        monkeypatch.setenv(_BUCKET_VARIABLE, "from-environment")

        reloaded = reload_config(tmp_path)

        assert reloaded.settings.AWS_S3_BUCKET_B3 == "from-environment"

    def test_missing_env_file_falls_back_to_defaults(
        self, tmp_path, monkeypatch, reload_config
    ):
        # The real .env, if any, was already loaded into os.environ when the
        # package was first imported, so its values must be cleared here.
        monkeypatch.delenv(_BUCKET_VARIABLE, raising=False)
        monkeypatch.delenv("AWS_S3_REGION", raising=False)

        reloaded = reload_config(tmp_path)

        assert reloaded.settings.AWS_S3_BUCKET_B3 == ""
        assert reloaded.settings.AWS_S3_REGION == "us-east-1"
