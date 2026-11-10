"""The API reports the package version, whose single source is ``pyproject.toml``."""

import tomllib
from pathlib import Path

from hookgate.api.main import APP_VERSION

PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


def test_the_api_reports_the_version_of_pyproject() -> None:
    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]
    assert project["version"] == APP_VERSION
