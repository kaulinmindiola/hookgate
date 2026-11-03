"""The domain purity check must fail on I/O imports and pass on clean code (NFR-7)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check_domain_purity.py"


def run_check(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root)],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize(
    "source",
    [
        "import hashlib\nimport hmac\nfrom datetime import datetime\n",
        "from dataclasses import dataclass\n\n\n@dataclass\nclass A:\n    x: int\n",
        "from . import sibling\n",
    ],
)
def test_clean_domain_passes(tmp_path: Path, source: str) -> None:
    (tmp_path / "ok.py").write_text(source, encoding="utf-8")

    result = run_check(tmp_path)

    assert result.returncode == 0, result.stdout
    assert "domain purity OK" in result.stdout


@pytest.mark.parametrize(
    ("source", "needle"),
    [
        ("import sqlalchemy\n", "sqlalchemy"),
        ("from sqlalchemy.orm import Session\n", "sqlalchemy.orm"),
        ("import redis\n", "redis"),
        ("import os\n", "os"),
        ("from hookgate.infrastructure import db\n", "hookgate.infrastructure"),
        ("from hookgate import infrastructure\n", "hookgate.infrastructure"),
        ("from .. import infrastructure\n", "relative import"),
        ("data = open('x')\n", "open()"),
    ],
)
def test_forbidden_code_fails(tmp_path: Path, source: str, needle: str) -> None:
    (tmp_path / "bad.py").write_text(source, encoding="utf-8")

    result = run_check(tmp_path)

    assert result.returncode == 1
    assert needle in result.stdout
    assert "bad.py:1" in result.stdout


def test_syntax_error_is_reported_as_violation(tmp_path: Path) -> None:
    (tmp_path / "broken.py").write_text("def (:\n", encoding="utf-8")

    result = run_check(tmp_path)

    assert result.returncode == 1
    assert "syntax error" in result.stdout


def test_missing_root_returns_exit_code_2(tmp_path: Path) -> None:
    result = run_check(tmp_path / "does-not-exist")

    assert result.returncode == 2
    assert "not a directory" in result.stderr


def test_real_domain_package_is_pure() -> None:
    result = run_check(REPO_ROOT / "src" / "hookgate" / "domain")

    assert result.returncode == 0, result.stdout
