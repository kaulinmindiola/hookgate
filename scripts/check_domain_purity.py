"""Fail if the pure domain package imports I/O or framework code (NFR-7).

Usage (from the repository root):
    python scripts/check_domain_purity.py [--root src/hookgate/domain]

Exit codes: 0 = clean, 1 = violations found, 2 = invalid root directory.

The domain package is assumed to be flat, so any relative import that climbs
two or more levels (``from .. import x``) is treated as escaping the package.
"""

from __future__ import annotations

import argparse
import ast
import sys
from collections.abc import Sequence
from pathlib import Path

DEFAULT_ROOT = Path("src/hookgate/domain")

# Third-party frameworks and stdlib modules that perform I/O.
FORBIDDEN_MODULES: frozenset[str] = frozenset(
    {
        "alembic",
        "celery",
        "fastapi",
        "httpx",
        "os",
        "prometheus_client",
        "psycopg",
        "pydantic_settings",
        "redis",
        "requests",
        "socket",
        "sqlalchemy",
        "sqlite3",
        "starlette",
        "structlog",
        "subprocess",
        "uvicorn",
    }
)

# Other layers of this project that the domain must never depend on.
FORBIDDEN_INTERNAL: tuple[str, ...] = (
    "hookgate.api",
    "hookgate.application",
    "hookgate.infrastructure",
)

FORBIDDEN_CALLS: frozenset[str] = frozenset({"open"})


def _is_forbidden(qualified_name: str) -> bool:
    if qualified_name.split(".")[0] in FORBIDDEN_MODULES:
        return True
    return any(
        qualified_name == prefix or qualified_name.startswith(prefix + ".")
        for prefix in FORBIDDEN_INTERNAL
    )


def _check_file(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        return [f"{path}:{exc.lineno or 0}: syntax error: {exc.msg}"]

    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_forbidden(alias.name):
                    violations.append(f"{path}:{node.lineno}: forbidden import '{alias.name}'")
        elif isinstance(node, ast.ImportFrom):
            if node.level >= 2:
                violations.append(
                    f"{path}:{node.lineno}: relative import escapes the domain package"
                )
            elif node.level == 0 and node.module:
                candidates = [node.module, *(f"{node.module}.{a.name}" for a in node.names)]
                hit = next((c for c in candidates if _is_forbidden(c)), None)
                if hit is not None:
                    violations.append(f"{path}:{node.lineno}: forbidden import '{hit}'")
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in FORBIDDEN_CALLS
        ):
            violations.append(f"{path}:{node.lineno}: forbidden call '{node.func.id}()'")
    return violations


def find_violations(root: Path) -> list[str]:
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        violations.extend(_check_file(path))
    return violations


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check that the domain layer stays pure.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args(argv)
    root: Path = args.root

    if not root.is_dir():
        print(f"error: {root} is not a directory", file=sys.stderr)
        return 2

    violations = find_violations(root)
    for violation in violations:
        print(violation)
    if violations:
        print(f"\n{len(violations)} domain purity violation(s) found.")
        return 1

    print(f"domain purity OK ({root})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
