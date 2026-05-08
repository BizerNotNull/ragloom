#!/usr/bin/env python3
"""Render curated GitHub Release notes from CHANGELOG.md."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path


def fail(message: str) -> "NoReturn":
    print(message, file=sys.stderr)
    raise SystemExit(1)


def normalized_version() -> str:
    raw = os.environ.get("EXPECTED_VERSION", "").strip()
    if not raw:
        fail("EXPECTED_VERSION is required")
    version = raw.removeprefix("v")
    if not version:
        fail(f"invalid EXPECTED_VERSION '{raw}'")
    return version


def extract_section(changelog: str, version: str) -> str:
    changelog = changelog.replace("\r\n", "\n")
    escaped = re.escape(version)
    pattern = re.compile(
        rf"^## \[{escaped}\](?: - [^\n]+)?\n(?P<body>.*?)(?=^## \[|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    match = pattern.search(changelog)
    if match is None:
        fail(
            f"CHANGELOG.md is missing a curated section for version {version}; "
            "promote the release notes from [Unreleased] before publishing"
        )

    body = match.group("body").strip()
    if not body:
        fail(f"CHANGELOG.md section for version {version} is empty")
    return body


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    changelog = (repo_root / "CHANGELOG.md").read_text(encoding="utf-8")
    version = normalized_version()
    body = extract_section(changelog, version)
    sys.stdout.write(body)
    if not body.endswith("\n"):
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
