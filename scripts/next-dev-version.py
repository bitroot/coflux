#!/usr/bin/env python3
"""Compute the next development version based on the current version and branch.

Usage: next-dev-version.py <branch>

Reads VERSION from the repo root and computes the next dev version:
  - main branch: minor bump (0.9.0 -> 0.10.0-dev)
  - release/* branch: patch bump (0.9.1 -> 0.9.2-dev)

Outputs (one per line):
  next_version=X.Y.Z-dev
  next_pep440=X.Y.Z.dev0
"""

import re
import sys
from pathlib import Path

VERSION_FILE = Path(__file__).resolve().parents[1] / "VERSION"


def compute_next_version(version: str, branch: str) -> str:
    parts = version.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected X.Y.Z version, got: {version}")
    major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

    if branch == "main":
        return f"{major}.{minor + 1}.0-dev"
    elif branch.startswith("release/"):
        return f"{major}.{minor}.{patch + 1}-dev"
    else:
        raise ValueError(f"Unsupported branch: {branch} (expected 'main' or 'release/*')")


def to_pep440(version: str) -> str:
    """Convert to PEP 440: 0.10.0-dev -> 0.10.0.dev0"""
    return re.sub(r"-dev$", ".dev0", version)


def main():
    if len(sys.argv) < 2:
        print("Usage: next-dev-version.py <branch>", file=sys.stderr)
        sys.exit(2)

    branch = sys.argv[1]

    raw = VERSION_FILE.read_text().strip()
    if not raw.endswith("-dev"):
        print(f"Error: VERSION ({raw}) does not end with -dev", file=sys.stderr)
        sys.exit(1)

    version = raw.removesuffix("-dev")
    next_version = compute_next_version(version, branch)
    next_pep440 = to_pep440(next_version)

    print(f"next_version={next_version}")
    print(f"next_pep440={next_pep440}")


if __name__ == "__main__":
    main()
