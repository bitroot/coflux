#!/usr/bin/env python3
"""Extract release notes for a given version from component changelogs.

Usage: extract-release-notes.py <version> [output-file]

Reads the specified version section from each component changelog and
combines them into a single markdown document suitable for a GitHub Release.
"""

import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]

CHANGELOGS = [
    ("Server", _REPO_ROOT / "server/CHANGELOG.md"),
    ("CLI", _REPO_ROOT / "cli/CHANGELOG.md"),
    ("Python Adapter", _REPO_ROOT / "adapters/python/CHANGELOG.md"),
]


def extract_section(path: Path, version: str) -> str | None:
    """Extract the content under `## {version}` up to the next `## ` or EOF."""
    text = path.read_text()
    pattern = rf"^## {re.escape(version)}\n(.*?)(?=^## |\Z)"
    match = re.search(pattern, text, re.DOTALL | re.MULTILINE)
    if match:
        return match.group(1).strip()
    return None


def main():
    if len(sys.argv) < 2:
        print("Usage: extract-release-notes.py <version> [output-file]", file=sys.stderr)
        sys.exit(2)

    version = sys.argv[1]
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None

    sections = []
    errors = []
    for label, path in CHANGELOGS:
        if not path.exists():
            errors.append(f"{label} ({path}): file not found")
            continue
        content = extract_section(path, version)
        if content:
            sections.append(f"## {label}\n\n{content}")
        else:
            errors.append(f"{label} ({path}): no entry for {version}")

    if errors:
        for msg in errors:
            print(f"Error: {msg}", file=sys.stderr)
        sys.exit(1)

    body = "\n\n".join(sections) + "\n"

    if output_path:
        output_path.write_text(body)
        print(f"Release notes written to {output_path}")
    else:
        print(body)


if __name__ == "__main__":
    main()
