#!/usr/bin/env python3
"""Lint CHANGELOG.md files for consistent formatting.

Expected format:
    ## X.Y.Z

    Category:

    - Item description.
    - Another item.

    ## X.Y.Z-1
    ...

A version section may also contain just "No changes." (no categories or items).

Rules:
    - Version headings must be `## X.Y.Z` (valid semver).
    - Category lines must be one of: Enhancements, Fixes, Changes.
    - Items must start with `- `.
    - No empty sections (category with no items).
    - Versions must appear in descending order.
    - "No changes." sections must not contain categories or items.
"""

import re
import sys
from pathlib import Path

VALID_CATEGORIES = {"Enhancements:", "Fixes:", "Changes:"}
VERSION_RE = re.compile(r"^## (\d+\.\d+\.\d+)$")


def lint_changelog(path: Path) -> list[str]:
    errors = []
    text = path.read_text()
    lines = text.splitlines()

    if not lines:
        errors.append(f"{path}: file is empty")
        return errors

    if text and not text.endswith("\n"):
        errors.append(f"{path}: file must end with a newline")

    versions = []
    current_category = None
    category_has_items = True  # no active category yet
    in_version = False
    no_changes = False  # current section is "No changes."

    for i, line in enumerate(lines, 1):
        # Version heading
        m = VERSION_RE.match(line)
        if m:
            if current_category and not category_has_items:
                errors.append(f"{path}:{i}: empty category '{current_category}'")
            versions.append(m.group(1))
            current_category = None
            category_has_items = True
            no_changes = False
            in_version = True
            continue

        # Reject unexpected headings
        if line.startswith("## "):
            errors.append(f"{path}:{i}: invalid version heading: '{line}'")
            continue
        if line.startswith("# "):
            errors.append(f"{path}:{i}: unexpected top-level heading: '{line}'")
            continue

        # Blank line
        if not line.strip():
            continue

        if not in_version:
            errors.append(f"{path}:{i}: content before first version heading")
            continue

        # "No changes." marker
        if line == "No changes.":
            if no_changes or current_category:
                errors.append(f"{path}:{i}: 'No changes.' must be the only content in a version section")
            no_changes = True
            continue

        if no_changes:
            errors.append(f"{path}:{i}: unexpected content after 'No changes.'")
            continue

        # Category line
        if line.endswith(":") and not line.startswith("- "):
            if current_category and not category_has_items:
                errors.append(f"{path}:{i}: empty category '{current_category}'")
            if line not in VALID_CATEGORIES:
                errors.append(
                    f"{path}:{i}: unknown category '{line}' "
                    f"(expected one of: {', '.join(sorted(VALID_CATEGORIES))})"
                )
            current_category = line
            category_has_items = False
            continue

        # Item line
        if line.startswith("- "):
            if not current_category:
                errors.append(f"{path}:{i}: item outside of a category")
            category_has_items = True
            continue

        # Free-form text (e.g. intro paragraphs like "First release of...")

    # Check final category
    if current_category and not category_has_items:
        errors.append(f"{path}: empty category '{current_category}' at end of file")

    if not versions:
        errors.append(f"{path}: no version headings found")

    # Check versions are in descending order
    for i in range(len(versions) - 1):
        current = tuple(int(x) for x in versions[i].split("."))
        previous = tuple(int(x) for x in versions[i + 1].split("."))
        if current <= previous:
            errors.append(
                f"{path}: version {versions[i]} should come after {versions[i + 1]}, "
                f"not before (versions must be in descending order)"
            )

    return errors


def main():
    paths = [Path(p) for p in sys.argv[1:]]
    if not paths:
        print("Usage: lint-changelog.py CHANGELOG.md [...]", file=sys.stderr)
        sys.exit(2)

    all_errors = []
    for path in paths:
        if not path.exists():
            all_errors.append(f"{path}: file not found")
            continue
        all_errors.extend(lint_changelog(path))

    if all_errors:
        for error in all_errors:
            print(error, file=sys.stderr)
        sys.exit(1)

    print(f"OK: {len(paths)} changelog(s) checked")


if __name__ == "__main__":
    main()
