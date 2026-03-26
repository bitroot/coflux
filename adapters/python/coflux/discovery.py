"""Discovery of @task and @workflow decorated functions."""

from __future__ import annotations

import importlib
import json
import sys
from typing import Any

from .target import Target, _to_ms, serialize_cache, serialize_defer, serialize_retries


def discover_targets(modules: list[str]) -> list[dict[str, Any]]:
    """Discover all targets in the specified modules.

    Args:
        modules: List of module names to scan (e.g., ["myapp.workflows", "myapp.tasks"])

    Returns:
        List of target definitions suitable for JSON serialization.
    """
    targets = []

    for module_name in modules:
        try:
            module = importlib.import_module(module_name)
        except ImportError as e:
            print(f"Warning: Failed to import module {module_name}: {e}", file=sys.stderr)
            continue

        # Scan module attributes for Target instances (excluding stubs)
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, Target) and not attr.definition.is_stub:
                target_def = _build_target_definition(attr, module_name)
                targets.append(target_def)

    return targets


def _build_target_definition(target: Any, module_name: str) -> dict[str, Any]:
    """Build a target definition dict from a Target instance."""
    definition = target.definition

    result: dict[str, Any] = {
        "module": module_name,
        "name": target.name,
        "type": definition.type,
        "parameters": [
            {
                "name": p.name,
                "annotation": p.annotation,
                "default": p.default,  # Already JSON-encoded
            }
            for p in definition.parameters
        ],
    }

    # Add optional fields only if set
    if definition.wait_for:
        result["wait_for"] = sorted(definition.wait_for)

    if definition.cache:
        result["cache"] = serialize_cache(definition.cache, definition.parameters)

    if definition.retries:
        result["retries"] = serialize_retries(definition.retries)

    if definition.defer:
        result["defer"] = serialize_defer(definition.defer, definition.parameters)

    if definition.delay:
        result["delay"] = _to_ms(definition.delay)

    if definition.memo:
        result["memo"] = definition.memo

    if definition.requires:
        result["requires"] = definition.requires

    if definition.recurrent:
        result["recurrent"] = True

    if definition.instruction:
        result["instruction"] = definition.instruction

    return result


def run_discovery(modules: list[str]) -> int:
    """Run discovery and output manifest to stdout.

    Args:
        modules: List of module names to scan.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    try:
        targets = discover_targets(modules)
        manifest = {"targets": targets}
        # Output as compact JSON
        print(json.dumps(manifest, separators=(",", ":")))
        return 0
    except Exception as e:
        print(f"Error during discovery: {e}", file=sys.stderr)
        return 1
