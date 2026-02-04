"""Discovery of @task and @workflow decorated functions."""

from __future__ import annotations

import importlib
import json
import sys
from typing import Any

from .decorators import Target


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

        # Scan module attributes for Target instances
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, Target):
                target_def = _build_target_definition(attr, module_name)
                targets.append(target_def)

    return targets


def _build_target_definition(target: Any, module_name: str) -> dict[str, Any]:
    """Build a target definition dict from a Target instance."""
    definition = target.definition

    result: dict[str, Any] = {
        "name": f"{module_name}.{target.name}",
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
        cache_def: dict[str, Any] = {}
        if definition.cache.params is not True:
            cache_def["params"] = definition.cache.params
        else:
            cache_def["params"] = True
        if definition.cache.max_age is not None:
            cache_def["max_age_ms"] = definition.cache.max_age
        if definition.cache.namespace:
            cache_def["namespace"] = definition.cache.namespace
        if definition.cache.version:
            cache_def["version"] = definition.cache.version
        result["cache"] = cache_def

    if definition.retries:
        retries_def: dict[str, Any] = {}
        if definition.retries.limit is not None:
            retries_def["limit"] = definition.retries.limit
        if definition.retries.delay_min:
            retries_def["delay_min_ms"] = int(definition.retries.delay_min)
        if definition.retries.delay_max:
            retries_def["delay_max_ms"] = int(definition.retries.delay_max)
        result["retries"] = retries_def

    if definition.defer:
        defer_def: dict[str, Any] = {}
        if definition.defer.params is not True:
            defer_def["params"] = definition.defer.params
        else:
            defer_def["params"] = True
        result["defer"] = defer_def

    if definition.delay:
        result["delay"] = int(definition.delay * 1000)  # Convert seconds to milliseconds

    if definition.memo:
        result["memo"] = definition.memo

    if definition.requires:
        result["requires"] = definition.requires

    if definition.recurrent:
        result["recurrent"] = True

    if definition.is_stub:
        result["is_stub"] = True

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
