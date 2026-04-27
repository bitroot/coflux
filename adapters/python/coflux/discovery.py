"""Discovery of @task and @workflow decorated functions."""

from __future__ import annotations

import importlib
import json
import pkgutil
import sys
from typing import Any

from .target import (
    Target,
    _to_ms,
    serialize_cache,
    serialize_defer,
    serialize_retries,
    serialize_streams,
)


def _expand_modules(module_names: list[str]) -> list[str]:
    """Expand package names into their constituent submodules.

    If a name refers to a Python package (has __path__), it is recursively
    walked using pkgutil.walk_packages. Plain module names are passed through
    unchanged. Submodules whose final component starts with '_' are skipped.
    """
    result: list[str] = []
    seen: set[str] = set()

    for name in module_names:
        try:
            mod = importlib.import_module(name)
        except Exception:
            # Let discover_targets handle the error with its own warning
            if name not in seen:
                seen.add(name)
                result.append(name)
            continue

        if name not in seen:
            seen.add(name)
            result.append(name)

        if hasattr(mod, "__path__"):
            for _importer, subname, _ispkg in pkgutil.walk_packages(
                mod.__path__, mod.__name__ + "."
            ):
                # Skip private submodules (e.g., _internal, _helpers)
                leaf = subname.rsplit(".", 1)[-1]
                if leaf.startswith("_"):
                    continue
                if subname not in seen:
                    seen.add(subname)
                    result.append(subname)

    return result


def discover_targets(modules: list[str]) -> list[dict[str, Any]]:
    """Discover all targets in the specified modules.

    If a module name refers to a package, all submodules are scanned
    recursively (private submodules starting with '_' are skipped).

    Args:
        modules: List of module or package names to scan
                 (e.g., ["myapp.workflows", "myapp.tasks"] or just ["myapp"])

    Returns:
        List of target definitions suitable for JSON serialization.
    """
    expanded = _expand_modules(modules)
    targets = []
    seen_targets: set[int] = set()

    for module_name in expanded:
        try:
            module = importlib.import_module(module_name)
        except Exception as e:
            print(
                f"Warning: Failed to import module {module_name}: {e}", file=sys.stderr
            )
            continue

        # Scan module attributes for Target instances (excluding stubs)
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, Target) and not attr.definition.is_stub:
                # Deduplicate targets re-exported across modules
                if id(attr) in seen_targets:
                    continue
                seen_targets.add(id(attr))
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

    if definition.timeout:
        result["timeout"] = _to_ms(definition.timeout)

    if definition.recurrent:
        result["recurrent"] = True

    if definition.instruction:
        result["instruction"] = definition.instruction

    if definition.streams is not None:
        streams_dict = serialize_streams(definition.streams)
        if streams_dict is not None:
            result["streams"] = streams_dict

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
