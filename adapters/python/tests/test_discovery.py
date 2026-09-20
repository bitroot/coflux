"""What discovery scans when it's given a package, or nothing at all.

A package name covers everything under it. No names means the working
directory: its top-level modules and packages, less the private ones and
the conventional non-targets that tend to blow up on import.
"""

from __future__ import annotations

import importlib
import sys
import textwrap

import pytest

from coflux.discovery import discover_targets

_TASK = textwrap.dedent(
    """
    import coflux as cf

    @cf.task()
    def {name}():
        return 1
    """
)


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A throwaway project as the working directory, cleaned out of
    ``sys.modules`` afterwards so its names don't leak between tests."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    before = set(sys.modules)

    def write(path, source):
        file = tmp_path / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(source)

    yield write

    for name in set(sys.modules) - before:
        sys.modules.pop(name, None)


def _names(targets):
    return sorted((t["module"], t["name"]) for t in targets)


def test_a_package_covers_its_submodules(project):
    project("dpkg/__init__.py", "")
    project("dpkg/flows.py", _TASK.format(name="flow"))
    project("dpkg/deep/__init__.py", "")
    project("dpkg/deep/jobs.py", _TASK.format(name="job"))
    project("dpkg/_private.py", _TASK.format(name="hidden"))

    targets, errors = discover_targets(["dpkg"])

    assert errors == []
    assert _names(targets) == [("dpkg.deep.jobs", "job"), ("dpkg.flows", "flow")]


def test_no_modules_scans_the_working_directory(project):
    project("dwd_app/__init__.py", "")
    project("dwd_app/flows.py", _TASK.format(name="flow"))
    project("dwd_scratch.py", _TASK.format(name="scratch"))
    project("_dwd_hidden.py", _TASK.format(name="hidden"))
    # Skipped by name, so importing them is never attempted
    project("conftest.py", "raise RuntimeError('conftest imported')")
    project("setup.py", "raise RuntimeError('setup imported')")
    project("tests/__init__.py", "raise RuntimeError('tests imported')")
    # Not a package without __init__.py, and not a module at all
    project("dwd_data/flows.py", _TASK.format(name="orphan"))
    project("notes.txt", "")

    targets, errors = discover_targets([])

    assert errors == []
    assert _names(targets) == [("dwd_app.flows", "flow"), ("dwd_scratch", "scratch")]


def test_an_empty_working_directory_finds_nothing(project):
    project("notes.txt", "")

    assert discover_targets([]) == ([], [])


def test_a_broken_module_in_the_working_directory_is_reported(project):
    project("dwd_ok.py", _TASK.format(name="ok"))
    project("dwd_broken.py", "import does_not_exist")

    targets, errors = discover_targets([])

    assert _names(targets) == [("dwd_ok", "ok")]
    assert [e.module for e in errors] == ["dwd_broken"]
