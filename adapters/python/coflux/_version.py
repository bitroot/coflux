"""Package version, isolated to avoid circular imports."""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("coflux")
except PackageNotFoundError:
    __version__ = "dev"
