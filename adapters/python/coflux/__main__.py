"""Entry point for the Coflux Python SDK.

Usage:
    python -m coflux discover <modules...>
    python -m coflux execute
"""

import argparse
import sys

from .discovery import run_discovery
from .executor import run_executor


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="coflux",
        description="Coflux Python SDK",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # discover command
    discover_parser = subparsers.add_parser(
        "discover",
        help="Discover @task and @workflow targets in modules",
    )
    discover_parser.add_argument(
        "modules",
        nargs="+",
        help="Python modules to scan for targets",
    )

    # execute command
    subparsers.add_parser(
        "execute",
        help="Run as an executor process (communicates via stdio)",
    )

    args = parser.parse_args()

    if args.command == "discover":
        return run_discovery(args.modules)
    elif args.command == "execute":
        return run_executor()
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
