"""Output capture for stdout/stderr during task execution."""

from __future__ import annotations

import io
import sys
from contextlib import contextmanager
from typing import Iterator, TextIO

from . import protocol


class OutputCapture(io.TextIOBase):
    """Captures writes and sends them as log messages."""

    def __init__(
        self,
        execution_id: str,
        level: int = 2,  # default to info (2)
        original: TextIO | None = None,
    ):
        self.execution_id = execution_id
        self.level = level
        self.original = original
        self._buffer = ""

    def write(self, s: str) -> int:
        if not s:
            return 0
        # Buffer the output and flush on newlines
        self._buffer += s
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line:  # Don't send empty lines
                protocol.send_log(self.execution_id, self.level, line)
        return len(s)

    def flush(self) -> None:
        # Flush any remaining buffered content
        if self._buffer:
            protocol.send_log(self.execution_id, self.level, self._buffer)
            self._buffer = ""

    def fileno(self) -> int:
        # Return original's fileno if available, for compatibility
        if self.original and hasattr(self.original, "fileno"):
            return self.original.fileno()
        raise io.UnsupportedOperation("fileno")

    def isatty(self) -> bool:
        return False

    @property
    def encoding(self) -> str:
        return "utf-8"


@contextmanager
def capture_output(execution_id: str) -> Iterator[None]:
    """Context manager that captures stdout/stderr and sends as log messages."""
    # Save original streams
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    # Create capture streams
    # Level 1 = stdout, Level 3 = stderr (matching server convention)
    stdout_capture = OutputCapture(execution_id, 1, original_stdout)
    stderr_capture = OutputCapture(execution_id, 3, original_stderr)

    try:
        # Replace streams
        sys.stdout = stdout_capture  # type: ignore
        sys.stderr = stderr_capture  # type: ignore
        yield
    finally:
        # Flush any remaining content
        stdout_capture.flush()
        stderr_capture.flush()
        # Restore original streams
        sys.stdout = original_stdout
        sys.stderr = original_stderr
