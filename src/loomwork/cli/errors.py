"""Errors and exit codes of the ``loomwork`` command."""

from __future__ import annotations

import enum


class ExitCode(enum.IntEnum):
    """Process exit codes.

    ``FAILURE`` means the command ran but its answer is "no": a lint found problems, a
    compatibility check found runs that would break, a workflow result was a failure.
    ``USAGE`` means the command itself was wrong. Scripts rely on telling the two apart.
    """

    OK = 0
    FAILURE = 1
    USAGE = 2
    NOT_FOUND = 3
    UNAVAILABLE = 4
    INTERRUPTED = 130


class CliError(Exception):
    """An error reported to the user as one line on stderr, without a traceback."""

    def __init__(self, message: str, code: ExitCode = ExitCode.FAILURE) -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class UsageError(CliError):
    def __init__(self, message: str) -> None:
        super().__init__(message, ExitCode.USAGE)
