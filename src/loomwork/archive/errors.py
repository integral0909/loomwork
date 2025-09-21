"""Errors raised while writing, reading or verifying archives."""

from __future__ import annotations


class ArchiveError(Exception):
    """Base of every error this package raises."""


class ArchiveNotFoundError(ArchiveError):
    """No archived run matches the requested workflow or run id."""


class ArchiveCorruptionError(ArchiveError):
    """A segment or the manifest is missing, truncated or does not match its checksum.

    The message names the segment (and, where possible, the line within it) so an
    operator can tell a truncated transfer from bit rot from a manifest that no longer
    matches the directory's contents.
    """
