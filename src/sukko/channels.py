"""Channel build/parse helpers — validation parity with ``@sukko/sdk`` ``utils.ts``.

A channel is ``{tenant}.{suffix}``: the tenant is the single segment before the first dot (no
interior dots); the suffix is the entire opaque remainder (it may itself contain dots, including
empty interior segments like ``acme..trades``, which are preserved verbatim). No fixed
suffix-segment count is imposed — the server matches the suffix against permission/routing patterns.
"""

from __future__ import annotations

from typing import NamedTuple


class ParsedChannel(NamedTuple):
    """A channel split into its tenant prefix and opaque suffix."""

    tenant: str
    suffix: str


def build_channel(tenant: str, suffix: str) -> str:
    """Build ``{tenant}.{suffix}``.

    Raises :class:`ValueError` (the Python analog of ``utils.ts``'s ``TypeError``) if the tenant is
    empty or contains a dot, or the suffix is empty. These guards keep :func:`build_channel`
    symmetric with :func:`parse_channel`: every value it returns parses back to the identical
    ``(tenant, suffix)`` and is never rejected.
    """
    if tenant == "" or "." in tenant or suffix == "":
        raise ValueError(
            "build_channel: tenant must be a non-empty single segment (no dots) "
            "and suffix must be non-empty"
        )
    return f"{tenant}.{suffix}"


def parse_channel(channel: str) -> ParsedChannel | None:
    """Parse ``{tenant}.{suffix}`` into its parts, or return ``None`` when malformed.

    Returns ``None`` when the channel has no dot, an empty tenant (leading dot), or an empty suffix
    — mirroring ``utils.ts``'s null return exactly. The suffix is kept verbatim (opaque; interior
    dots and empty segments preserved).
    """
    first_dot = channel.find(".")
    if first_dot <= 0:  # no dot, or empty tenant (leading dot)
        return None
    suffix = channel[first_dot + 1 :]
    if suffix == "":  # empty whole suffix
        return None
    return ParsedChannel(tenant=channel[:first_dot], suffix=suffix)
