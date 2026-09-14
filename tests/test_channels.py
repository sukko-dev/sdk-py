"""Channel-helper tests — build/parse round-trip + edge semantics (parity utils.ts)."""

from __future__ import annotations

import pytest

from sukko.channels import ParsedChannel, build_channel, parse_channel


def test_build_parse_roundtrip() -> None:
    channel = build_channel("acme", "rooms.eng")
    assert channel == "acme.rooms.eng"
    assert parse_channel(channel) == ParsedChannel(tenant="acme", suffix="rooms.eng")


def test_build_channel_rejects_bad_input() -> None:
    with pytest.raises(ValueError):
        build_channel("", "suffix")
    with pytest.raises(ValueError):
        build_channel("acme.sub", "suffix")  # dotted tenant
    with pytest.raises(ValueError):
        build_channel("acme", "")  # empty suffix


def test_parse_channel_none_cases() -> None:
    assert parse_channel("nodot") is None  # no dot
    assert parse_channel(".suffix") is None  # empty tenant (leading dot)
    assert parse_channel("acme.") is None  # empty suffix


def test_parse_channel_preserves_opaque_suffix() -> None:
    # interior dots and empty segments in the suffix are kept verbatim
    assert parse_channel("acme..trades") == ParsedChannel(tenant="acme", suffix=".trades")
    assert parse_channel("acme.a.b.c") == ParsedChannel(tenant="acme", suffix="a.b.c")
