"""Public-API smoke — the curated `sukko` surface imports and is usable."""

from __future__ import annotations

from importlib.metadata import version

import sukko


def test_top_level_exports() -> None:
    from sukko import (
        CLOSE_CODES,
        ConnectionState,
        Message,
        SukkoClient,
        SukkoError,
        SyncSukkoClient,
        build_channel,
        parse_channel,
    )

    # __version__ must stay in sync with the packaged (pyproject) version, rather
    # than a hardcoded literal that silently goes stale on every release bump.
    assert sukko.__version__ == version("sukko")
    assert build_channel("acme", "trades") == "acme.trades"
    assert parse_channel("acme.trades") is not None
    assert ConnectionState.CONNECTED == "connected"
    assert CLOSE_CODES.POLICY_VIOLATION == 1008
    # types exist and are the expected objects
    assert SukkoClient.__name__ == "SukkoClient"
    assert SyncSukkoClient.__name__ == "SyncSukkoClient"
    assert issubclass(SukkoError, Exception)
    assert Message.__name__ == "Message"


def test_all_names_are_importable() -> None:
    for name in sukko.__all__:
        assert hasattr(sukko, name), f"__all__ lists {name!r} but it is not on the module"
