"""Contract-coverage test: every AsyncAPI message ``type`` has a typed SDK model, and the
SDK's numeric bounds match the server's ``envDefault`` source of truth.

This is the mechanical guard behind the founding rule ("derive from the contract, don't copy").
It parses the authoritative AsyncAPI YAML directly rather than trusting a hand-maintained list.

**Cross-repo path resolution.** The contract lives in the sibling ``sukko`` checkout. That path
exists in local dev but not on a bare CI runner, so:
- the AsyncAPI path comes from ``SUKKO_ASYNCAPI_PATH`` (default: the sibling checkout),
- the server-config path from ``SUKKO_SERVER_CONFIG`` (default: the sibling checkout),
- and each assertion **skips with a clear message** when its source file is absent.

Skipping keeps ordinary CI green until the permanent portability story is wired (vendor a pinned
copy of the contract into the repo, or check the sibling out in CI). Until that portability story
lands, this test is fully enforced locally, where the contract is present.
"""

from __future__ import annotations

import os
import re
import typing
from pathlib import Path
from typing import Any

import pytest
import yaml

from sukko import constants, messages

_DEFAULT_ASYNCAPI = "../sukko/ws/docs/asyncapi/client-ws.asyncapi.yaml"
_DEFAULT_SERVER_CONFIG = "../sukko/ws/internal/shared/platform/server_config.go"


def _asyncapi_path() -> Path:
    return Path(os.environ.get("SUKKO_ASYNCAPI_PATH", _DEFAULT_ASYNCAPI))


def _server_config_path() -> Path:
    return Path(os.environ.get("SUKKO_SERVER_CONFIG", _DEFAULT_SERVER_CONFIG))


def _collect_contract_message_types(node: Any, found: set[str]) -> None:
    """Recursively collect every message ``type`` identifier: the ``const``/``enum`` value(s) of any
    mapping keyed by ``type`` (the protocol's discriminator pattern). JSON-schema type declarations
    (``type: string``) map ``type`` to a *string*, not a dict, so they are naturally excluded.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "type" and isinstance(value, dict):
                if "const" in value:
                    found.add(value["const"])
                for item in value.get("enum", []):
                    found.add(item)
            _collect_contract_message_types(value, found)
    elif isinstance(node, list):
        for item in node:
            _collect_contract_message_types(item, found)


def _modeled_tags(union: object) -> set[str]:
    """The set of wire ``type`` tags modeled by a msgspec tagged-union alias."""
    tags: set[str] = set()
    for struct in typing.get_args(union):
        tag = struct.__struct_config__.tag
        assert isinstance(tag, str), f"{struct.__name__} has a non-string tag {tag!r}"
        tags.add(tag)
    return tags


def test_every_contract_message_type_has_a_model() -> None:
    """Every AsyncAPI client-ws message ``type`` is represented by an SDK model."""
    path = _asyncapi_path()
    if not path.exists():
        pytest.skip(f"AsyncAPI contract not found at {path} (set SUKKO_ASYNCAPI_PATH)")

    spec = yaml.safe_load(path.read_text())
    contract_types: set[str] = set()
    _collect_contract_message_types(spec, contract_types)
    assert contract_types, "parsed no message types from the contract — parser or contract changed"

    modeled = _modeled_tags(messages.ServerMessage) | _modeled_tags(messages.ClientMessage)

    missing = contract_types - modeled
    assert not missing, f"contract message types with no SDK model: {sorted(missing)}"

    # No phantom models: every modeled tag is a real contract type (PossibleGap is intentionally not
    # in either union, so it is not checked here — it is an SDK-internal event, not a wire type).
    phantom = modeled - contract_types
    assert not phantom, f"SDK models a `type` the contract does not define: {sorted(phantom)}"


def test_contract_type_counts() -> None:
    """Guards the documented counts (18 server + 8 client) so a contract change is noticed."""
    assert len(_modeled_tags(messages.ServerMessage)) == 18
    assert len(_modeled_tags(messages.ClientMessage)) == 8


def _go_env_default(source: str, env_var: str) -> int | None:
    """Extract ``env:"NAME" envDefault:"<int>"`` for ``env_var`` from a Go config source."""
    match = re.search(rf'env:"{re.escape(env_var)}"\s+envDefault:"(\d+)"', source)
    return int(match.group(1)) if match else None


def test_sdk_bounds_match_server_env_defaults() -> None:
    """Finding A/Q: the SDK's ``MAX_REPLAY_MESSAGES`` / ``DEFAULT_HISTORY_LIMIT`` track the server's
    ``envDefault`` source of truth — the AsyncAPI references these bounds only symbolically, so the
    numeric authority is the Go config."""
    path = _server_config_path()
    if not path.exists():
        pytest.skip(f"server config not found at {path} (set SUKKO_SERVER_CONFIG)")

    source = path.read_text()
    max_replay = _go_env_default(source, "WS_MAX_REPLAY_MESSAGES")
    history_max = _go_env_default(source, "WS_HISTORY_MAX_LIMIT")
    assert max_replay is not None, "WS_MAX_REPLAY_MESSAGES envDefault not found — config changed"
    assert history_max is not None, "WS_HISTORY_MAX_LIMIT envDefault not found — config changed"

    assert max_replay == constants.MAX_REPLAY_MESSAGES, (
        f"MAX_REPLAY_MESSAGES ({constants.MAX_REPLAY_MESSAGES}) != server {max_replay}"
    )
    assert history_max == constants.DEFAULT_HISTORY_LIMIT, (
        f"DEFAULT_HISTORY_LIMIT ({constants.DEFAULT_HISTORY_LIMIT}) != server {history_max}"
    )


def test_queue_maxsize_default_satisfies_recovery_floor() -> None:
    """The default queue size must clear the construction floor (history_limit + max_replay)."""
    floor = constants.DEFAULT_HISTORY_LIMIT + constants.MAX_REPLAY_MESSAGES
    assert floor <= constants.DEFAULT_QUEUE_MAXSIZE, (
        f"DEFAULT_QUEUE_MAXSIZE ({constants.DEFAULT_QUEUE_MAXSIZE}) < floor {floor}"
    )
