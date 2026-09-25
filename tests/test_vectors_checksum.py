"""Checksum guard for the vendored parity-vector corpus — the go.sum-style pin that platform
ADR-0023 / sukko-py ADR-0004 rely on. Without it a stale or hand-edited vendored vector would still
round-trip its own binding and pass silently. Fails when a vendored vector's sha256 no longer
matches CHECKSUMS, or when a vector is vendored without a CHECKSUMS entry.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

VECTORS_DIR = Path(__file__).parent / "vectors"


def _read_checksums() -> dict[str, str]:
    """Parse the shasum-format CHECKSUMS → {"recovery/<name>.json": "<sha256hex>"}."""
    sums: dict[str, str] = {}
    for line in (VECTORS_DIR / "CHECKSUMS").read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        fields = stripped.split()
        assert len(fields) == 2, f"CHECKSUMS line not in shasum format: {stripped!r}"
        sums[fields[1].lstrip("*")] = fields[0]
    return sums


_CHECKSUMS = _read_checksums()


def test_checksums_not_empty() -> None:
    assert _CHECKSUMS, "vectors CHECKSUMS is empty"


@pytest.mark.parametrize("name", sorted(_CHECKSUMS), ids=lambda n: n)
def test_vendored_vector_matches_checksum(name: str) -> None:
    path = VECTORS_DIR / name
    assert path.exists(), f"CHECKSUMS names {name} but it is not vendored"
    got = hashlib.sha256(path.read_bytes()).hexdigest()
    assert got == _CHECKSUMS[name], (
        f"checksum mismatch for {name} — re-vendor the vector and regenerate CHECKSUMS together"
    )


def test_every_vendored_vector_has_a_checksum() -> None:
    for path in sorted((VECTORS_DIR / "recovery").glob("*.json")):
        rel = f"recovery/{path.name}"
        assert rel in _CHECKSUMS, f"{rel} is vendored but has no CHECKSUMS entry"
