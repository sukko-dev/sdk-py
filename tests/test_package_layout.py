"""Package-layout invariant: no accidental PEP 420 namespace packages under ``src/sukko``.

A subdirectory that holds ``*.py`` modules but no ``__init__.py`` is an implicit namespace package
(:pep:`420`). It imports fine at runtime, but static tools (griffe, IDEs, mypy without
``--namespace-packages``) silently skip it, dropping its exports from generated surfaces. This is
the ``flake8-no-pep420`` rule as an in-repo test -- it guards the ``transport/`` fix and catches any
future subpackage that forgets its ``__init__.py``.
"""

from __future__ import annotations

import importlib
from pathlib import Path

SRC_SUKKO = Path(__file__).resolve().parent.parent / "src" / "sukko"


def _module_dirs() -> list[Path]:
    """Every directory under src/sukko (inclusive) that contains at least one .py module."""
    dirs: list[Path] = []
    for path in [SRC_SUKKO, *sorted(SRC_SUKKO.rglob("*"))]:
        if not path.is_dir() or path.name == "__pycache__":
            continue
        if any(child.suffix == ".py" for child in path.iterdir()):
            dirs.append(path)
    return dirs


def test_no_implicit_namespace_packages() -> None:
    """Every package dir under src/sukko carries an __init__.py (no implicit namespace pkg)."""
    package_dirs = _module_dirs()
    # Sanity: the walk found the tree, including the transport subpackage this guards.
    assert SRC_SUKKO in package_dirs
    assert SRC_SUKKO / "transport" in package_dirs

    missing = [
        d.relative_to(SRC_SUKKO.parent) for d in package_dirs if not (d / "__init__.py").is_file()
    ]
    assert not missing, f"directories missing __init__.py (implicit namespace packages): {missing}"


def test_transport_is_a_regular_package() -> None:
    """sukko.transport resolves as a regular package (spec origin points at its __init__.py)."""
    module = importlib.import_module("sukko.transport")
    # A PEP 420 namespace package has __spec__.origin None; a regular package points at __init__.py.
    assert module.__spec__ is not None
    assert module.__spec__.origin is not None
