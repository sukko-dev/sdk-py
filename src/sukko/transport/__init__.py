"""Transport adapters for the Sukko client (WebSocket, SSE).

This package intentionally carries an ``__init__.py``. Without it, ``transport/`` would be an
implicit :pep:`420` namespace package -- importable at runtime, but silently skipped by static
analysis tools (griffe, IDEs, mypy without ``--namespace-packages``), which drops the public
transport exports from generated API surfaces. The public transport types
(:class:`~sukko.Transport`, :class:`~sukko.TransportCapabilities`, :class:`~sukko.ConnectionState`,
:class:`~sukko.SseTransport`, :class:`~sukko.WebSocketTransport`) are re-exported from the top-level
``sukko`` package.
"""
