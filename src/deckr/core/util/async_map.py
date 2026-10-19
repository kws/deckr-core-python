"""Async map."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Generic

import anyio

from deckr.core.util.state import K, V


class ConcurrentModificationError(RuntimeError):
    """Raised when the map is modified during iteration."""

    pass


class AsyncMap(Generic[K, V]):
    """Thread-safe async map with concurrent modification detection.

    Provides a dictionary-like interface with async operations protected by
    a lock. Iteration detects concurrent modifications and raises
    ConcurrentModificationError if the map is modified during iteration.
    """

    def __init__(
        self, initial_values: Mapping[K, V] | None = None, proxy: bool = False
    ):
        """Initialize an AsyncMap.

        Args:
            initial_values: Initial key-value pairs to populate the map
            proxy: If True, act as a proxy to initial_values instead of copying.
                WARNING: External modifications to the underlying map will bypass
                thread-safety guarantees and break concurrent modification detection.
                Only use if you can guarantee the underlying map won't be modified
                externally and you need the memory/performance benefits.
        """
        assert not (proxy and initial_values is None), (
            "initial_values must be provided if proxy is True"
        )
        if proxy and initial_values is not None:
            self._map = initial_values
            self._is_proxy = True
        else:
            self._map = dict(initial_values) if initial_values is not None else {}
            self._is_proxy = False
        self._lock = anyio.Lock()
        self._version = 0

    async def get(self, key: K, default: V | None = None) -> V:
        """Get the value for a key, or default if not present."""
        async with self._lock:
            return self._map.get(key, default)

    async def set(self, key: K, value: V) -> None:
        """Set a key-value pair."""
        async with self._lock:
            self._map[key] = value
            self._version += 1

    async def get_and_set(self, key: K, value: V) -> V | None:
        """Get the value for a key, and set a new value."""
        async with self._lock:
            old_value = self._map.get(key)
            self._map[key] = value
            self._version += 1
        return old_value

    async def items(self) -> list[tuple[K, V]]:
        """Return a list of (key, value) pairs."""
        async with self._lock:
            return list(self._map.items())

    async def keys(self) -> list[K]:
        """Return a list of all keys."""
        async with self._lock:
            return list(self._map.keys())

    async def values(self) -> list[V]:
        """Return a list of all values."""
        async with self._lock:
            return list(self._map.values())

    async def clear(self) -> None:
        """Remove all items from the map."""
        async with self._lock:
            self._map.clear()
            self._version += 1

    async def has_key(self, key: K) -> bool:
        """Check if a key exists in the map."""
        async with self._lock:
            return key in self._map

    async def delete(self, key: K) -> None:
        """Delete a key from the map. Does nothing if key doesn't exist."""
        async with self._lock:
            if key in self._map:
                del self._map[key]
                self._version += 1

    async def pop(self, key: K, default: V | None = None) -> V | None:
        """Remove and return the value for a key.

        Args:
            key: The key to remove
            default: Value to return if key doesn't exist (default: None)

        Returns:
            The value for the key, or default if key doesn't exist
        """
        async with self._lock:
            if key in self._map:
                value = self._map.pop(key)
                self._version += 1
                return value
            return default

    @asynccontextmanager
    async def lock(self) -> AsyncGenerator[None, None]:
        """Allows direct access to the underlying map in a single 'transaction'.
        Since the map may be modified during the transaction, the version is always incremented.
        """
        async with self._lock:
            yield self._map
            self._version += 1

    async def __aiter__(self) -> AsyncIterator[tuple[K, V]]:
        """Iterate over (key, value) pairs.

        Raises ConcurrentModificationError if the map is modified during iteration.
        """
        # Capture version and snapshot under lock
        async with self._lock:
            initial_version = self._version
            items = list(self._map.items())

        # Iterate outside lock, checking for concurrent modifications
        for key, value in items:
            async with self._lock:
                if self._version != initial_version:
                    raise ConcurrentModificationError(
                        "AsyncMap was modified during iteration"
                    )
            yield key, value
