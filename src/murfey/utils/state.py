from __future__ import annotations

import asyncio
import inspect
from typing import Awaitable, Callable, Mapping, TypeVar, Union

T = TypeVar("T")
GlobalStateValues = Union[str, int, None]


class State(Mapping[str, T]):
    """A helper class to coordinate shared state across server instances.
    This is a Mapping with added (synchronous) set and delete functionality,
    as well as asynchronous .update/.delete calls. It implements the Observer
    pattern notifying synchronous and asynchronous callback functions.
    """

    def __init__(self):
        self.data: dict[str, T] = {}
        self._listeners: list[Callable[[str, T | None], Awaitable[None] | None]] = []
        super().__init__()

    def __repr__(self):
        return f"{type(self).__name__}({self.data}; {len(self._listeners)} subscribers)"

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __contains__(self, key) -> bool:
        return key in self.data

    def __getitem__(self, key: str) -> T:
        if key in self.data:
            return self.data[key]
        raise KeyError(key)

    async def delete(self, key: str):
        del self.data[key]
        await self._async_notify(key, None)

    async def update(self, key: str, value: T):
        self.data[key] = value
        await self._async_notify(key, value)

    def subscribe(self, fn: Callable[[str, T | None], Awaitable[None] | None]):
        self._listeners.append(fn)

    async def _async_notify(self, key: str, value: T | None):
        awaitables: list[Awaitable] = []
        for notify_function in self._listeners:
            result = notify_function(key, value)
            if result is not None and inspect.isawaitable(result):
                awaitables.append(result)
        if awaitables:
            await self._await_all(awaitables)

    @staticmethod
    async def _await_all(awaitables: list[Awaitable]):
        for awaitable in asyncio.as_completed(awaitables):
            await awaitable

    def _sync_notify(self, key: str, value: T | None):
        awaitables: list[Awaitable] = []
        for notify_function in self._listeners:
            result = notify_function(key, value)
            if result is not None and inspect.isawaitable(result):
                awaitables.append(result)
        if awaitables:
            asyncio.run(self._await_all(awaitables))

    def __setitem__(self, key: str, item: T):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # This is synchronous code, we're not running in an event loop
            self.data[key] = item
            self._sync_notify(key, item)
            return
        raise RuntimeError(
            "__setitem__() called from async code. Use async .update() instead"
        )

    def __delitem__(self, key: str):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # This is synchronous code, we're not running in an event loop
            del self.data[key]
            self._sync_notify(key, None)
            return
        raise RuntimeError(
            "__delitem__() called from async code. Use async .delete() instead"
        )


global_state: State[GlobalStateValues] = State()
