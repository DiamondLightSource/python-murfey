from __future__ import annotations

import collections
from typing import Callable, TypeVar, Union

T = TypeVar("T")
GlobalStateValues = Union[str, int, None]


class State(collections.UserDict[str, T]):
    """A helper class to coordinate shared state across server instances.
    This is a dictionary implementing the Observer pattern and is mostly used
    as a singleton."""

    def __init__(self):
        self._listeners: list[Callable[[str, T | None], None]] = []
        super().__init__()

    def __delitem__(self, key: str):
        super().__delitem__(key)
        for notify_function in self._listeners:
            notify_function(key, None)

    def __setitem__(self, key: str, value: T):
        super().__setitem__(key, value)
        for notify_function in self._listeners:
            notify_function(key, value)

    def __repr__(self):
        return f"{type(self).__name__}({self.data}; {len(self._listeners)} subscribers)"

    def subscribe(self, fn: Callable[[str, T | None], None]):
        self._listeners.append(fn)


global_state: State[GlobalStateValues] = State()
