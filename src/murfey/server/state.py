from __future__ import annotations

import collections
from typing import Callable, Union

AttributeValue = Union[str, int, None]
UpdateFunction = Callable[[str, AttributeValue], None]


class State(collections.UserDict[str, AttributeValue]):
    """A helper class to coordinate shared state across server instances.
    This is a dictionary implementing the Observer pattern and is mostly used
    as a singleton."""

    def __init__(self):
        self._listeners: list[UpdateFunction] = []
        super().__init__()

    def __delitem__(self, key):
        super().__delitem__(key)
        for notify_function in self._listeners:
            notify_function(key, None)

    def __setitem__(self, key: str, value: AttributeValue):
        super().__setitem__(key, value)
        for notify_function in self._listeners:
            notify_function(key, value)

    def __repr__(self):
        return f"{type(self).__name__}({self.data}; {len(self._listeners)} subscribers)"

    def subscribe(self, fn: UpdateFunction):
        self._listeners.append(fn)


state = State()
