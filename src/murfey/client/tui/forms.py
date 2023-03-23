from __future__ import annotations

from collections import UserString
from typing import Any


class TUIFormValue(UserString):
    def __init__(
        self, seq: Any, top: bool = False, colour: str = "", readable_label: str = ""
    ):
        super().__init__(seq)
        self._top = top
        self._colour = colour
        self.readable_label = readable_label or str(seq)

    def __str__(self):
        if self._colour:
            return f"[{self._colour}]{self.data}[/{self._colour}]"
        return self.data

    def __eq__(self, other: object):
        if isinstance(other, UserString):
            return self.data == other.data
        return self.data == other
