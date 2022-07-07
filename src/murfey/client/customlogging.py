from __future__ import annotations

import json
import logging
from asyncio import Queue

from rich.logging import RichHandler


class CustomHandler(logging.Handler):
    def __init__(self, callback):
        """Set up a handler instance, record the callback function."""
        super().__init__()
        self._callback = callback

    def prepare(self, record):
        self.format(record)
        record_dict = record.__dict__
        record_dict["type"] = "log"
        return json.dumps(record_dict)

    def emit(self, record):
        try:
            self._callback(self.prepare(record))
        except Exception:
            self.handleError(record)


class DirectableRichHandler(RichHandler):
    def __init__(self, queue: Queue, **kwargs):
        super().__init__(**kwargs)
        self._queue = queue
        self.redirect = False

    def emit(self, record):
        try:
            if self.redirect:
                self._queue.put(record)
            else:
                super().emit(record)
        except Exception:
            self.handleError(record)
