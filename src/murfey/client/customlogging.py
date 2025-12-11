from __future__ import annotations

import json
import logging

logger = logging.getLogger("murfey.client.customlogging")


class CustomHandler(logging.Handler):
    def __init__(self, callback):
        """Set up a handler instance, record the callback function."""
        super().__init__()
        self._callback = callback

    def prepare(self, record):
        self.format(record)
        record_dict = record.__dict__
        record_dict["type"] = "log"
        try:
            return json.dumps(record_dict)
        except TypeError:
            return json.dumps({str(k): str(v) for k, v in record_dict.items()})

    def emit(self, record):
        try:
            self._callback(self.prepare(record))
        except Exception:
            self.handleError(record)
