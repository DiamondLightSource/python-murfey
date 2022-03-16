from __future__ import annotations

import json
import logging


class CustomHandler(logging.Handler):
    def __init__(self, callback):
        """Set up a handler instance, record the callback function."""
        super().__init__()
        self._callback = callback

    def prepare(self, record):
        self.format(record)
        # print("DICT", record.__dict__)
        record_dict = record.__dict__
        record_dict["type"] = "log"
        # record_dict["record"] = record
        # print(json.dumps(record_dict))
        return json.dumps(record_dict)

    def emit(self, record):
        try:
            self._callback(self.prepare(record))
        except Exception:
            self.handleError(record)
