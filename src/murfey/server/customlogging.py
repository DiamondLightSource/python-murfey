from __future__ import annotations

import logging


class CustomHandler(logging.Handler):
    def prepare(self, record):
        self.format(record)
        record.msg = record.message
        # record.args = None
        # record.exc_info = None
        return record

    def emit(self, record):
        try:
            self.prepare(record)
        except Exception:
            self.handleError(record)
