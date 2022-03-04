from __future__ import annotations

import logging


class CustomHandler(logging.Handler):
    def prepare(self, record):
        self.format(
            record
        )  # when format() isn't called, the message "Hello" is preserved
        record.msg = (
            record.message
        )  # when record.msg = "Hello" the entire message is "Hello"
        # record.args = None
        # record.exc_info = None
        return record

    def emit(self, record):
        try:
            self.prepare(record)
        except Exception:
            self.handleError(record)
