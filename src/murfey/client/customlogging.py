from __future__ import annotations

import json
import logging

from rich.logging import RichHandler

from murfey.client.tui.screens import LogBook

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
        return json.dumps(record_dict)

    def emit(self, record):
        try:
            self._callback(self.prepare(record))
        except Exception:
            self.handleError(record)


class DirectableRichHandler(RichHandler):
    def __init__(self, text_log: LogBook | None = None, **kwargs):
        super().__init__(**kwargs)
        self.text_log = text_log
        self.redirect = False
        self._last_time = None

    def emit(self, record):
        try:
            if self.text_log:
                message = self.format(record)
                message_renderable = self.render_message(record, message)
                log_renderable = self.render(
                    record=record, traceback=None, message_renderable=message_renderable
                )
                self.text_log.post_message(self.text_log.Log(log_renderable))
        except Exception:
            self.handleError(record)
