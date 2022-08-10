from __future__ import annotations

import json
import logging
from asyncio import Queue
from datetime import datetime
from pathlib import Path
from typing import List

from rich.console import Console, RenderableType
from rich.containers import Renderables
from rich.logging import RichHandler
from rich.text import Text


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
        self._console = Console()
        self._last_time = None

    def get_log_row(self, record, message_renderable) -> list:
        row: List[RenderableType] = []
        renderables = [message_renderable]
        path = Path(record.pathname).name
        level = self.get_level_text(record)
        time_format = None if self.formatter is None else self.formatter.datefmt
        log_time = datetime.fromtimestamp(record.created) or self.console.get_datetime()
        time_format = time_format or self._log_render.time_format
        link_path = record.pathname if self.enable_link_path else None
        if callable(time_format):
            log_time_display = time_format(log_time)
        else:
            log_time_display = Text(log_time.strftime(time_format))
        if log_time_display == self._last_time:
            row.append(Text(" " * len(log_time_display)))
        else:
            row.append(log_time_display)
            self._last_time = log_time_display
        row.append(level)

        row.append(Renderables(renderables))
        if path:
            path_text = Text()
            path_text.append(
                path, style=f"link file://{link_path}" if link_path else ""
            )
            if record.lineno:
                path_text.append(":")
                path_text.append(
                    f"{record.lineno}",
                    style=f"link file://{link_path}#{record.lineno}"
                    if link_path
                    else "",
                )
            row.append(path_text)
        return row

    def emit(self, record):
        try:
            if self.redirect:
                message = self.format(record)
                message_renderable = self.render_message(record, message)
                rendered_log = self.render(
                    record=record,
                    traceback=None,
                    message_renderable=message_renderable,
                )
                rendered_log_row = self.get_log_row(
                    record=record, message_renderable=message_renderable
                )
                self._queue.put((rendered_log_row, rendered_log))
            else:
                super().emit(record)
        except Exception:
            self.handleError(record)
