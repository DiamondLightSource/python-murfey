from __future__ import annotations

import json
import logging
from asyncio import Queue
from datetime import datetime
from pathlib import Path
from typing import List, TypeVar

from rich.console import Console, RenderableType
from rich.containers import Renderables
from rich.logging import RichHandler
from rich.table import Table
from rich.text import Text
from textual.reactive import Reactive

ReactiveType = TypeVar("ReactiveType")


class RedirectedReactive(Reactive):
    def __init__(
        self,
        default: ReactiveType,
        *,
        layout: bool = False,
        repaint: bool = True,
    ) -> None:
        self.redirection = None
        super().__init__(default, layout=layout, repaint=repaint)

    def redirect_to(self, obj):
        self.redirection = obj
        self.repaint = True

    def __get__(self, obj, obj_type):
        return self

    def __set__(self, obj, value):
        if self.redirection:
            setattr(obj, self.internal_name, value)
            super().__set__(self.redirection, value)
        else:
            super().__set__(obj, value)


class LogHolder:
    def __init__(self, rendered_log):
        self.rendered_log = rendered_log


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
    next_log = RedirectedReactive(Text("log book"), repaint=False)

    def __init__(self, queue: Queue, lock, **kwargs):
        super().__init__(**kwargs)
        self._queue = queue
        self._lock = lock
        self.redirect = False
        self._console = Console()
        self._last_time = None

    def get_log_row(self, record, message_renderable) -> list:
        row: List["RenderableType"] = []
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
                with self._lock:
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
                    if self.next_log.redirection is not None:
                        if not isinstance(
                            getattr(self, self.next_log.internal_name), Table
                        ):
                            self.next_log = rendered_log
                        else:
                            if rendered_log_row:
                                self.next_log._first = True
                                getattr(self, self.next_log.internal_name).add_row(
                                    *rendered_log_row
                                )
                                self.next_log = getattr(
                                    self, self.next_log.internal_name
                                )
            else:
                super().emit(record)
        except Exception:
            self.handleError(record)
