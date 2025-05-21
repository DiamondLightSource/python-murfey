from __future__ import annotations

import logging
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Any, Callable, Optional
from uuid import uuid4

from werkzeug.utils import secure_filename

logger = logging.getLogger("murfey.util")


def sanitise(in_string: str) -> str:
    return in_string.replace("\r\n", "").replace("\n", "")


def sanitise_path(in_path: Path) -> Path:
    return Path("/".join(secure_filename(p) for p in in_path.parts))


def sanitise_nonpath(in_string: str) -> str:
    for c in ("\r\n", "\n", "/", "\\", ":", ";"):
        in_string = in_string.replace(c, "")
    return in_string


def secure_path(in_path: Path, keep_spaces: bool = False) -> Path:
    if keep_spaces:
        secured_parts = []
        for p, part in enumerate(in_path.parts):
            if " " in part:
                secured_parts.append(part)
            elif ":" in part and not p:
                secured_parts.append(secure_filename(part) + ":")
            else:
                secured_parts.append(secure_filename(part))
    else:
        secured_parts = [
            (
                secure_filename(part) + ":"
                if p == 0 and ":" in part
                else secure_filename(part)
            )
            for p, part in enumerate(in_path.parts)
        ]
    return Path("/".join(secured_parts))


def posix_path(path: Path) -> str:
    """
    Converts a Windows-style path into a Posix one. Used primarily when running
    subproceses in bash terminals on Windows devices, which can only accept
    Posix paths.

    Returns it as a string because this path won't be recognised as an existing
    path when converted to a Path object.
    """
    path_parts = list(path.parts)
    # Check if it's a Windows-style path and converts it to a Posix one
    #   e.g.: C:\Users\user -> /c/Users/user
    if path_parts[0].endswith((":/", ":\\")):
        path_parts[0] = "/" + path_parts[0].strip(":/\\").lower()
        posix_path = "/".join(path_parts)
        return posix_path
    return str(path)


class Processor:
    def __init__(self, name: Optional[str] = None):
        self._in: Queue = Queue()
        self._out: Queue = Queue()
        self._previous: Optional[Processor] = None
        self.thread: Optional[Thread] = None
        self.name = name or str(uuid4())[:8]

    def __rshift__(self, other: Processor):
        self.point_to(other)

    def point_to(self, other: Processor):
        if isinstance(other, Processor):
            other._in = self._out
            other._previous = self

    def process(self, in_thread: bool = False, thread_name: str = "", **kwargs):
        if in_thread:
            self.thread = Thread(
                target=self._process,
                kwargs=kwargs,
                name=thread_name or self.name,
            )
            self.thread.start()
        else:
            self._process(**kwargs)

    def _process(self, **kwargs):
        pass

    def wait(self):
        if self.thread:
            self.thread.join()


class LogFilter(logging.Filter):
    """A filter to limit messages going to Graylog"""

    def __repr__(self):
        return "<murfey.server.LogFilter>"

    def __init__(self):
        super().__init__()
        self._filter_levels = {
            "murfey": logging.DEBUG,
            "ispyb": logging.DEBUG,
            "zocalo": logging.DEBUG,
            "uvicorn": logging.INFO,
            "fastapi": logging.INFO,
            "starlette": logging.INFO,
            "sqlalchemy": logging.INFO,
        }

    @staticmethod
    def install() -> LogFilter:
        logfilter = LogFilter()
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            handler.addFilter(logfilter)
        return logfilter

    def filter(self, record: logging.LogRecord) -> bool:
        logger_name = record.name
        while True:
            if logger_name in self._filter_levels:
                return record.levelno >= self._filter_levels[logger_name]
            if "." not in logger_name:
                return False
            logger_name = logger_name.rsplit(".", maxsplit=1)[0]


def safe_run(
    func: Callable,
    args: list | tuple = [],
    kwargs: dict[str, Any] = {},
    label: str = "",
):
    """
    A wrapper to encase individual functions in try-except blocks so that a warning
    is raised if the function fails, but the process continues as normal otherwise.
    """
    try:
        return func(*args, **kwargs)
    except Exception:
        logger.warning(
            f"Function {func.__name__!r} failed to run for object {label!r}",
            exc_info=True,
        )
        return None
