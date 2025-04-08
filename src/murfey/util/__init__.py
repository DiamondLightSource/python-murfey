from __future__ import annotations

import asyncio
import inspect
import logging
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Awaitable, Callable, Optional
from uuid import uuid4

from werkzeug.utils import secure_filename

logger = logging.getLogger("murfey.util")


def sanitise(in_string: str) -> str:
    return in_string.replace("\r\n", "").replace("\n", "")


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


class Observer:
    """
    A helper class implementing the observer pattern supporting both
    synchronous and asynchronous notification calls and both synchronous and
    asynchronous callback functions.
    """

    # The class here should be derived from typing.Generic[P]
    # with P = ParamSpec("P"), and the notify/anotify functions should use
    # *args: P.args, **kwargs: P.kwargs.
    # However, ParamSpec is Python 3.10+ (PEP 612), so we can't use that yet.

    def __init__(self):
        self._listeners: list[Callable[..., Awaitable[None] | None]] = []
        self._secondary_listeners: list[Callable[..., Awaitable[None] | None]] = []
        self._final_listeners: list[Callable[..., Awaitable[None] | None]] = []
        super().__init__()

    def subscribe(
        self,
        fn: Callable[..., Awaitable[None] | None],
        secondary: bool = False,
        final: bool = False,
    ):
        if final:
            self._final_listeners.append(fn)
        elif secondary:
            self._secondary_listeners.append(fn)
        else:
            self._listeners.append(fn)

    async def anotify(
        self, *args, secondary: bool = False, final: bool = False, **kwargs
    ) -> None:
        awaitables: list[Awaitable] = []
        listeners = (
            self._secondary_listeners
            if secondary
            else self._final_listeners if final else self._listeners
        )
        for notify_function in listeners:
            result = notify_function(*args, **kwargs)
            if result is not None and inspect.isawaitable(result):
                awaitables.append(result)
        if awaitables:
            await self._await_all(awaitables)

    @staticmethod
    async def _await_all(awaitables: list[Awaitable]):
        for awaitable in asyncio.as_completed(awaitables):
            await awaitable

    def notify(
        self, *args, secondary: bool = False, final: bool = False, **kwargs
    ) -> None:
        awaitables: list[Awaitable] = []
        listeners = (
            self._secondary_listeners
            if secondary
            else self._final_listeners if final else self._listeners
        )
        for notify_function in listeners:
            result = notify_function(*args, **kwargs)
            if result is not None and inspect.isawaitable(result):
                awaitables.append(result)
        if awaitables:
            asyncio.run(self._await_all(awaitables))


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
