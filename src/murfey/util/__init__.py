from __future__ import annotations

import asyncio
import inspect
from functools import lru_cache
from queue import Queue
from threading import Thread
from typing import Awaitable, Callable, Optional
from urllib.parse import ParseResult
from uuid import uuid4

import requests

from murfey.util.models import Visit


@lru_cache(maxsize=1)
def get_machine_config(url: str, demo: bool = False) -> dict:
    if demo:
        return {}
    return requests.get(f"{url}/machine/").json()


def _get_visit_list(api_base: ParseResult):
    get_visits_url = api_base._replace(path="/visits_raw")
    server_reply = requests.get(get_visits_url.geturl())
    if server_reply.status_code != 200:
        raise ValueError(f"Server unreachable ({server_reply.status_code})")
    return [Visit.parse_obj(v) for v in server_reply.json()]


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
        super().__init__()

    def subscribe(self, fn: Callable[..., Awaitable[None] | None]):
        self._listeners.append(fn)

    async def anotify(self, *args, **kwargs) -> None:
        awaitables: list[Awaitable] = []
        for notify_function in self._listeners:
            result = notify_function(*args, **kwargs)
            if result is not None and inspect.isawaitable(result):
                awaitables.append(result)
        if awaitables:
            await self._await_all(awaitables)

    @staticmethod
    async def _await_all(awaitables: list[Awaitable]):
        for awaitable in asyncio.as_completed(awaitables):
            await awaitable

    def notify(self, *args, **kwargs) -> None:
        awaitables: list[Awaitable] = []
        for notify_function in self._listeners:
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
