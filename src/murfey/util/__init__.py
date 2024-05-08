from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
import shutil
from functools import lru_cache
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Awaitable, Callable, Dict, List, Optional, Union
from urllib.parse import ParseResult, urljoin
from uuid import uuid4

import requests

from murfey.util.models import Visit

logger = logging.getLogger("murfey.util")


@lru_cache(maxsize=1)
def get_machine_config(url: str, demo: bool = False) -> dict:
    return requests.get(f"{url}/machine/").json()


def _get_visit_list(api_base: ParseResult):
    get_visits_url = api_base._replace(path="/visits_raw")
    server_reply = requests.get(get_visits_url.geturl())
    if server_reply.status_code != 200:
        raise ValueError(f"Server unreachable ({server_reply.status_code})")
    return [Visit.parse_obj(v) for v in server_reply.json()]


def capture_post(url: str, json: dict | list = {}) -> requests.Response | None:
    try:
        response = requests.post(url, json=json)
    except Exception as e:
        logger.error(f"Exception encountered in post to {url}: {e}")
        response = None
    if not response or response.status_code != 200:
        if response:
            logger.warning(
                f"Response to post to {url} with data {json} had status code "
                f"{response.status_code}. The reason given was {response.reason}"
            )
        failure_url = urljoin(url, "failed_client_post")
        try:
            resend_response = requests.post(
                failure_url, json={"url": url, "data": json}
            )
        except Exception as e:
            logger.error(f"Exception encountered in post to {failure_url}: {e}")
            resend_response = None
        if resend_response and resend_response.status_code != 200:
            logger.warning(
                f"Response to post to {failure_url} failed with {resend_response.reason}"
            )

    return response


def capture_get(url: str) -> requests.Response | None:
    try:
        response = requests.get(url)
    except Exception as e:
        logger.error(f"Exception encountered in get from {url}: {e}")
        response = None
    if response and response.status_code != 200:
        logger.warning(
            f"Response to get from {url} had status code {response.status_code}. "
            f"The reason given was {response.reason}"
        )
    return response


def set_default_acquisition_output(
    new_output_dir: Path,
    software_settings_output_directories: Dict[str, List[str]],
    safe: bool = True,
):
    for p, keys in software_settings_output_directories.items():
        if safe:
            settings_copy_path = Path(p)
            settings_copy_path = settings_copy_path.parent / (
                "_murfey_" + settings_copy_path.name
            )
            shutil.copy(p, str(settings_copy_path))
        with open(p, "r") as for_parsing:
            settings = json.load(for_parsing)
        # for safety
        settings_copy = copy.deepcopy(settings)

        def _set(d: dict, keys_list: List[str], value: str) -> dict:
            if len(keys_list) > 1:
                tmp_value: Union[dict, str] = _set(
                    d[keys_list[0]], keys_list[1:], value
                )
            else:
                tmp_value = value
            return {_k: tmp_value if _k == keys_list[0] else _v for _k, _v in d.items()}

        settings_copy = _set(settings_copy, keys, str(new_output_dir))

        def _check_dict_structure(d1: dict, d2: dict) -> bool:
            if set(d1.keys()) != set(d2.keys()):
                return False
            for k in d1.keys():
                if isinstance(d1[k], dict):
                    if not isinstance(d2[k], dict):
                        return False
                    _check_dict_structure(d1[k], d2[k])
            return True

        if _check_dict_structure(settings, settings_copy):
            with open(p, "w") as sf:
                json.dump(settings_copy, sf)


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
        super().__init__()

    def subscribe(
        self, fn: Callable[..., Awaitable[None] | None], secondary: bool = False
    ):
        if secondary:
            self._secondary_listeners.append(fn)
        else:
            self._listeners.append(fn)

    async def anotify(self, *args, secondary: bool = False, **kwargs) -> None:
        awaitables: list[Awaitable] = []
        listeners = self._secondary_listeners if secondary else self._listeners
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

    def notify(self, *args, secondary: bool = False, **kwargs) -> None:
        awaitables: list[Awaitable] = []
        listeners = self._secondary_listeners if secondary else self._listeners
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
