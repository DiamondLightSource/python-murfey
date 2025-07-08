"""
Utility functions used solely by the Murfey client. They help set up its
configuration, communicate with the backend server using the correct credentials,
and set default directories to work with.
"""

from __future__ import annotations

import asyncio
import configparser
import copy
import inspect
import json
import logging
import os
import shutil
from functools import lru_cache, partial
from pathlib import Path
from typing import Awaitable, Callable, Optional, Union
from urllib.parse import urlparse, urlunparse

import requests

from murfey.util.api import url_path_for

logger = logging.getLogger("murfey.util.client")


def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()

    # Look for 'MURFEY_CLIENT_CONFIGURATION' environment variable first
    mcc = os.environ.get("MURFEY_CLIENT_CONFIGURATION")
    if mcc:
        config_file = Path(mcc)
    # If not set, look for 'MURFEY_CLIENT_CONFIG_HOME' or '~' and then for '.murfey'
    else:
        mcch = os.environ.get("MURFEY_CLIENT_CONFIG_HOME")
        murfey_client_config_home = Path(mcch) if mcch else Path.home()
        config_file = murfey_client_config_home / ".murfey"

    # Attempt to read the file and return the config
    try:
        with open(config_file) as file:
            config.read_file(file)
    except FileNotFoundError:
        logger.warning(
            f"Murfey client configuration file {str(config_file)!r} not found"
        )
    if "Murfey" not in config:
        config["Murfey"] = {}
    return config


@lru_cache(maxsize=1)
def get_machine_config_client(
    url: str, instrument_name: str = "", demo: bool = False
) -> dict:
    _instrument_name: Optional[str] = instrument_name or os.getenv("BEAMLINE")
    if not _instrument_name:
        return {}
    return requests.get(
        f"{url}{url_path_for('session_control.router', 'machine_info_by_instrument', instrument_name=_instrument_name)}"
    ).json()


def authorised_requests() -> tuple[Callable, Callable, Callable, Callable]:
    token = read_config()["Murfey"].get("token", "")
    _get = partial(requests.get, headers={"Authorization": f"Bearer {token}"})
    _post = partial(requests.post, headers={"Authorization": f"Bearer {token}"})
    _put = partial(requests.put, headers={"Authorization": f"Bearer {token}"})
    _delete = partial(requests.delete, headers={"Authorization": f"Bearer {token}"})
    return _get, _post, _put, _delete


requests.get, requests.post, requests.put, requests.delete = authorised_requests()


def capture_post(url: str, json: Union[dict, list] = {}) -> Optional[requests.Response]:
    try:
        response = requests.post(url, json=json)
    except Exception as e:
        logger.error(f"Exception encountered in post to {url}: {e}")
        response = requests.Response()
    if response.status_code != 200:
        logger.warning(
            f"Response to post to {url} with data {json} had status code "
            f"{response.status_code}. The reason given was {response.reason}"
        )
        split_url = urlparse(url)
        client_config = read_config()
        failure_url = urlunparse(
            split_url._replace(
                path=f"/instruments/{client_config['Murfey']['instrument_name']}/failed_client_post"
            )
        )
        try:
            resend_response = requests.post(
                failure_url, json={"url": url, "data": json}
            )
        except Exception as e:
            logger.error(f"Exception encountered in post to {failure_url}: {e}")
            resend_response = requests.Response()
        if resend_response.status_code != 200:
            logger.warning(
                f"Response to post to {failure_url} failed with {resend_response.reason}"
            )

    return response


def capture_get(url: str) -> Optional[requests.Response]:
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


def capture_delete(url: str) -> Optional[requests.Response]:
    try:
        response = requests.delete(url)
    except Exception as e:
        logger.error(f"Exception encountered in delete of {url}: {e}")
        response = None
    if response and response.status_code != 200:
        logger.warning(
            f"Response to delete of {url} had status code {response.status_code}. "
            f"The reason given was {response.reason}"
        )
    return response


def set_default_acquisition_output(
    new_output_dir: Path,
    software_settings_output_directories: dict[str, list[str]],
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

        def _set(d: dict, keys_list: list[str], value: str) -> dict:
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
