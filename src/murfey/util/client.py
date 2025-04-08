"""
Utility functions used solely by the Murfey client. They help set up its
configuration, communicate with the backend server using the correct credentials,
and set default directories to work with.
"""

import configparser
import copy
import json
import logging
import os
import shutil
from functools import lru_cache, partial
from pathlib import Path
from typing import Callable, Optional, Union
from urllib.parse import ParseResult, urlparse, urlunparse

import requests

from murfey.util.models import Visit

logger = logging.getLogger("murfey.util.client")


def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    try:
        mcch = os.environ.get("MURFEY_CLIENT_CONFIG_HOME")
        murfey_client_config_home = Path(mcch) if mcch else Path.home()
        with open(murfey_client_config_home / ".murfey") as configfile:
            config.read_file(configfile)
    except FileNotFoundError:
        logger.warning(
            f"Murfey client configuration file {murfey_client_config_home / '.murfey'} not found"
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
    return requests.get(f"{url}/instruments/{_instrument_name}/machine").json()


def authorised_requests() -> tuple[Callable, Callable, Callable, Callable]:
    token = read_config()["Murfey"].get("token", "")
    _get = partial(requests.get, headers={"Authorization": f"Bearer {token}"})
    _post = partial(requests.post, headers={"Authorization": f"Bearer {token}"})
    _put = partial(requests.put, headers={"Authorization": f"Bearer {token}"})
    _delete = partial(requests.delete, headers={"Authorization": f"Bearer {token}"})
    return _get, _post, _put, _delete


requests.get, requests.post, requests.put, requests.delete = authorised_requests()


def _get_visit_list(api_base: ParseResult, instrument_name: str):
    get_visits_url = api_base._replace(
        path=f"/instruments/{instrument_name}/visits_raw"
    )
    server_reply = requests.get(get_visits_url.geturl())
    if server_reply.status_code != 200:
        raise ValueError(f"Server unreachable ({server_reply.status_code})")
    return [Visit.parse_obj(v) for v in server_reply.json()]


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
