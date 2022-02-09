from __future__ import annotations

import argparse
import os
import pathlib
from typing import List, NamedTuple, Union

import requests

from murfey.utils.file_monitor import Monitor
from murfey.utils.rsync import RsyncPipe


class MonitoringPipeline(NamedTuple):
    monitor: Monitor
    rsync: RsyncPipe


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey client")
    parser.add_argument("--visit", help="Name of visit", required=True)
    # args = parser.parse_args()
    # print("Visit name: ", args.visit)
    # print(get_all_visits().text)
    # print(get_visit_info(args.visit).text)


def get_all_visits() -> Union[dict, List[dict]]:
    bl = os.getenv("BEAMLINE")
    if bl:
        path = "http://127.0.0.1:8000/visits/" + bl
    else:
        raise RuntimeError("No BEAMLINE environment variable was specified")
    # uvicorn default host and port, specified in uvicorn.run in server/main.py
    r = requests.get(path)
    return r.json()


def get_visit_info(visit_name: str) -> Union[dict, List[dict]]:
    bl = os.getenv("BEAMLINE")
    if bl:
        path = "http://127.0.0.1:8000/visits/" + visit_name
    else:
        raise RuntimeError("No BEAMLINE environment variable was specified")
    # uvicorn default host and port, specified in uvicorn.run in server/main.py
    r = requests.get(path)
    return r.json()


def notify_file(visit_name: str, transferred_file: pathlib.Path) -> dict:
    bl = os.getenv("BEAMLINE")
    if bl:
        path = "http://127.0.0.1:8000/visits/" + bl + "/" + visit_name + "/files"
    else:
        raise RuntimeError("No BEAMLINE environment variable was specified")
    request_body = {
        "name": str(transferred_file),
        "description": f"Transferred file from visit {visit_name}",
        "size": transferred_file.stat().st_size,
        "timestamp": transferred_file.stat().st_mtime,
    }
    r = requests.post(path, data=request_body)
    return r.json()


def setup_rsync(
    visit_name: str, directory: pathlib.Path, destination: pathlib.Path
) -> MonitoringPipeline:
    monitor = Monitor(directory)
    monitor.process(in_thread=True)

    def _notify(transferred_file: pathlib.Path) -> dict:
        request_json = notify_file(visit_name, transferred_file)
        return request_json

    rp = RsyncPipe(destination, notify=_notify)
    monitor >> rp
    rp.process(in_thread=True)
    return MonitoringPipeline(monitor, rp)
