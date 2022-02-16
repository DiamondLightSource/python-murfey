from __future__ import annotations

import argparse
import os
import pathlib
from typing import List, NamedTuple, Union

import requests
from websocket import create_connection

from murfey.utils.file_monitor import Monitor
from murfey.utils.rsync import RsyncPipe


class MonitoringPipeline(NamedTuple):
    monitor: Monitor
    rsync: RsyncPipe


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey client")
    parser.add_argument("--visit", help="Name of visit", required=True)
    visit_name = parser.parse_args().visit
    example_websocket_connection(visit_name)


def example_websocket_connection(visit_name):
    ws = create_connection("ws://127.0.0.1:8000/ws/test")
    post_file(visit_name)
    send_message(ws)


def send_message(ws):
    print("Sending message 1")
    ws.send("Message 1")
    result = ws.recv()
    print("Received ", result)
    ws.close()


def post_file(visit):
    url = "http://127.0.0.1:8000/visits/" + visit + "/files"
    data = {"name": "file1", "description": "8361", "size": 25, "timestamp": 24.0}
    requests.post(url, json=data)


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
