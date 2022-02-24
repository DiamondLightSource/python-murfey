from __future__ import annotations

import os
import pathlib
import random
from typing import List, NamedTuple, Union

import requests
import websocket
from websocket import create_connection

from murfey.utils.file_monitor import Monitor
from murfey.utils.rsync import RsyncPipe


class MonitoringPipeline(NamedTuple):
    monitor: Monitor
    rsync: RsyncPipe


def open_websocket_connection():
    id = str(random.randint(0, 100))
    url = "ws://127.0.0.1:8000/ws/test/" + id
    ws = create_connection(url)
    print(ws.connected)
    print(f"Websocket connection opened for Client {id}")
    return ws


def receive_messages(ws):
    while True:
        result = ws.recv()
        print("Received ", result)
    # Do other stuff with the received message


def close_websocket_connection(ws):
    print("Closing websocket connection")
    ws.close()


def on_message(message):
    print(message)


def on_error(ws, error):
    print(error.text)


def on_close(ws):
    print("Closing connection")
    ws.close()
    print("### closed ###")


def on_open():
    print("Opened connection")


def websocket_app():
    websocket.enableTrace(True)
    id = str(random.randint(0, 1000))
    url = "ws://127.0.0.1:8000/ws/test/" + id
    ws = websocket.WebSocketApp(url, on_close=on_close)
    ws.run_forever()


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
        path = "http://127.0.0.1:8000/visits/" + visit_name + "/files"
    else:
        raise RuntimeError("No BEAMLINE environment variable was specified")
    request_body = {
        "name": str(transferred_file),
        "description": f"Transferred file from visit {visit_name}",
        "size": transferred_file.stat().st_size,
        "timestamp": transferred_file.stat().st_mtime,
    }
    r = requests.post(path, json=request_body)
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


def stop_rsync(mpipeline: MonitoringPipeline):
    mpipeline.monitor.stop()
    mpipeline.monitor.wait()
    mpipeline.rsync.wait()
