from __future__ import annotations

import argparse
import os
import pathlib

import requests

from transferscript.utils.file_monitor import Monitor
from transferscript.utils.rsync import RsyncPipe


def run():
    parser = argparse.ArgumentParser(description="Start the transferscript client")
    parser.add_argument("--visit", help="Name of visit", required=True)
    args = parser.parse_args()
    print("Visit name: ", args.visit)
    print(get_all_visits().text)
    print(get_visit_info(args.visit).text)


def get_all_visits():
    path = "http://127.0.0.1:8000/visits/" + os.getenv("BEAMLINE")
    # uvicorn default host and port, specified in uvicorn.run in server/main.py
    r = requests.get(path)
    return r


def get_visit_info(visit_name: str):
    path = (
        "http://127.0.0.1:8000/visits/"
        + (os.getenv("BEAMLINE") or "")
        + "/"
        + visit_name
    )
    # uvicorn default host and port, specified in uvicorn.run in server/main.py
    r = requests.get(path)
    return r


def watch_directory(directory: pathlib.Path) -> Monitor:
    monitor = Monitor(directory)
    monitor.monitor(in_thread=True)
    return monitor


def stop_watching(monitor: Monitor):
    monitor.stop()
    monitor.wait()


def start_transfer(monitor: Monitor, destination: pathlib.Path) -> RsyncPipe:
    rp = RsyncPipe(monitor, destination)
    rp.process(in_thread=True)
    return rp


run()
