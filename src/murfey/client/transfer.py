from __future__ import annotations

import logging
import os
import pathlib
from typing import List, NamedTuple, Union

import requests

from murfey.util.file_monitor import Monitor
from murfey.util.rsync import RsyncPipe

log = logging.getLogger("murfey.client.transfer")


class MonitoringPipeline(NamedTuple):
    monitor: Monitor
    rsync: RsyncPipe


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
    log.info("Notifying for {visit_name=} {transferred_file=}")
    return {}
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


class _NotRSyncingPipeline(RsyncPipe):
    def _run_rsync(
        self,
        root: pathlib.Path,
        files: List[pathlib.Path],
        retry: bool = True,
    ):
        log.info(f"Would sync {len(files)} elements")
        for file in files:
            log.debug(f"- {file} ({file.stat().st_size} bytes)")


def setup_rsync(
    visit_name: str, directory: pathlib.Path, destination: pathlib.Path
) -> MonitoringPipeline:
    monitor = Monitor(directory)
    monitor.process(in_thread=True)

    def _notify(transferred_file: pathlib.Path) -> dict:
        request_json = notify_file(visit_name, transferred_file)
        return request_json

    # rp = RsyncPipe(destination, notify=_notify)
    rp = _NotRSyncingPipeline(destination, notify=_notify)

    monitor >> rp
    rp.process(in_thread=True)
    return MonitoringPipeline(monitor, rp)


def stop_rsync(mpipeline: MonitoringPipeline):
    mpipeline.monitor.stop()
    mpipeline.monitor.wait()
    mpipeline.rsync.wait()


def just_watch_files(visit_name: str, monitor: Monitor):
    def _notify(transferred_file: pathlib.Path) -> dict:
        request_json = notify_file(visit_name, transferred_file)
        return request_json

    if monitor.thread:
        while monitor.thread.is_alive():
            files_transferred = monitor._out.get()
            for file in files_transferred:
                _notify(file)
