from __future__ import annotations

import logging
from itertools import count
from pathlib import Path
from typing import Any, Callable, Dict, List
from urllib.parse import ParseResult

from murfey.client.watchdir import DirWatcher

logger = logging.getLogger("murfey.client.instance_environment")

MurfeyID = count(1)
MovieID = count(1)


class MurfeyInstanceEnvironment:
    def __init__(
        self,
        murfey_url: ParseResult,
        source: Path | None = None,
        default_destination: str = "",
        watcher: DirWatcher | None = None,
        demo: bool = False,
        data_collection_group_id: int | None = None,
        visit: str = "",
    ):
        self.murfey_url = murfey_url
        self.source = source
        self.default_destination = default_destination
        self.watcher = watcher
        self.demo = demo
        self.data_collection_group_id = data_collection_group_id
        self.visit = visit
        self._listeners: List[Callable] = []
        self._dcg_listeners: List[Callable] = []
        self._dc_listeners: List[Callable] = []
        self._autoproc_programs: Dict[str, int] = {}
        self._processing_jobs: Dict[str, int] = {}
        self._data_collections: Dict[str, int] = {}
        self._data_collection_parameters: Dict[str, Any] = {}
        self.movie_ids: Dict[Path, int] = {}
        self.motion_correction_ids: Dict[Path, int] = {}
        self.movie_numbers: Dict[Path, int] = {}

    def subscribe(self, callback: Callable):
        self._listeners.append(callback)

    def subscribe_dcg(self, callback: Callable):
        self._dcg_listeners.append(callback)

    def subscribe_dc(self, callback: Callable):
        self._dc_listeners.append(callback)

    def new_processing_id(self, pid: int, tag: str):
        self._processing_jobs[tag] = pid
        for l in self._listeners:
            l(tag)

    def register_dcg(self, dcg_id: int):
        self.data_collection_group_id = dcg_id
        for l in self._dcg_listeners:
            l()

    def register_dc(self, dcid: int, tag: str):
        if tag not in list(self._data_collections):
            self._data_collections[tag] = dcid
            for l in self._dc_listeners:
                l(tag)

    def register_app(self, appid: int, tag: str):
        if tag not in list(self._autoproc_programs):
            self._autoproc_programs[tag] = appid
            logger.debug(f"APPID registered: {tag}: {self._listeners}")
            for l in self._listeners:
                logger.debug(f"calling {l} with {tag}")
                l(tag)
                logger.debug(f"called {l} with {tag}")
