from __future__ import annotations

import logging
from itertools import count
from pathlib import Path
from typing import Callable, Dict, NamedTuple, Set
from urllib.parse import ParseResult

from pydantic import BaseModel, validator

from murfey.client.watchdir import DirWatcher

logger = logging.getLogger("murfey.client.instance_environment")

MurfeyID = count(1)
MovieID = count(1)


class MovieTracker(NamedTuple):
    movie_number: int
    movie_uuid: int
    motion_correction_uuid: int


class MurfeyInstanceEnvironment(BaseModel):
    url: ParseResult
    source: Path | None = None
    default_destination: str = ""
    watcher: DirWatcher | None = None
    demo: bool = False
    data_collection_group_id: int | None = None
    data_collection_ids: Dict[str, int] = {}
    processing_job_ids: Dict[str, int] = {}
    autoproc_program_ids: Dict[str, int] = {}
    movies: Dict[Path, MovieTracker] = {}
    listeners: Dict[str, Set[Callable]] = {}
    visit: str = ""

    class Config:
        validate_assignment: bool = True
        arbitrary_types_allowed: bool = True

    @validator("data_collection_group_id")
    def dcg_callback(cls, v, values):
        for l in values.get("listeners", {}).get("data_collection_group_id", []):
            l()

    @validator("data_collection_ids")
    def dc_callback(cls, v, values):
        for l in values.get("listeners", {}).get("data_collection_ids", []):
            if values.get("data_collection_ids"):
                for k in set(values["data_collection_ids"].keys()) ^ set(v.keys()):
                    l(k)
            else:
                for k in v.keys():
                    l(k)

    @validator("autoproc_program_ids")
    def app_callback(cls, v, values):
        for l in values.get("listeners", {}).get("autoproc_program_ids", []):
            if values.get("autoproc_program_ids"):
                for k in set(values["autoproc_program_ids"].keys()) ^ set(v.keys()):
                    l(k)
            else:
                for k in v.keys():
                    l(k)

    # def subscribe(self, callback: Callable):
    #     self._listeners.append(callback)

    # def subscribe_dcg(self, callback: Callable):
    #     self._dcg_listeners.append(callback)

    # def subscribe_dc(self, callback: Callable):
    #     self._dc_listeners.append(callback)

    # def new_processing_id(self, pid: int, tag: str):
    #     self._processing_jobs[tag] = pid
    #     for l in self._listeners:
    #         l(tag)
