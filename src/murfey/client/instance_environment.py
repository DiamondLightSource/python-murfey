from __future__ import annotations

import logging
from itertools import count
from pathlib import Path
from typing import Callable, Dict, NamedTuple, Optional, Set
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
    source: Optional[Path] = None
    default_destination: str = ""
    watcher: Optional[DirWatcher] = None
    demo: bool = False
    data_collection_group_id: Optional[int] = None
    data_collection_ids: Dict[str, int] = {}
    processing_job_ids: Dict[str, int] = {}
    autoproc_program_ids: Dict[str, int] = {}
    data_collection_parameters: dict = {}
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
