from __future__ import annotations

import logging
from itertools import count
from pathlib import Path
from threading import RLock
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


global_env_lock = RLock()


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
    motion_corrected_movies: Dict[Path, Path] = {}
    listeners: Dict[str, Set[Callable]] = {}
    movie_tilt_pair: Dict[Path, str] = {}
    visit: str = ""

    class Config:
        validate_assignment: bool = True
        arbitrary_types_allowed: bool = True

    @validator("data_collection_group_id")
    def dcg_callback(cls, v, values):
        with global_env_lock:
            for l in values.get("listeners", {}).get("data_collection_group_id", []):
                l()
        return v

    @validator("data_collection_ids")
    def dc_callback(cls, v, values):
        with global_env_lock:
            for l in values.get("listeners", {}).get("data_collection_ids", []):
                if values.get("data_collection_ids"):
                    for k in set(values["data_collection_ids"].keys()) ^ set(v.keys()):
                        l(k)
                else:
                    for k in v.keys():
                        l(k)
        return v

    @validator("autoproc_program_ids")
    def app_callback(cls, v, values):
        with global_env_lock:
            for l in values.get("listeners", {}).get("autoproc_program_ids", []):
                if values.get("autoproc_program_ids"):
                    for k in set(values["autoproc_program_ids"].keys()) ^ set(v.keys()):
                        l(k)
                else:
                    for k in v.keys():
                        l(k)
        return v

    @validator("motion_corrected_movies")
    def motion_corrected_callback(cls, v, values):
        _url = f"{str(values['url'].geturl())}/visits/{values['visit']}/align"
        for l in values.get("listeners", {}).get("motion_corrected_movies", []):
            if values.get("motion_corrected_movies"):
                for k in set(values["motion_corrected_movies"].keys()) ^ set(
                    v.keys()
                ):  # k is a path (key), v[k] is the value matching the key
                    try:
                        print(values.get("processing_job_ids")[k])
                        print(set(values["autoproc_program_ids"].keys()))
                        print(values["processing_job_ids"][k])
                    except Exception as e:
                        print(e)
                    l(
                        k,
                        v[k],
                        _url,
                        values["processing_job_ids"][k],
                        values["autoproc_program_ids"][k],
                        values["movies"][k].movie_uuid,
                    )
            else:
                for k in v.keys():
                    try:
                        # logger.warn(values.get("processing_job_ids")[k])
                        # logger.warn(f"MTP: {values['movie_tilt_pair']}")
                        tilt = values["movie_tilt_pair"][k]
                        logger.warn(f"Tilt: {tilt}")
                        logger.warn(f"Values, {values['autoproc_program_ids'][tilt]}")
                        logger.warn(f"Values, {values['movies']}")
                        logger.warn(f"Values, {values['processing_job_ids'][tilt]}")
                        # logger.warn(values["processing_job_ids"][k]) # error because k (a file path) isn't a key in processing_job_ids
                    except Exception as e:
                        logger.warn(f"ERROR {e}")
                    l(
                        k,
                        v[k],
                        _url,
                        values["processing_job_ids"][tilt],
                        values["autoproc_program_ids"][tilt],
                        values["movies"][k].movie_uuid,
                    )
        return v
