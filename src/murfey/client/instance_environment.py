from __future__ import annotations

import logging
from itertools import count
from pathlib import Path
from threading import RLock
from typing import Callable, Dict, List, NamedTuple, Optional, Set
from urllib.parse import ParseResult

from pydantic import BaseModel, validator

from murfey.client.watchdir import DirWatcher

logger = logging.getLogger("murfey.client.instance_environment")

MurfeyID = count(1)
MovieID = count(1)


class MovieTracker(NamedTuple):
    movie_number: int
    motion_correction_uuid: int


global_env_lock = RLock()


class MurfeyInstanceEnvironment(BaseModel):
    url: ParseResult
    software_versions: Dict[str, str] = {}
    source: Optional[Path] = None
    default_destination: str = ""
    watcher: Optional[DirWatcher] = None
    demo: bool = False
    data_collection_group_id: Optional[int] = None
    data_collection_ids: Dict[str, int] = {}
    processing_job_ids: Dict[str, Dict[str, int]] = {}
    autoproc_program_ids: Dict[str, Dict[str, int]] = {}
    data_collection_parameters: dict = {}
    movies: Dict[Path, MovieTracker] = {}
    motion_corrected_movies: Dict[Path, List[str]] = {}
    listeners: Dict[str, Set[Callable]] = {}
    movie_tilt_pair: Dict[Path, str] = {}
    tilt_angles: Dict[str, List[List[str]]] = {}
    visit: str = ""
    processing_only_mode: bool = False
    gain_ref: Optional[Path] = None

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
        # logger.info(f"autoproc program ids validator: {v}")
        with global_env_lock:
            for l in values.get("listeners", {}).get("autoproc_program_ids", []):
                if values.get("autoproc_program_ids"):
                    for k in set(values["autoproc_program_ids"].keys()) ^ set(v.keys()):
                        if v[k].get("em-tomo-preprocess"):
                            l(k, v[k]["em-tomo-preprocess"])
                else:
                    for k in v.keys():
                        if v[k].get("em-tomo-preprocess"):
                            l(k, v[k]["em-tomo-preprocess"])
        return v

    @validator("motion_corrected_movies")
    def motion_corrected_callback(cls, v, values):
        # logger.info("motion corrected callback")
        _url = f"{str(values['url'].geturl())}/visits/{values['visit']}/align"
        for l in values.get("listeners", {}).get("motion_corrected_movies", []):
            if values.get("motion_corrected_movies"):
                for k in set(values["motion_corrected_movies"].keys()) ^ set(
                    v.keys()
                ):  # k is a path (key), v[k] is the value matching the key
                    tilt = values["movie_tilt_pair"][k]
                    file_tilt_list = []
                    for movie, angle in values["tilt_angles"][tilt]:
                        if movie in v:  # values["motion_corrected_movies"]:
                            # file_tilt_list.append(
                            #    [values["motion_corrected_movies"][movie], angle]
                            # )
                            file_tilt_list.append(
                                [
                                    str(v[Path(movie)][0]),
                                    angle,
                                    str(v[Path(movie)][1]),
                                ]
                            )
                    l(
                        k,
                        v[k][0],
                        _url,
                        values["processing_job_ids"][k]["em-tomo-align"],
                        values["autoproc_program_ids"][k]["em-tomo-align"],
                        v[k][1],
                        file_tilt_list,
                    )
            else:
                for k in v.keys():
                    try:
                        # possible race condition here where values accessing by [k] sometimes aren't ready when we
                        # try to access them - it throws a key error for a value which has just been set.
                        tilt = values["movie_tilt_pair"][k]
                        file_tilt_list = []
                        for movie, angle in values["tilt_angles"][tilt]:
                            # file_tilt_list.append([str(movie), angle])
                            file_tilt_list.append(
                                [
                                    str(v[Path(movie)][0]),
                                    angle,
                                    str(v[Path(movie)][1]),
                                ]
                            )  # or v(k)
                        l(
                            k,
                            v[k][0],
                            _url,
                            values["data_collection_ids"][tilt],
                            values["processing_job_ids"][tilt]["em-tomo-align"],
                            values["autoproc_program_ids"][tilt]["em-tomo-align"],
                            v[k][1],
                            file_tilt_list,
                        )
                    except KeyError:
                        pass
                    except Exception as e:
                        logger.warning(f"ERROR {e}")
        return v
