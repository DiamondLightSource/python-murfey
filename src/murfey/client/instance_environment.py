from __future__ import annotations

import json
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


class MurfeyInstanceEnvironmentBase(BaseModel):
    software_versions: Dict[str, str] = {}
    source: Optional[Path] = None
    default_destination: str = ""
    demo: bool = False
    data_collection_group_id: Optional[int] = None
    data_collection_ids: Dict[str, int] = {}
    processing_job_ids: Dict[str, Dict[str, int]] = {}
    autoproc_program_ids: Dict[str, Dict[str, int]] = {}
    data_collection_parameters: dict = {}
    movies: Dict[Path, MovieTracker] = {}
    motion_corrected_movies: Dict[Path, List[str]] = {}
    movie_tilt_pair: Dict[Path, str] = {}
    tilt_angles: Dict[str, List[List[str]]] = {}
    visit: str = ""
    processing_only_mode: bool = False
    tilt_offset: Optional[float] = None
    gain_ref: Optional[Path] = None
    cache_path: Optional[Path] = None

    @classmethod
    def _cache_from_dict(cls, out_path: Path, data: dict):
        if not data.get("source"):
            return
        if out_path.is_file():
            with open(out_path, "r") as env_cache:
                current_cache = json.load(env_cache)
        else:
            current_cache = {}
        print(out_path)
        with open(out_path, "w") as env_cache:
            as_dict = {}
            for k in MurfeyInstanceEnvironmentBase.__fields__.keys():
                v = data.get(k)
                as_dict[k] = str(v) if isinstance(v, Path) else v
            current_cache.update({str(data.get("source")): as_dict})
            json.dump(current_cache, env_cache)

    def write(self, out_path: Path | None = None):
        self.cache_path = out_path or Path.home() / ".murfey_cache.json"
        self._cache_from_dict(self.cache_path, self.dict())

    def clear_from_cache(self):
        cache_path = self.cache_path or Path.home() / ".murfey_cache.json"
        if cache_path.is_file():
            with open(cache_path, "r") as env_cache:
                current_cache = json.load(env_cache)
            current_cache.pop(self.source)
            with open(cache_path, "w") as env_cache:
                json.dump(current_cache, env_cache)


class MurfeyInstanceEnvironment(MurfeyInstanceEnvironmentBase):
    url: ParseResult
    watcher: Optional[DirWatcher] = None
    listeners: Dict[str, Set[Callable]] = {}

    class Config:
        validate_assignment: bool = True
        arbitrary_types_allowed: bool = True

    @classmethod
    def read(
        cls, url: ParseResult, source: Path, in_path: Path | None = None, **kwargs
    ):
        with open(in_path or Path.home() / ".murfey_cache.json", "r") as env_cache:
            # only validate with the MurfeyInstanceEnvironmentBase validators to avoid calls to the callbacks below at init
            validated_read = MurfeyInstanceEnvironmentBase(
                **json.load(env_cache).get(str(source))
            )
            inst = cls.construct(url=url, **validated_read.dict(), **kwargs)
        return inst

    @validator("*")
    def cache(cls, v, values):
        if values.get("cache_path"):
            cls._cache_from_dict(values["cache_path"], values)
        return v

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
        if values.get("visit"):
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
                                values["tilt_offset"],
                            )
                        except KeyError:
                            pass
                        except Exception as e:
                            logger.warning(f"ERROR {e}")
        return v
