from __future__ import annotations

import logging
from itertools import count
from pathlib import Path
from threading import RLock
from typing import Callable, Dict, List, NamedTuple, Optional, Set
from urllib.parse import ParseResult

from pydantic import BaseModel

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
    client_id: int
    software_versions: Dict[str, str] = {}
    sources: List[Path] = []
    default_destinations: Dict[Path, str] = {}
    destination_registry: Dict[str, str] = {}
    watchers: Dict[Path, DirWatcher] = {}
    demo: bool = False
    data_collection_group_ids: Dict[str, int] = {}
    data_collection_ids: Dict[str, int] = {}
    processing_job_ids: Dict[str, Dict[str, int]] = {}
    autoproc_program_ids: Dict[str, Dict[str, int]] = {}
    id_tag_registry: Dict[str, List[str]] = {
        "data_collection_group": [],
        "data_collection": [],
        "processing_job": [],
        "auto_proc_program": [],
    }
    data_collection_parameters: dict = {}
    movies: Dict[Path, MovieTracker] = {}
    motion_corrected_movies: Dict[Path, List[str]] = {}
    listeners: Dict[str, Set[Callable]] = {}
    movie_tilt_pair: Dict[Path, str] = {}
    tilt_angles: Dict[str, List[List[str]]] = {}
    visit: str = ""
    processing_only_mode: bool = False
    gain_ref: Optional[Path] = None
    superres: bool = True
    murfey_session: Optional[int] = None

    class Config:
        validate_assignment: bool = True
        arbitrary_types_allowed: bool = True

    def clear(self):
        self.sources = []
        self.default_destinations = {}
        for w in self.watchers.values():
            w.stop()
        self.watchers = {}
        self.data_collection_group_ids = {}
        self.data_collection_ids = {}
        self.processing_job_ids = {}
        self.autoproc_program_ids = {}
        self.data_collection_parameters = {}
        self.movies = {}
        self.motion_corrected_movies = {}
        self.listeners = {}
        self.movie_tilt_pair = {}
        self.tilt_angles = {}
        self.visit = ""
        self.gain_ref = None
