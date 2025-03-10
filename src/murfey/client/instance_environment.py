from __future__ import annotations

import itertools
import logging
from itertools import count
from pathlib import Path
from threading import RLock
from typing import Dict, List, NamedTuple, Optional
from urllib.parse import ParseResult

from pydantic import BaseModel

from murfey.client.watchdir import DirWatcher

logger = logging.getLogger("murfey.client.instance_environment")

MurfeyID = count(1)
MovieID = count(1)


class MovieTracker(NamedTuple):
    movie_number: int
    motion_correction_uuid: int


class SampleInfo(NamedTuple):
    atlas: Path
    sample: int


global_env_lock = RLock()


class MurfeyInstanceEnvironment(BaseModel):
    url: ParseResult
    client_id: int
    instrument_name: str
    software_versions: Dict[str, str] = {}
    sources: List[Path] = []
    default_destinations: Dict[Path, str] = {}
    destination_registry: Dict[str, str] = {}
    watchers: Dict[Path, DirWatcher] = {}
    demo: bool = False
    data_collection_parameters: dict = {}
    movies: Dict[Path, MovieTracker] = {}
    movie_tilt_pair: Dict[Path, str] = {}
    tilt_angles: Dict[str, List[List[str]]] = {}
    movie_counters: Dict[str, itertools.count] = {}
    visit: str = ""
    processing_only_mode: bool = False
    gain_ref: Optional[Path] = None
    superres: bool = True
    murfey_session: Optional[int] = None
    samples: Dict[Path, SampleInfo] = {}
    rsync_url: str = ""

    class Config:
        validate_assignment: bool = True
        arbitrary_types_allowed: bool = True

    def clear(self):
        self.sources = []
        self.default_destinations = {}
        for w in self.watchers.values():
            w.stop()
        self.watchers = {}
        self.data_collection_parameters = {}
        self.movies = {}
        self.movie_tilt_pair = {}
        self.tilt_angles = {}
        self.visit = ""
        self.gain_ref = None
