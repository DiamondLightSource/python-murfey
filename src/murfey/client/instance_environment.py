from __future__ import annotations

import logging
from itertools import count
from pathlib import Path
from threading import RLock
from typing import Callable, Dict, List, NamedTuple, Optional, Set
from urllib.parse import ParseResult

from pydantic import BaseModel

from murfey.client.watchdir import DirWatcher
from murfey.client.websocket import WSApp

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
    tilt_offset: Optional[float] = None
    gain_ref: Optional[Path] = None
    websocket: Optional[WSApp] = None

    class Config:
        validate_assignment: bool = True
        arbitrary_types_allowed: bool = True
