from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

"""
General Models
==============
Models used in multiple workflows.
"""


class RegistrationMessage(BaseModel):
    registration: str
    params: Optional[Dict[str, Any]] = None


class File(BaseModel):
    name: str
    description: str
    size: int
    timestamp: datetime
    full_path: str


class ConnectionFileParameters(BaseModel):
    filename: str
    destinations: List[str]


class ClientInfo(BaseModel):
    id: int


class RsyncerSource(BaseModel):
    source: str


class RsyncerInfo(BaseModel):
    source: str
    destination: str
    session_id: int
    transferring: bool = True
    increment_count: int = 1
    bytes: int = 0
    increment_data_count: int = 0
    data_bytes: int = 0
    tag: str = ""


"""
FIB
===
Models related to FIB, as part of correlative workflow with TEM.
"""


class Sample(BaseModel):
    sample_group_id: int
    sample_id: int
    subsample_id: int
    image_path: Optional[Path]


class BLSampleImageParameters(BaseModel):
    sample_id: int
    sample_path: Path


class BLSampleParameters(BaseModel):
    sample_group_id: int


class BLSubSampleParameters(BaseModel):
    sample_id: int
    image_path: Optional[Path] = None


class MillingParameters(BaseModel):
    lamella_number: int
    images: List[str]
    raw_directory: str


"""
Single Particle Analysis
========================
Models related to the single-particle analysis workflow.
"""


class GridSquareParameters(BaseModel):
    tag: str
    x_location: Optional[float] = None
    y_location: Optional[float] = None
    x_stage_position: Optional[float] = None
    y_stage_position: Optional[float] = None
    readout_area_x: Optional[int] = None
    readout_area_y: Optional[int] = None
    thumbnail_size_x: Optional[int] = None
    thumbnail_size_y: Optional[int] = None
    height: Optional[int] = None
    width: Optional[int] = None
    pixel_size: Optional[float] = None
    image: str = ""
    angle: Optional[float] = None


class FoilHoleParameters(BaseModel):
    tag: str
    name: int
    x_location: Optional[float] = None
    y_location: Optional[float] = None
    x_stage_position: Optional[float] = None
    y_stage_position: Optional[float] = None
    readout_area_x: Optional[int] = None
    readout_area_y: Optional[int] = None
    thumbnail_size_x: Optional[int] = None
    thumbnail_size_y: Optional[int] = None
    pixel_size: Optional[float] = None
    image: str = ""
    diameter: Optional[float] = None


class PostInfo(BaseModel):
    url: str
    data: dict


class MultigridWatcherSetup(BaseModel):
    source: Path
    skip_existing_processing: bool = False
    destination_overrides: Dict[Path, str] = {}
    rsync_restarts: List[str] = []


class CurrentGainRef(BaseModel):
    path: str


class Token(BaseModel):
    access_token: str
    token_type: str


"""
Tomography
==========
Models related to the tomographic reconstruction workflow.
"""


class CompletedTiltSeries(BaseModel):
    tilt_series: List[str]
    rsync_source: str
