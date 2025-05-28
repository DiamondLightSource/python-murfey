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


class Visit(BaseModel):
    start: datetime
    end: datetime
    session_id: int
    name: str
    beamline: str
    proposal_title: str

    def __repr__(self) -> str:
        return (
            "Visit("
            f"start='{self.start:%Y-%m-%d %H:%M}', "
            f"end='{self.end:%Y-%m-%d %H:%M}', "
            f"session_id='{self.session_id!r}',"
            f"name={self.name!r}, "
            f"beamline={self.beamline!r}, "
            f"proposal_title={self.proposal_title!r}"
            ")"
        )


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
Single Particle Analysis
========================
Models related to the single-particle analysis workflow.
"""


class ProcessingParametersSPA(BaseModel):
    tag: str
    dose_per_frame: float
    gain_ref: Optional[str]
    experiment_type: str
    voltage: float
    image_size_x: int
    image_size_y: int
    pixel_size_on_image: str
    motion_corr_binning: int
    file_extension: str
    acquisition_software: str
    use_cryolo: bool
    symmetry: str
    mask_diameter: Optional[int]
    boxsize: Optional[int]
    downscale: bool
    small_boxsize: Optional[int]
    eer_fractionation_file: str = ""
    particle_diameter: Optional[float]
    magnification: Optional[int] = None
    total_exposed_dose: Optional[float] = None
    c2aperture: Optional[float] = None
    exposure_time: Optional[float] = None
    slit_width: Optional[float] = None
    phase_plate: bool = False

    class Base(BaseModel):
        dose_per_frame: Optional[float]
        gain_ref: Optional[str]
        use_cryolo: bool
        symmetry: str
        mask_diameter: Optional[int]
        boxsize: Optional[int]
        downscale: bool
        small_boxsize: Optional[int]
        eer_fractionation: int


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


class MultigridWatcherSetup(BaseModel):
    source: Path
    skip_existing_processing: bool = False
    destination_overrides: Dict[Path, str] = {}
    rsync_restarts: List[str] = []


class Token(BaseModel):
    access_token: str
    token_type: str


"""
Tomography
==========
Models related to the tomographic reconstruction workflow.
"""


class ProcessingParametersTomo(BaseModel):
    dose_per_frame: Optional[float]
    frame_count: int
    tilt_axis: float
    gain_ref: Optional[str]
    experiment_type: str
    voltage: float
    image_size_x: int
    image_size_y: int
    pixel_size_on_image: str
    motion_corr_binning: int
    file_extension: str
    tag: str
    tilt_series_tag: str
    eer_fractionation_file: Optional[str]
    eer_fractionation: int

    class Base(BaseModel):
        dose_per_frame: Optional[float]
        gain_ref: Optional[str]
        eer_fractionation: int


class CompletedTiltSeries(BaseModel):
    tilt_series: List[str]
    rsync_source: str
