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


class SuggestedPathParameters(BaseModel):
    base_path: Path
    touch: bool = False
    extra_directory: str = ""


class DCGroupParameters(BaseModel):
    # DC = Data collection
    experiment_type: str
    experiment_type_id: int
    tag: str
    atlas: str = ""
    sample: Optional[int] = None
    atlas_pixel_size: int = 0


class DCParameters(BaseModel):
    voltage: float
    pixel_size_on_image: str
    experiment_type: str
    image_size_x: int
    image_size_y: int
    file_extension: str
    acquisition_software: str
    image_directory: str
    tag: str
    source: str
    magnification: float
    total_exposed_dose: Optional[float] = None
    c2aperture: Optional[float] = None
    exposure_time: Optional[float] = None
    slit_width: Optional[float] = None
    phase_plate: bool = False
    data_collection_tag: str = ""


class ProcessingJobParameters(BaseModel):
    tag: str
    source: str
    recipe: str
    parameters: Dict[str, Any] = {}
    experiment_type: str = "spa"


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


class SessionInfo(BaseModel):
    session_id: Optional[int]
    session_name: str = ""
    rescale: bool = True


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


class GainReference(BaseModel):
    gain_ref: Path
    rescale: bool = True
    eer: bool = False
    tag: str = ""


class FractionationParameters(BaseModel):
    fractionation: int
    dose_per_frame: float
    num_frames: int = 0
    eer_path: Optional[str] = None
    fractionation_file_name: str = "eer_fractionation.txt"


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


class SPAProcessFile(BaseModel):
    tag: str
    path: str
    description: str
    processing_job: Optional[int]
    data_collection_id: Optional[int]
    image_number: int
    autoproc_program_id: Optional[int]
    foil_hole_id: Optional[int]
    pixel_size: Optional[float]
    dose_per_frame: Optional[float]
    mc_binning: Optional[int] = 1
    gain_ref: Optional[str] = None
    extract_downscale: bool = True
    eer_fractionation_file: Optional[str] = None
    source: str = ""


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


class TomoProcessFile(BaseModel):
    path: str
    description: str
    tag: str
    image_number: int
    pixel_size: float
    dose_per_frame: Optional[float]
    frame_count: int
    tilt_axis: Optional[float]
    mc_uuid: Optional[int] = None
    voltage: float = 300
    mc_binning: int = 1
    gain_ref: Optional[str] = None
    extract_downscale: int = 1
    eer_fractionation_file: Optional[str] = None
    group_tag: Optional[str] = None


class TiltInfo(BaseModel):
    tilt_series_tag: str
    movie_path: str
    source: str


class TiltSeriesInfo(BaseModel):
    session_id: int
    tag: str
    source: str


class TiltSeriesGroupInfo(BaseModel):
    tags: List[str]
    source: str
    tilt_series_lengths: List[int]


class CompletedTiltSeries(BaseModel):
    tilt_series: List[str]
    rsync_source: str


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
