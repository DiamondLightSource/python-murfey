from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, computed_field, field_validator

"""
=======================================================================================
General Models
=======================================================================================
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

    @field_validator("size", mode="before")
    @classmethod
    def round_file_size_correctly(cls, v: Any) -> int:
        if isinstance(v, float):
            if v - math.floor(v) == 0.5:
                return math.ceil(v)
            return round(v)
        return v


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


class RsyncerSkippedFiles(BaseModel):
    source: str
    session_id: int
    increment_count: int = 1


class UpstreamFileRequestInfo(BaseModel):
    # Used in backend server for cross-instrument file download requests
    upstream_instrument: str
    upstream_visit_path: Path


"""
=======================================================================================
FIB
=======================================================================================
Models related to the FIB workflow.
"""


class StagePositionValues(BaseModel):
    # Coordinates are in metres
    x: float | None = None
    y: float | None = None
    z: float | None = None
    # Angles are in degrees
    rotation: float | None = None
    tilt_alpha: float | None = None

    @computed_field
    def slot_number(self) -> int | None:
        if self.x is None:
            return None
        return 1 if self.x < 0 else 2


class StagePositionInfo(BaseModel):
    """
    Stage position values associated with the different stages of the milling
    process. The XML paths they're associated with (with "Site" as the parent
    node) are indicated in the comments.

    The image acquisition steps have a "SiteLocationType" field that appear to
    be associated with either "ChunkSiteLocation" or "ThinningSiteLocation".
    "ThinningStagePosition" appears to be a duplicate of "ThinningSiteLocation"
    so far, and it is unclear for now what stages "PreparationSiteLocation" and
    "ChunkCoincidenceStagePosition" currently correspond to.
    """

    # Top-level values
    preparation_site: StagePositionValues | None = (
        None  # PreparationSiteLocation/StagePosition/StagePosition
    )
    chunk_site: StagePositionValues | None = (
        None  # ChunkSiteLocation/StagePosition/StagePosition
    )
    thinning_site: StagePositionValues | None = (
        None  # ThinningSiteLocation/StagePosition/StagePosition
    )
    # Stored under Parameters
    chunk_coincidence_params: StagePositionValues | None = (
        None  # Parameters/ChunkCoincidenceStagePosition/StagePosition
    )
    thinning_params: StagePositionValues | None = (
        None  # Parameters/ThinningStagePosition/StagePosition
    )


class MillingStepInfo(BaseModel):
    """
    These are the parameters configured per milling step that we are interested
    in tracking. Some attributes are present only for certain steps.
    """

    # Step setup
    step_name: str | None = None
    recipe_name: str | None = None
    is_enabled: bool | None = None
    status: str | None = None
    execution_time: float | None = None

    # Associated stage position information
    site_location_type: str | None = None

    # Beam info
    beam_type: str | None = None
    voltage: float | None = None
    current: float | None = None

    # Milling info
    milling_angle: float | None = None
    depth_correction: float | None = None
    lamella_offset: float | None = None
    trench_height_front: float | None = None
    trench_height_rear: float | None = None
    width_overlap_front_left: float | None = None
    width_overlap_front_right: float | None = None
    width_overlap_rear_left: float | None = None
    width_overlap_rear_right: float | None = None


class MillingSteps(BaseModel):
    # Processing steps supported by AutoTEM
    # Preparation stage
    eucentric_tilt: MillingStepInfo | None = None
    artificial_features: MillingStepInfo | None = None
    milling_angle: MillingStepInfo | None = None
    image_acquisition: MillingStepInfo | None = None
    lamella_placement: MillingStepInfo | None = None
    # Milling stage
    delay_1: MillingStepInfo | None = None
    reference_definition: MillingStepInfo | None = None
    reference_definition_electron: MillingStepInfo | None = None
    stress_relief_cuts: MillingStepInfo | None = None
    reference_redefinition_1: MillingStepInfo | None = None
    rough_milling: MillingStepInfo | None = None
    rough_milling_electron: MillingStepInfo | None = None
    reference_redefinition_2: MillingStepInfo | None = None
    medium_milling: MillingStepInfo | None = None
    medium_milling_electron: MillingStepInfo | None = None
    fine_milling: MillingStepInfo | None = None
    fine_milling_electron: MillingStepInfo | None = None
    finer_milling: MillingStepInfo | None = None
    finer_milling_electron: MillingStepInfo | None = None
    # Thinning stage
    delay_2: MillingStepInfo | None = None
    polishing_1: MillingStepInfo | None = None
    polishing_1_electron: MillingStepInfo | None = None
    polishing_2: MillingStepInfo | None = None
    polishing_2_ion: MillingStepInfo | None = None
    polishing_2_electron: MillingStepInfo | None = None


class LamellaSiteInfo(BaseModel):
    """
    Pydantic model that stores all the metadata of interest for a single lamella
    site.
    """

    # Values not associated with a single step
    project_name: str | None = None
    site_name: str | None = None
    site_number: int | None = None
    stage_info: StagePositionInfo | None = None
    steps: MillingSteps | None = None


"""
=======================================================================================
Single Particle Analysis
=======================================================================================
Models related to the single-particle analysis workflow.
"""


class ProcessingParametersSPA(BaseModel):
    tag: str
    dose_per_frame: Optional[float] = None
    gain_ref: Optional[str] = None
    experiment_type: str
    voltage: float
    image_size_x: int
    image_size_y: int
    pixel_size_on_image: str
    motion_corr_binning: int
    file_extension: str
    acquisition_software: str
    symmetry: str
    eer_fractionation_file: str = ""
    magnification: Optional[int] = None
    total_exposed_dose: Optional[float] = None
    c2aperture: Optional[float] = None
    exposure_time: Optional[float] = None
    slit_width: Optional[float] = None
    phase_plate: bool = False

    class Base(BaseModel):
        dose_per_frame: Optional[float] = None
        gain_ref: Optional[str] = None
        symmetry: str
        eer_fractionation: int


class GridSquareParameters(BaseModel):
    tag: str
    image: str = ""

    # Actual coordinates for image centre in real space
    x_location: Optional[float] = None
    y_location: Optional[float] = None

    # Coordinates for image centre when overlaid on atlas (in pixels)
    x_location_scaled: Optional[int] = None
    y_location_scaled: Optional[int] = None

    x_stage_position: Optional[float] = None
    y_stage_position: Optional[float] = None

    # Size of original image (in pixels)
    readout_area_x: Optional[int] = None
    readout_area_y: Optional[int] = None

    # Size of thumbnail used (in pixels)
    thumbnail_size_x: Optional[int] = None
    thumbnail_size_y: Optional[int] = None

    height: Optional[int] = None
    width: Optional[int] = None

    # Size of image when overlaid on atlas (in pixels)
    height_scaled: Optional[int] = None
    width_scaled: Optional[int] = None

    pixel_size: Optional[float] = None
    angle: Optional[float] = None

    # Collection mode
    collection_mode: Optional[str] = None


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


class SearchMapParameters(BaseModel):
    tag: str
    x_location: float | None = None
    y_location: float | None = None
    x_stage_position: float | None = None
    y_stage_position: float | None = None
    pixel_size: float | None = None
    image: str | None = None
    binning: float | None = None
    reference_matrix: Dict[str, float] = {}
    stage_correction: Dict[str, float] = {}
    image_shift_correction: Dict[str, float] = {}
    height: int | None = None
    width: int | None = None
    height_on_atlas: int | None = None
    width_on_atlas: int | None = None
    lamella: bool | None = None


class BatchPositionParameters(BaseModel):
    tag: str
    x_stage_position: float
    y_stage_position: float
    x_beamshift: float
    y_beamshift: float
    search_map_name: str


class MultigridWatcherSetup(BaseModel):
    source: Path
    destination_overrides: Dict[Path, str] = {}
    rsync_restarts: List[str] = []
    serialem: bool = False


class Token(BaseModel):
    access_token: str
    token_type: str


"""
=======================================================================================
Tomography
=======================================================================================
Models related to the tomographic reconstruction workflow.
"""


class ProcessingParametersTomo(BaseModel):
    dose_per_frame: Optional[float] = None
    frame_count: int
    tilt_axis: float
    gain_ref: Optional[str] = None
    experiment_type: str
    voltage: float
    image_size_x: int
    image_size_y: int
    pixel_size_on_image: str
    motion_corr_binning: int
    file_extension: str
    tag: str
    tilt_series_tag: str
    eer_fractionation_file: Optional[str] = None
    eer_fractionation: int

    class Base(BaseModel):
        dose_per_frame: Optional[float] = None
        gain_ref: Optional[str] = None
        eer_fractionation: int


class CompletedTiltSeries(BaseModel):
    tilt_series: List[str]
    rsync_source: str
