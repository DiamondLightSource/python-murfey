from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


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


class ContextInfo(BaseModel):
    experiment_type: str
    acquisition_software: str


class ClearanceKeys(BaseModel):
    data_collection_group: List[str]
    data_collection: List[str]
    processing_job: List[str]
    autoproc_program: List[str]


class File(BaseModel):
    name: str
    description: str
    size: int
    timestamp: float


class SPAProcessingParameters(BaseModel):
    job_id: int


class ProcessFile(BaseModel):
    path: str
    description: str
    size: int
    timestamp: float
    processing_job: int
    data_collection_id: int
    image_number: int
    mc_uuid: int
    autoproc_program_id: int
    pixel_size: float
    dose_per_frame: float
    mc_binning: int = 1
    gain_ref: Optional[str] = None
    eer_fractionation_file: Optional[str] = None


class TiltSeries(BaseModel):
    name: str
    file_tilt_list: str
    dcid: int
    processing_job: int
    autoproc_program_id: int
    motion_corrected_path: str
    movie_id: int
    pixel_size: float
    manual_tilt_offset: int


class SuggestedPathParameters(BaseModel):
    base_path: Path
    touch: bool = False


class DCGroupParameters(BaseModel):
    experiment_type: str
    experiment_type_id: int
    tag: str


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


class DCParametersTomo(BaseModel):
    dose_per_frame: float
    gain_ref: Optional[str]
    experiment_type: str
    voltage: float
    image_size_x: int
    image_size_y: int
    pixel_size_on_image: str
    motion_corr_binning: int
    manual_tilt_offset: float
    file_extension: str
    acquisition_software: str

    class Base(BaseModel):
        dose_per_frame: float
        gain_ref: Optional[str]
        manual_tilt_offset: float


class DCParametersSPA(BaseModel):
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
    eer_grouping: int
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
        eer_grouping: int


class ProcessingJobParameters(BaseModel):
    tag: str
    recipe: str
    parameters: Dict[str, Any] = {}


class RegistrationMessage(BaseModel):
    registration: str
    params: Optional[Dict[str, Any]] = None


class ConnectionFileParameters(BaseModel):
    filename: str
    destinations: List[str]


class GainReference(BaseModel):
    gain_ref: Path
    rescale: bool = True


class MillingParameters(BaseModel):
    lamella_number: int
    images: List[str]
    raw_directory: str


class FractionationParameters(BaseModel):
    num_frames: int
    fractionation: int
    dose_per_frame: float
    fractionation_file_name: str = "eer_fractionation.txt"
