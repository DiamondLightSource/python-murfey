from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

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


class ContextInfo(BaseModel):
    experiment_type: str
    acquisition_software: str


class File(BaseModel):
    name: str
    description: str
    size: int
    timestamp: float


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
    gain_ref: Optional[str] = None


class TiltSeries(BaseModel):
    name: str
    file_tilt_list: str
    dcid: int
    processing_job: int
    autoproc_program_id: int
    motion_corrected_path: str
    movie_id: int


class SuggestedPathParameters(BaseModel):
    base_path: Path


class DCGroupParameters(BaseModel):
    experiment_type: str
    experiment_type_id: int


class DCParameters(BaseModel):
    voltage: float
    pixel_size_on_image: str
    experiment_type: str
    image_size_x: int
    image_size_y: int
    tilt: int
    file_extension: str
    acquisition_software: str
    image_directory: str
    tag: str


class ProcessingJobParameters(BaseModel):
    tag: str
    recipe: str


class RegistrationMessage(BaseModel):
    registration: str
    params: Optional[Dict[str, Any]] = None
