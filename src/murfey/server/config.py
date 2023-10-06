from __future__ import annotations

import os
import socket
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, BaseSettings


class MachineConfig(BaseModel):
    acquisition_software: List[str]
    calibrations: Dict[str, Dict[str, Union[dict, float]]]
    data_directories: Dict[Path, str]
    rsync_basepath: Path
    murfey_db_credentials: str
    display_name: str = ""
    image_path: Optional[Path] = None
    software_versions: Dict[str, str] = {}
    external_executables: Dict[str, str] = {}
    external_environment: Dict[str, str] = {}
    rsync_module: str = ""
    create_directories: List[str] = ["atlas"]
    gain_reference_directory: Optional[Path] = None
    processed_directory_name: str = "processed"
    gain_directory_name: str = "processing"
    feedback_queue: str = "murfey_feedback"
    superres: bool = False
    camera: str = "FALCON"
    data_required_substrings: Dict[str, Dict[str, List[str]]] = {}
    allow_removal: bool = False
    modular_spa: bool = False
    processing_enabled: bool = True
    machine_override: str = ""
    processed_extra_directory: str = ""


def from_file(config_file_path: Path, microscope: str) -> MachineConfig:
    with open(config_file_path, "r") as config_stream:
        config = yaml.safe_load(config_stream)
    return MachineConfig(**config.get(microscope, {}))


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""


settings = Settings()


@lru_cache()
def get_hostname():
    return socket.gethostname()


@lru_cache()
def get_microscope():
    try:
        hostname = get_hostname()
        microscope_from_hostname = hostname.split(".")[0]
    except OSError:
        microscope_from_hostname = "Unknown"
    microscope_name = os.getenv("BEAMLINE", microscope_from_hostname)
    return microscope_name


@lru_cache(maxsize=1)
def get_machine_config() -> MachineConfig:
    machine_config: MachineConfig = MachineConfig(
        acquisition_software=[],
        calibrations={},
        data_directories={},
        rsync_basepath=Path("dls/tmp"),
        murfey_db_credentials="",
    )
    if settings.murfey_machine_configuration:
        microscope = get_microscope()
        machine_config = from_file(
            Path(settings.murfey_machine_configuration), microscope
        )
    return machine_config
