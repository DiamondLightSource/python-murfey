from __future__ import annotations

import os
import socket
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

import yaml
from backports.entry_points_selectable import entry_points
from pydantic import BaseModel, BaseSettings, Extra, validator


class MachineConfig(BaseModel, extra=Extra.allow):  # type: ignore
    acquisition_software: List[str]
    calibrations: Dict[str, Dict[str, Union[dict, float]]]
    data_directories: List[Path]
    rsync_basepath: Path
    default_model: Path
    display_name: str = ""
    instrument_name: str = ""
    image_path: Optional[Path] = None
    software_versions: Dict[str, str] = {}
    external_executables: Dict[str, str] = {}
    external_executables_eer: Dict[str, str] = {}
    external_environment: Dict[str, str] = {}
    rsync_module: str = ""
    create_directories: list[str] = ["atlas"]
    analyse_created_directories: List[str] = []
    gain_reference_directory: Optional[Path] = None
    eer_fractionation_file_template: str = ""
    processed_directory_name: str = "processed"
    gain_directory_name: str = "processing"
    node_creator_queue: str = "node_creator"
    superres: bool = False
    camera: str = "FALCON"
    data_required_substrings: Dict[str, Dict[str, List[str]]] = {}
    allow_removal: bool = False
    data_transfer_enabled: bool = True
    processing_enabled: bool = True
    machine_override: str = ""
    processed_extra_directory: str = ""
    plugin_packages: Dict[str, Path] = {}
    software_settings_output_directories: Dict[str, List[str]] = {}
    process_by_default: bool = True
    recipes: Dict[str, str] = {
        "em-spa-bfactor": "em-spa-bfactor",
        "em-spa-class2d": "em-spa-class2d",
        "em-spa-class3d": "em-spa-class3d",
        "em-spa-preprocess": "em-spa-preprocess",
        "em-spa-refine": "em-spa-refine",
        "em-tomo-preprocess": "em-tomo-preprocess",
        "em-tomo-align": "em-tomo-align",
    }

    # Find and download upstream directories
    upstream_data_directories: List[Path] = []  # Previous sessions
    upstream_data_download_directory: Optional[Path] = None  # Set by microscope config
    upstream_data_tiff_locations: List[str] = ["processed"]  # Location of CLEM TIFFs

    model_search_directory: str = "processing"
    initial_model_search_directory: str = "processing/initial_model"

    failure_queue: str = ""
    instrument_server_url: str = "http://localhost:8001"
    frontend_url: str = "http://localhost:3000"
    murfey_url: str = "http://localhost:8000"
    rsync_url: str = ""

    security_configuration_path: Optional[Path] = None
    auth_url: str = ""

    notifications_queue: str = "pato_notification"


def from_file(config_file_path: Path, instrument: str = "") -> Dict[str, MachineConfig]:
    with open(config_file_path, "r") as config_stream:
        config = yaml.safe_load(config_stream)
    return {
        i: MachineConfig(**config[i])
        for i in config.keys()
        if not instrument or i == instrument
    }


class Security(BaseModel):
    murfey_db_credentials: Path
    crypto_key: str
    auth_key: str = ""
    auth_algorithm: str = ""
    sqlalchemy_pooling: bool = True
    allow_origins: List[str] = ["*"]
    session_validation: str = ""
    session_token_timeout: Optional[int] = None
    auth_type: Literal["password", "cookie"] = "password"
    cookie_key: str = ""
    rabbitmq_credentials: Path
    feedback_queue: str = "murfey_feedback"
    graylog_host: str = ""
    graylog_port: Optional[int] = None
    ispyb_credentials: Optional[Path] = None

    @validator("graylog_port")
    def check_port_present_if_host_is(
        cls, v: Optional[int], values: dict, **kwargs
    ) -> Optional[int]:
        if values["graylog_host"] and v is None:
            raise ValueError("The Graylog port must be set if the Graylog host is")
        return v


def security_from_file(config_file_path: Path) -> Security:
    with open(config_file_path, "r") as config_stream:
        config = yaml.safe_load(config_stream)
    return Security(**config)


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""
    murfey_security_configuration: str = ""


settings = Settings()


@lru_cache()
def get_hostname():
    return socket.gethostname()


def get_microscope(machine_config: MachineConfig | None = None) -> str:
    if machine_config:
        microscope_name = machine_config.machine_override or os.getenv("BEAMLINE", "")
    else:
        microscope_name = os.getenv("BEAMLINE", "")
    return microscope_name


@lru_cache(maxsize=1)
def get_security_config() -> Security:
    if settings.murfey_security_configuration:
        return security_from_file(Path(settings.murfey_security_configuration))
    if settings.murfey_machine_configuration and os.getenv("BEAMLINE"):
        machine_config = get_machine_config(instrument_name=os.getenv("BEAMLINE"))[
            os.getenv("BEAMLINE", "")
        ]
        if machine_config.security_configuration_path:
            return security_from_file(machine_config.security_configuration_path)
    return Security(
        rabbitmq_credentials="",
        session_validation="",
        murfey_db_credentials="",
        crypto_key="",
        auth_key="",
        auth_algorithm="",
        sqlalchemy_pooling=True,
    )


@lru_cache(maxsize=1)
def get_machine_config(instrument_name: str = "") -> Dict[str, MachineConfig]:
    machine_config = {
        "": MachineConfig(
            acquisition_software=[],
            calibrations={},
            data_directories=[],
            rsync_basepath=Path("dls/tmp"),
            murfey_db_credentials="",
            default_model="/tmp/weights.h5",
        )
    }
    if settings.murfey_machine_configuration:
        microscope = instrument_name
        machine_config = from_file(
            Path(settings.murfey_machine_configuration), microscope
        )
    return machine_config


def get_extended_machine_config(
    extension_name: str, instrument_name: str = ""
) -> Optional[BaseModel]:
    machine_config = get_machine_config(instrument_name=instrument_name).get(
        instrument_name or get_microscope()
    )
    if not machine_config:
        return None
    model = entry_points().select(group="murfey.config", name=extension_name)[0].load()
    data = getattr(machine_config, extension_name, {})
    return model(**data)
