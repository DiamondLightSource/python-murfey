from __future__ import annotations

import os
import socket
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
from backports.entry_points_selectable import entry_points
from pydantic import BaseModel, ConfigDict, RootModel, ValidationInfo, field_validator
from pydantic_settings import BaseSettings


class MagnificationTable(RootModel[dict[int, float]]):
    pass


CALIBRATIONS_VALIDATION_SCHEMAS = {
    "magnification": MagnificationTable,
}


class MachineConfig(BaseModel):  # type: ignore
    """
    Keys that describe the type of workflow conducted on the client side, and how
    Murfey will handle its data transfer and processing
    """

    # General info --------------------------------------------------------------------
    display_name: str = ""
    instrument_name: str = ""
    image_path: Optional[Path] = None
    machine_override: str = ""

    # Hardware and software -----------------------------------------------------------
    camera: str = "FALCON"
    superres: bool = False
    calibrations: dict[str, Any]
    acquisition_software: list[str]
    software_versions: dict[str, str] = {}
    software_settings_output_directories: dict[str, list[str]] = {}
    data_required_substrings: dict[str, dict[str, list[str]]] = {}

    # Client side directory setup -----------------------------------------------------
    data_directories: list[Path]
    create_directories: list[str] = ["atlas"]
    analyse_created_directories: list[str] = []
    gain_reference_directory: Optional[Path] = None
    eer_fractionation_file_template: str = ""

    # Data transfer setup -------------------------------------------------------------
    # Rsync setup
    data_transfer_enabled: bool = True
    rsync_url: str = ""
    rsync_module: str = ""
    rsync_basepath: Path
    allow_removal: bool = False

    # Upstream data download setup
    upstream_data_directories: list[Path] = []  # Previous sessions
    upstream_data_download_directory: Optional[Path] = None  # Set by microscope config
    upstream_data_tiff_locations: list[str] = ["processed"]  # Location of CLEM TIFFs

    # Data processing setup -----------------------------------------------------------
    # General processing setup
    processing_enabled: bool = True
    process_by_default: bool = True
    gain_directory_name: str = "processing"
    process_multiple_datasets: bool = True
    processed_directory_name: str = "processed"
    processed_extra_directory: str = ""
    recipes: dict[str, str] = {
        "em-spa-bfactor": "em-spa-bfactor",
        "em-spa-class2d": "em-spa-class2d",
        "em-spa-class3d": "em-spa-class3d",
        "em-spa-preprocess": "em-spa-preprocess",
        "em-spa-refine": "em-spa-refine",
        "em-tomo-preprocess": "em-tomo-preprocess",
        "em-tomo-align": "em-tomo-align",
    }

    # Particle picking setup
    default_model: Path
    picking_model_search_directory: str = "processing"
    initial_model_search_directory: str = "processing/initial_model"

    # Data analysis plugins
    external_executables: dict[str, str] = {}
    external_executables_eer: dict[str, str] = {}
    external_environment: dict[str, str] = {}
    plugin_packages: dict[str, Path] = {}

    # Server and network setup --------------------------------------------------------
    # Configurations and URLs
    security_configuration_path: Optional[Path] = None
    murfey_url: str = "http://localhost:8000"
    frontend_url: str = "http://localhost:3000"
    instrument_server_url: str = "http://localhost:8001"

    # Messaging queues
    failure_queue: str = ""
    node_creator_queue: str = "node_creator"
    notifications_queue: str = "pato_notification"

    # Pydantic BaseModel settings
    model_config = ConfigDict(extra="allow")

    @field_validator("calibrations", mode="before")
    @classmethod
    def validate_calibration_data(
        cls, v: dict[str, dict[Any, Any]]
    ) -> dict[str, dict[Any, Any]]:
        # Pass the calibration dictionaries through their matching Pydantic models, if any are set
        if isinstance(v, dict):
            validated = {}
            for (
                key,
                value,
            ) in v.items():
                model_cls = CALIBRATIONS_VALIDATION_SCHEMAS.get(key)
                if model_cls:
                    try:
                        # Validate and store as a dict object with the corrected types
                        validated[key] = model_cls.model_validate(value).root
                    except Exception as e:
                        raise ValueError(f"Validation failed for key '{key}': {e}")
                else:
                    validated[key] = value
            return validated
        # Let it validate and fail as-is
        return v

    @field_validator("software_versions", mode="before")
    @classmethod
    def validate_software_versions(cls, v: dict[str, Any]) -> dict[str, str]:
        # Software versions should be numerical strings, even if they appear int- or float-like
        if isinstance(v, dict):
            validated = {key: str(value) for key, value in v.items()}
            return validated
        # Let it validate and fail as-is
        return v


def from_file(config_file_path: Path, instrument: str = "") -> dict[str, MachineConfig]:
    with open(config_file_path, "r") as config_stream:
        config = yaml.safe_load(config_stream)
    return {
        i: MachineConfig(**config[i])
        for i in config.keys()
        if not instrument or i == instrument
    }


class Security(BaseModel):
    # Murfey database settings
    murfey_db_credentials: Path
    crypto_key: str
    sqlalchemy_pooling: bool = True

    # ISPyB settings
    ispyb_credentials: Optional[Path] = None

    # Murfey server connection settings
    auth_url: str = ""
    auth_type: Literal["password", "cookie"] = "password"
    auth_algorithm: str = ""
    auth_key: str = ""
    cookie_key: str = ""
    instrument_auth_url: str = ""
    instrument_auth_type: Literal["token", ""] = "token"
    allow_user_token: bool = False  # TUI 'user' token support
    session_validation: str = ""
    session_token_timeout: Optional[int] = None
    allow_origins: list[str] = ["*"]

    # RabbitMQ settings
    rabbitmq_credentials: Path
    feedback_queue: str = "murfey_feedback"

    # Graylog settings
    graylog_host: str = ""
    graylog_port: Optional[int] = None

    model_config = ConfigDict()

    @field_validator("graylog_port")
    def check_port_present_if_host_is(
        cls, v: Optional[int], info: ValidationInfo, **kwargs
    ) -> Optional[int]:
        if info.data.get("graylog_host") and v is None:
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
def get_machine_config(instrument_name: str = "") -> dict[str, MachineConfig]:
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
