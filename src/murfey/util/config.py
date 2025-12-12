from __future__ import annotations

import os
import socket
from functools import lru_cache
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
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
    instrument_type: str = ""  # For use with hierarchical config files
    image_path: Optional[Path] = None
    machine_override: str = ""

    # Hardware and software -----------------------------------------------------------
    camera: str = "FALCON"
    superres: bool = False
    calibrations: dict[str, Any] = {}
    acquisition_software: list[str] = []
    software_versions: dict[str, str] = {}
    software_settings_output_directories: dict[str, list[str]] = {}
    data_required_substrings: dict[str, dict[str, list[str]]] = {}

    # Client side directory setup -----------------------------------------------------
    data_directories: list[Path] = []
    create_directories: list[str] = ["atlas"]
    analyse_created_directories: list[str] = []
    gain_reference_directory: Optional[Path] = None
    eer_fractionation_file_template: str = ""

    # Data transfer setup -------------------------------------------------------------
    # General setup
    data_transfer_enabled: bool = True
    substrings_blacklist: dict[str, list[str]] = {
        "directories": [],
        "files": [],
    }

    # Rsync setup
    rsync_url: str = ""
    rsync_module: str = ""
    rsync_basepath: Optional[Path] = None
    allow_removal: bool = False

    # Upstream data download setup
    upstream_data_directories: dict[str, Path] = {}  # Previous sessions
    upstream_data_download_directory: Optional[Path] = None  # Set by microscope config
    upstream_data_search_strings: dict[str, list[str]] = {}  # For glob search
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
    default_model: Optional[Path] = None
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


@lru_cache(maxsize=1)
def machine_config_from_file(
    config_file_path: Path,
    instrument_name: str,
) -> dict[str, MachineConfig]:
    """
    Loads the machine config YAML file and constructs instrument-specific configs from
    a hierarchical set of dictionary key-value pairs. It will populate the keys listed
    in the general dictionary, then update the keys specified in the shared instrument
    dictionary, before finally updating the keys for that specific instrument.
    """

    def _recursive_update(base: dict[str, Any], new: dict[str, Any]):
        """
        Helper function to recursively update nested dictionaries.

        If the old and new values are both dicts, it will add the new keys and values
        to the existing dictionary recursively without overwriting entries.

        If the old and new values are both lists, it will extend the existing list.
        For all other values, it will overwrite the existing value with the new one.
        """
        for key, value in new.items():
            # If new values are dicts and dict values already exist, do recursive update
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                base[key] = _recursive_update(base[key], value)
            # If new values are lists and a list already exists, extend the list
            elif (
                key in base and isinstance(base[key], list) and isinstance(value, list)
            ):
                base[key].extend(value)
            # Otherwise, overwrite/add values as normal
            else:
                base[key] = value
        return base

    # Load the dict from the file
    with open(config_file_path, "r") as config_stream:
        master_config: dict[str, Any] = yaml.safe_load(config_stream)

    # Construct requested machine configs from the YAML file
    all_machine_configs: dict[str, MachineConfig] = {}
    for i in sorted(master_config.keys()):
        # Skip reserved top-level keys
        if i in ("general", "clem", "fib", "tem"):
            continue
        # If instrument name is set, skip irrelevant configs
        if instrument_name and i != instrument_name:
            continue

        # Construct instrument config hierarchically
        config: dict[str, Any] = {}

        # Populate with general values
        general_config: dict[str, Any] = master_config.get("general", {})
        config = _recursive_update(config, general_config)

        # Populate with shared instrument values
        instrument_config: dict[str, Any] = master_config.get(i, {})
        instrument_shared_config: dict[str, Any] = master_config.get(
            str(instrument_config.get("instrument_type", "")).lower(), {}
        )
        config = _recursive_update(config, instrument_shared_config)

        # Insert instrument-specific values
        config = _recursive_update(config, instrument_config)

        # Add to master dictionary
        all_machine_configs[i] = MachineConfig(**config)

    return all_machine_configs


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
    # Create an empty machine config as a placeholder
    machine_configs = {instrument_name: MachineConfig()}
    if settings.murfey_machine_configuration:
        machine_configs = machine_config_from_file(
            Path(settings.murfey_machine_configuration), instrument_name
        )
    return machine_configs


def get_extended_machine_config(
    extension_name: str, instrument_name: str = ""
) -> Optional[BaseModel]:
    machine_config = get_machine_config(instrument_name=instrument_name).get(
        instrument_name or get_microscope()
    )
    if not machine_config:
        return None
    model = list(entry_points(group="murfey.config", name=extension_name))[0].load()
    data = getattr(machine_config, extension_name, {})
    return model(**data)
