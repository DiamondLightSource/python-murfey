from __future__ import annotations

import os
import socket
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Mapping, Optional, Union

import yaml
from backports.entry_points_selectable import entry_points
from pydantic import (
    BaseConfig,
    BaseModel,
    BaseSettings,
    Extra,
    Field,
    root_validator,
    validator,
)
from pydantic.errors import NoneIsNotAllowedError


class MachineConfig(BaseModel):
    """
    General information about the instrument being supported
    """

    display_name: str = Field(
        default="",
        description="Name of instrument used for display purposes, i.e. Krios I.",
    )
    instrument_name: str = Field(
        default="",
        description=(
            "Computer-friendly instrument reference name, i.e. m02. "
            "The name must not contain special characters or whitespace."
        ),
    )
    image_path: Optional[Path] = Field(
        default=None,
        description="Path to an image of the instrument for display purposes.",
    )
    machine_override: str = Field(
        default="",
        description=(
            "Override the instrument name as defined in the environment variable or "
            "the configuration with this one. This is used if, for example, many "
            "machines are sharing a server, and need to be named differently."
        ),
    )

    """
    Information about the hardware and software on the instrument machine
    """
    camera: Literal["FALCON", "K3_FLIPX", "K3_FLIPY", ""] = Field(
        default="",
        description=(
            "Name of the camera used by the TEM. This is only relevant for TEMs to "
            "determine how the gain reference needs to be processed, e.g., if it has "
            "to be binned down from superres or flipped along the x- or y-axis. "
            "Options: 'FALCON', 'K3_FLIPX', 'K3_FLIPY', ''"
        ),
        # NOTE:
        #   Eventually need to support Falcon 4, Falcon 4I, K2, K3 (superres)
        #   _FLIPX/_FLIPY is to tell it what to do with the gain reference.
        #   -   These will eventually be removed, leaving only the camera name
        #   -   Will need to create a new key to record whether the gain reference
        #       image needs to be flippedflip_gain: X, Y, None
    )
    superres: bool = Field(
        default=False,
        description=(
            "Check if the superres feature present on this microscope? "
            "For a Gatan K3, this will be set to True."
        ),
    )
    flip_gain: Literal["x", "y", ""] = Field(
        default="",
        description=(
            "State if the gain reference needs to be flipped along a specific axis. "
            "Options: 'x', 'y', or ''."
        ),
        # NOTE: This is a placeholder for a key that will be implemented in the future
    )
    calibrations: dict[str, dict[str, Union[dict, float]]] = Field(
        default={},
        description=(
            "Nested dictionary containing the calibrations for this microscope. "
            "E.g., 'magnification' would be a valid dictionary, in which the "
            "pixel size (in angstroms) at each magnfication level is provided as a "
            "key-value pair. Options: 'magnification'"
        ),
    )

    # NOTE:
    #   acquisition_software, software_versions, and software_settings_output_directories
    #   can all potentially be combined into one nested dictionary
    acquisition_software: list[
        Literal["epu", "tomo", "serialem", "autotem", "leica"]
    ] = Field(
        default=[],
        description=("List of all the acquisition software present on this machine."),
    )
    software_versions: dict[str, str] = Field(
        default={},
        description=(
            "Dictionary containing the version number of the acquisition software as "
            "key-value pairs."
        ),
    )
    software_settings_output_directories: dict[str, list[str]] = Field(
        default={},
        description=(
            "A dictionary in which the keys are the full file paths to the settings "
            "for the acquisition software packages, and the values are lists of keys "
            "through the layered structure of the XML settings files to where the save "
            "directory can be overwritten."
        ),
    )

    # Instrument-side file paths
    data_required_substrings: dict[str, dict[str, list[str]]] = Field(
        default={},
        description=(
            "Nested dictionary stating the file suffixes to look for as part of the "
            "processing workflow for a given software package, and subsequently the "
            "key phrases to search for within the file name for it to be selected for "
            "processing."
        ),
    )
    data_directories: list[Path] = Field(
        default=[],
        description=(
            "List of full paths to where data is stored on the instrument machine."
        ),
    )
    create_directories: dict[str, str] = Field(
        default={"atlas": "atlas"},
        description=(
            "Dictionary describing the directories to create within each visit on the "
            "instrument machine. The key will be what Murfey calls the folder internaly, "
            "while the value is what the folder is actually called on the file system."
        ),
        # NOTE: This key should eventually be changed into a list of strings
    )
    analyse_created_directories: list[str] = Field(
        default=[],
        description=(
            "List of folders to be considered for analysis by Murfey. This will "
            "generally be a subset of the list of folders specified earlier when "
            "creating the directories for each visit."
        ),
    )
    gain_reference_directory: Optional[Path] = Field(
        default=None,
        description=(
            "Full path to where the gain reference from the detector is saved."
        ),
    )
    eer_fractionation_file_template: str = Field(
        default="",
        description=(
            "File path template that can be provided if the EER fractionation files "
            "are saved in a location separate from the rest of the data. This will "
            "be a string, with '{visit}' and '{year}' being optional arguments that "
            "can be embedded in the string. E.g.: '/home/user/data/{year}/{visit}'"
        ),
        # Only if Falcon is used
        # To avoid others having to follow the {year}/{visit} format we are doing
    )

    """
    Data transfer-related settings
    """
    # rsync-related settings (only if rsync is used)
    data_transfer_enabled: bool = Field(
        default=False,
        description=("Toggle whether to enable data transfer via rsync."),
        # NOTE: Only request input for this code block if data transfer is enabled
    )
    allow_removal: bool = Field(
        default=False, description="Allow original files to be removed after rsync."
    )
    rsync_basepath: Path = Field(
        default=Path("/"),
        description=(
            "Full path on the storage server that the rsync daemon will append the "
            "relative paths of the transferred files to."
        ),
        # If rsync is disabled, rsync_basepath works out to be "/".
        # Must always be set.
    )
    rsync_module: str = Field(
        default="",
        description=(
            "Name of the rsync module the files are being transferred with. The module "
            "will be appended to the rsync base path, and the relative paths will be "
            "appended to the module. This is particularly useful when many instrument "
            "machines are transferring to the same storage server, as you can specify "
            "different sub-folders to save the data to."
        ),
    )
    rsync_url: str = Field(
        default="",
        description=(
            "URL to a remote rsync daemon. By default, the rsync daemon will be "
            "running on the client machine, and this defaults to an empty string."
        ),
    )

    # Related visits and data
    upstream_data_directories: list[Path] = Field(
        default=[],
        description=(
            "List of full paths to folders on other machines for Murfey to look for the "
            "current visit in. This is primarily used for multi-instrument workflows "
            "that use processed data from other instruments as input."
        ),
    )
    upstream_data_download_directory: Optional[Path] = Field(
        default=None,
        description=(
            "Path to the folder on this instrument machine to transfer files from other "
            "machines to."
        ),
    )
    upstream_data_tiff_locations: list[str] = Field(
        default=["processed"],
        description=(
            "Name of the sub-folder within the visit folder from which to transfer the "
            "results. This would typically be the 'processed' folder."
        ),
        # NOTE: This should eventually be converted into a dictionary, which looks for
        # files in different locations according to the workflows they correspond to
    )

    """
    Data processing-related settings
    """
    # Processing-related keys
    processing_enabled: bool = Field(
        default=False,
        description="Toggle whether to enable data processing.",
        # NOTE: Only request input for this code block if processing is enabled
    )
    process_by_default: bool = Field(
        default=True,
        description=(
            "Toggle whether processing should be enabled by default. If False, Murfey "
            "will ask the user whether they want to process the data in their current "
            "session."
        ),
    )

    # Server-side file paths
    gain_directory_name: str = Field(
        default="processing",
        description=(
            "Name of the folder to save the files used to facilitate data processing to. "
            "This folder will be located under the current visit."
        ),
    )
    processed_directory_name: str = Field(
        default="processed",
        description=(
            "Name of the folder to save the output of the data processing workflow to. "
            "This folder will be located under the current visit."
        ),
    )
    processed_extra_directory: str = Field(
        default="",
        description=(
            "Name of the sub-folder in the processed directory to save the output of "
            "additional processing workflows to. E.g., if you are using Relion for "
            "processing, its output files could be stored in a 'relion' sub-folder."
        ),
        # NOTE: This should eventually be a list of strings, if we want to allow
        # users to add more processing options to their workflow
    )

    # TEM-related processing workflows
    recipes: dict[
        Literal[
            "em-spa-bfactor",
            "em-spa-class2d",
            "em-spa-class3d",
            "em-spa-preprocess",
            "em-spa-refine",
            "em-tomo-preprocess",
            "em-tomo-align",
        ],
        str,
    ] = Field(
        default={
            "em-spa-bfactor": "em-spa-bfactor",
            "em-spa-class2d": "em-spa-class2d",
            "em-spa-class3d": "em-spa-class3d",
            "em-spa-preprocess": "em-spa-preprocess",
            "em-spa-refine": "em-spa-refine",
            "em-tomo-preprocess": "em-tomo-preprocess",
            "em-tomo-align": "em-tomo-align",
        },
        description=(
            "A dictionary of recipes for Murfey to run to facilitate data processing. "
            "The key represents the name of the recipe used by Murfey, while its value "
            "is the name of the recipe in the repository it's in."
        ),
        # NOTE: Currently, this recipe-searching structure is tied to the GitLab repo;
        # need to provide an option to map it file paths instead, or even a folder.
        # A parameter like recipe_folder might work?
    )
    modular_spa: bool = Field(
        default=True,
        description=(
            "Deprecated key to toggle SPA processing; will be phased out eventually."
        ),
    )

    # Particle picking settings
    default_model: Optional[Path] = Field(
        default=None,
        description=(
            "Path to the default machine learning model used for particle picking."
        ),
    )
    model_search_directory: str = Field(
        default="processing",
        description=(
            "Relative path to where user-uploaded machine learning models are stored. "
            "Murfey will look for the folders under the current visit."
        ),
    )
    initial_model_search_directory: str = Field(
        default="processing/initial_model",  # User-uploaded electron density models
        description=(
            "Relative path to where user-uploaded electron density models are stored. "
            "Murfey will look for the folders under the current visit."
        ),
    )

    # Extra plugins for data acquisition(?)
    external_executables: dict[str, Path] = Field(
        default={},
        description=(
            "Dictionary containing additional software packages to be used as part of "
            "the processing workflow. The keys are the names of the packages and the "
            "values are the full paths to where the executables are located."
        ),
    )
    external_executables_eer: dict[str, Path] = Field(
        default={},
        description=(
            "A similar dictionary, but for the executables associated with processing "
            "EER files."
        ),
        # NOTE: Both external_executables variables should be combined into one. The
        # EER ones could be their own key, where different software packages are
        # provided for different file types in different workflows.
    )
    external_environment: dict[str, str] = Field(
        default={},
        description=(
            "Dictionary containing full paths to folders containing the supporting "
            "software needed to run the executables to be used. These paths will be "
            "appended to the $PATH environment variable, so if multiple paths are "
            "associated with a single executable, they need to be provided as colon-"
            "separated strings. E.g. '/this/is/one/folder:/this/is/another/one'"
        ),
    )
    plugin_packages: dict[str, Path] = Field(
        default={},
        description=(
            "Dictionary containing full paths to additional plugins for Murfey that "
            "help support the data collection and processing workflow."
        ),
    )

    """
    Server and network-related configurations
    """
    # Security-related keys
    global_configuration_path: Optional[Path] = Field(
        description=(
            "Full file path to the YAML file containing the configurations for the "
            "Murfey server."
        ),
        alias="security_configuration_path",
    )
    # Network connections
    frontend_url: str = Field(
        default="http://localhost:3000",
        description="URL to the Murfey frontend.",
    )
    murfey_url: str = Field(
        default="http://localhost:8000",
        description="URL to the Murfey API.",
    )
    instrument_server_url: str = Field(
        default="http://localhost:8001",
        description="URL to the instrument server.",
    )
    auth_url: str = Field(
        default="",
        description="URL to where users can authenticate their Murfey sessions.",
    )

    # RabbitMQ-specific keys
    failure_queue: str = Field(
        default="",
        description="Name of RabbitMQ queue where failed API calls will be recorded.",
    )
    node_creator_queue: str = Field(
        default="node_creator",
        description=(
            "Name of the RabbitMQ queue where requests for creating job nodes are sent."
        ),
    )

    class Config(BaseConfig):
        """
        Additional settings for how this Pydantic model behaves
        """

        extra = Extra.allow
        json_encoders = {Path: str}

    @validator("camera", always=True, pre=True)
    def __validate_camera_model__(cls, value: str):
        # Let non-strings fail validation naturally
        if not isinstance(value, str):
            return value
        # Handle empty string
        if len(value) == 0:
            return value
        # Match string to known camera models
        supported_camera_models = ("FALCON", "K3")
        if value.upper().startswith(
            supported_camera_models
        ):  # Case-insensitive matching
            return value.upper()
        else:
            raise ValueError(
                f"unexpected value; permitted: {supported_camera_models!r} "
                f"(type=value_error.const; given={value!r}; "
                f"permitted={supported_camera_models!r})"
            )

    @root_validator(pre=False)
    def __validate_superres__(cls, model: dict):
        camera: str = model.get("camera", "")
        model["superres"] = True if camera.startswith("K3") else False
        return model

    @validator("rsync_basepath", always=True)
    def __validate_rsync_basepath_if_transfer_enabled__(
        cls, v: Optional[str], values: Mapping[str, Any]
    ) -> Any:
        """
        If data transfer is enabled, an rsync basepath must be provided.
        """
        if values.get("data_transfer_enabled"):
            if v is None:
                raise NoneIsNotAllowedError
        return v

    @validator("default_model", always=True)
    def __validate_default_model_if_processing_enabled_and_spa_possible__(
        cls, v: Optional[str], values: Mapping[str, Any]
    ) -> Any:
        """
        If data processing is enabled, a machine learning model must be provided.
        """
        if values.get("processing_enabled") and "epu" in values.get(
            "acquisition_software", []
        ):
            if v is None:
                raise NoneIsNotAllowedError
        return v


def machine_config_from_file(
    config_file_path: Path, instrument: str = ""
) -> dict[str, MachineConfig]:
    with open(config_file_path, "r") as config_stream:
        config = yaml.safe_load(config_stream)
    return {
        i: MachineConfig(**config[i])
        for i in config.keys()
        if not instrument or i == instrument
    }


class GlobalConfig(BaseModel):
    # Database connection settings
    murfey_db_credentials: Optional[Path] = Field(
        description=(
            "Full file path to where Murfey's SQL database credentials are stored. "
            "This is typically a YAML file."
        ),
    )
    sqlalchemy_pooling: bool = Field(
        default=True,
        description=(
            "Toggles connection pooling functionality in the SQL database. If 'True', "
            "clients will connect to the database using an existing pool of connections "
            "instead of creating a new one every time."
        ),
    )
    crypto_key: str = Field(
        default="",
        description=(
            "The encryption key used for the SQL database. This can be generated by "
            "Murfey using the 'murfey.generate_key' command."
        ),
    )

    # RabbitMQ settings
    rabbitmq_credentials: Optional[Path]
    feedback_queue: str = Field(
        default="murfey_feedback",
        description=(
            "The name of the RabbitMQ queue that will receive instructions and "
            "the results of processing jobs on behalf of Murfey. This queue can be "
            "by multiple server instances, which is why it's stored here instead of "
            "in the machine configuration."
        ),
    )

    # Server authentication settings
    auth_type: Literal["password", "cookie"] = Field(
        default="password",
        description=(
            "Choose how Murfey will authenticate new connections that it receives. "
            "This can be done at present via password authentication or exchanging "
            "cookies."
        ),
    )
    auth_key: str = ""
    auth_algorithm: str = ""
    cookie_key: str = ""
    session_validation: str = ""
    session_token_timeout: Optional[int] = (
        None  # seconds; typically the length of a microscope session plus a bit
    )
    allow_origins: list[str] = ["*"]  # Restrict to only certain hostnames

    # Graylog settings
    graylog_host: str = ""
    graylog_port: Optional[int] = None

    @validator("graylog_port")
    def check_port_present_if_host_is(
        cls, v: Optional[int], values: dict, **kwargs
    ) -> Optional[int]:
        if values["graylog_host"] and v is None:
            raise ValueError("The Graylog port must be set if the Graylog host is")
        return v


def global_config_from_file(config_file_path: Path) -> GlobalConfig:
    with open(config_file_path, "r") as config_stream:
        config = yaml.safe_load(config_stream)
    return GlobalConfig(**config)


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""
    murfey_global_configuration: str = ""


settings = Settings()


@lru_cache()
def get_hostname():
    return socket.gethostname()


# How does microscope_name differ from instrument_name?
# Should we stick to one?
def get_microscope(machine_config: MachineConfig | None = None) -> str:
    if machine_config:
        microscope_name = machine_config.machine_override or os.getenv("BEAMLINE", "")
    else:
        microscope_name = os.getenv("BEAMLINE", "")
    return microscope_name


@lru_cache(maxsize=1)
def get_global_config() -> GlobalConfig:
    if settings.murfey_global_configuration:
        return global_config_from_file(Path(settings.murfey_global_configuration))
    if settings.murfey_machine_configuration and os.getenv("BEAMLINE"):
        machine_config = get_machine_config(instrument_name=os.getenv("BEAMLINE"))[
            os.getenv("BEAMLINE", "")
        ]
        if not machine_config.global_configuration_path:
            raise FileNotFoundError("No global configuration file provided")
        return global_config_from_file(machine_config.global_configuration_path)
    return GlobalConfig(
        rabbitmq_credentials=None,
        session_validation="",
        murfey_db_credentials=None,
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
            default_model="/tmp/weights.h5",
        )
    }
    if settings.murfey_machine_configuration:
        microscope = instrument_name
        machine_config = machine_config_from_file(
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
