from __future__ import annotations

import argparse
import json
import re
from ast import literal_eval
from pathlib import Path
from typing import Any, Callable, Optional, Type, get_type_hints

import yaml
from pydantic import ValidationError
from pydantic.error_wrappers import ErrorWrapper
from pydantic.fields import ModelField, UndefinedType
from rich.console import Console

from murfey.util.config import MachineConfig

# Create a console object for pretty printing
console = Console()

# Compile types for each key present in MachineConfig
machine_config_types: dict = get_type_hints(MachineConfig)


def prompt(message: str, style: str = "") -> str:
    """
    Helper function to pretty print a message and have the user input their response
    on a new line.
    """
    console.print(message, style=style)
    return input("> ")


def print_field_info(field: ModelField):
    """
    Helper function to print out the name of the key being set up, along with a short
    description of what purpose the key serves.
    """
    console.print(
        f"{field.name.replace('_', ' ').title()} ({field.name})",
        style="bold bright_cyan",
    )
    console.print(field.field_info.description, style="italic bright_cyan")
    if not isinstance(field.field_info.default, UndefinedType):
        console.print(f"Default: {field.field_info.default!r}", style="bright_cyan")


def ask_for_permission(message: str) -> bool:
    """
    Helper function to generate a Boolean based on user input
    """
    while True:
        answer = prompt(message, style="yellow").lower().strip()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        console.print("Invalid input. Please try again.", style="red")
        continue


def ask_for_input(parameter: str, again: bool = False):
    """
    Asks the user if another value should be entered into the current data structure.
    """
    message = (
        "Would you like to add "
        + (
            "another"
            if again
            else (
                "an" if parameter.lower().startswith(("a", "e", "i", "o", "u")) else "a"
            )
        )
        + f" {parameter}? (y/n)"
    )
    return ask_for_permission(message)


def confirm_overwrite(value: str):
    """
    Asks the user if a value that already exists should be overwritten.
    """
    message = f"{value!r} already exists; do you wish to overwrite it? (y/n)"
    return ask_for_permission(message)


def confirm_duplicate(value: str):
    """
    Asks the user if a duplicate value should be allowed.
    """
    message = f"{value!r} already exists; do you want to add a duplicate? (y/n)"
    return ask_for_permission(message)


def get_folder_name(message: Optional[str] = None) -> str:
    """
    Helper function to interactively generate, validate, and return a folder name.
    """
    while True:
        message = "Please enter the folder name." if message is None else message
        value = prompt(message, style="yellow").strip()
        if bool(re.fullmatch(r"[\w\s\-]*", value)) is True:
            return value
        console.print(
            "There are unsafe characters present in this folder name. Please "
            "use a different one.",
            style="red",
        )
        if ask_for_input("folder name", True) is False:
            return ""
        continue


def get_folder_path(message: Optional[str] = None) -> Path | None:
    """
    Helper function to interactively generate, validate, and return the full path
    to a folder.
    """
    while True:
        message = (
            "Please enter the full path to the folder." if message is None else message
        )
        value = prompt(message, style="yellow").strip()
        if not value:
            return None
        try:
            path = Path(value).resolve()
            return path
        except Exception:
            console.print("Unable to resolve provided file path", style="red")
            if ask_for_input("file path", True) is False:
                return None
            continue


def get_file_path(message: Optional[str] = None) -> Path | None:
    """
    Helper function to interactively generate, validate, and return the full path
    to a file.
    """
    while True:
        message = (
            "Please enter the full path to the file." if message is None else message
        )
        value = prompt(message, style="yellow").strip()
        if not value:
            return None
        file = Path(value).resolve()
        if file.suffix:
            return file
        console.print(f"{str(file)!r} doesn't appear to be a file", style="red")
        if ask_for_input("file", True) is False:
            return None
        continue


def construct_list(
    value_name: str,
    value_method: Optional[Callable] = None,
    value_method_args: dict = {},
    allow_empty: bool = False,
    allow_eval: bool = True,
    many_types: bool = True,
    restrict_to_types: Optional[Type[Any] | tuple[Type[Any]]] = None,
    sort_values: bool = True,
    debug: bool = False,
) -> list[Any]:
    """
    Helper function to facilitate interactive construction of a list.
    """
    lst: list = []
    add_entry = ask_for_input(value_name, False)
    while add_entry is True:
        value = (
            prompt(
                "Please enter "
                + ("an" if value_name.startswith(("a", "e", "i", "o", "u")) else "a")
                + f" {value_name}",
                style="yellow",
            )
            if value_method is None
            else value_method(**value_method_args)
        )
        # Reject empty inputs if set
        if not value and not allow_empty:
            console.print("No value provided.", style="red")
            add_entry = ask_for_input(value_name, True)
            continue
        # Convert values if set
        try:
            eval_value = literal_eval(value) if allow_eval else value
        except Exception:
            eval_value = value
        # Check if it's a permitted type (continue to allow None as value)
        if restrict_to_types is not None:
            allowed_types = (
                (restrict_to_types,)
                if not isinstance(restrict_to_types, (list, tuple))
                else restrict_to_types
            )
            if not isinstance(eval_value, allowed_types):
                console.print(
                    f"The provided value ({type(eval_value)}) is not an allowed type.",
                    style="red",
                )
                add_entry = ask_for_input(value_name, True)
                continue
        # Confirm if duplicate entry should be added
        if eval_value in lst and confirm_duplicate(str(eval_value)) is False:
            add_entry = ask_for_input(value_name, True)
            continue
        lst.append(eval_value)
        # Reject list with multiple types if set
        if not many_types and len({type(item) for item in lst}) > 1:
            console.print(
                "The provided value is of a different type to the other members. It "
                "won't be added to the list.",
                style="red",
            )
            lst = lst[:-1]
        # Sort values if set
        # Sort numeric values differently from alphanumeric ones
        lst = (
            sorted(
                lst,
                key=lambda v: (
                    (0, float(v), 0)
                    if isinstance(v, (int, float))
                    else (
                        (1, abs(v), v.real)
                        if isinstance(v, complex)
                        else (2, str(v), "")
                    )
                ),
            )
            if sort_values
            else lst
        )
        add_entry = ask_for_input(value_name, True)
        continue
    return lst


def construct_dict(
    dict_name: str,
    key_name: str,
    value_name: str,
    key_method: Optional[Callable] = None,
    key_method_args: dict = {},
    value_method: Optional[Callable] = None,
    value_method_args: dict = {},
    allow_empty_key: bool = True,
    allow_empty_value: bool = True,
    allow_eval: bool = True,
    sort_keys: bool = True,
    restrict_to_types: Optional[Type[Any] | tuple[Type[Any], ...]] = None,
    debug: bool = False,
) -> dict[str, Any]:
    """
    Helper function to facilitate the interative construction of a dictionary.
    """

    def is_type(value: str, instance: Type[Any] | tuple[Type[Any], ...]) -> bool:
        """
        Checks if the string provided evaluates to one of the desired types
        """
        instance = (instance,) if not isinstance(instance, (list, tuple)) else instance
        try:
            eval_value = literal_eval(value)
        except Exception:
            eval_value = value
        return isinstance(eval_value, instance)

    dct: dict = {}
    add_entry = ask_for_input(dict_name, False)
    key_message = f"Please enter the {key_name}"
    value_message = f"Please enter the {value_name}"
    while add_entry is True:
        # Add key
        key = str(
            prompt(key_message, style="yellow").strip()
            if key_method is None
            else key_method(**key_method_args)
        )
        # Reject empty keys if set
        if not allow_empty_key and not key:
            console.print(f"No {key_name} provided.", style="red")
            add_entry = ask_for_input(dict_name, True)
            continue
        # Confirm overwrite key on duplicate
        if key in dct.keys():
            if confirm_overwrite(key) is False:
                add_entry = ask_for_input(dict_name, True)
                continue
        # Add value
        value = (
            prompt(value_message, style="yellow").strip()
            if value_method is None
            else value_method(**value_method_args)
        )
        # Reject empty values if set
        if not allow_empty_value and not value:
            console.print("No value provided", style="red")
            add_entry = ask_for_input(dict_name, True)
            continue
        # Convert values if set
        try:
            eval_value = literal_eval(value) if allow_eval else value
        except Exception:
            eval_value = value
        # Reject incorrect value types if set
        if restrict_to_types is not None:
            allowed_types = (
                (restrict_to_types,)
                if not isinstance(restrict_to_types, (tuple, list))
                else restrict_to_types
            )
            if not isinstance(eval_value, allowed_types):
                console.print("The value is not of an allowed type.", style="red")
                add_entry = ask_for_input(dict_name, True)
                continue
        # Assign value to key
        dct[key] = eval_value
        add_entry = ask_for_input(dict_name, True)
        continue

    # Sort keys if set
    # Sort numeric keys separately from alphanumeric ones
    dct = (
        {
            key: dct[key]
            for key in sorted(
                dct.keys(),
                key=lambda k: (
                    (0, float(k), 0)
                    if is_type(k, (int, float))
                    else (
                        (1, abs(complex(k)), complex(k).real)
                        if is_type(k, complex)
                        else (2, str(k), "")
                    )
                ),
            )
        }
        if sort_keys
        else dct
    )
    return dct


def validate_value(value: Any, key: str, field: ModelField, debug: bool = False) -> Any:
    """
    Helper function to validate the value of a field in the Pydantic model.
    """
    validated_value, errors = field.validate(value, {}, loc=key)
    if errors:
        raise ValidationError(
            ([errors] if isinstance(errors, ErrorWrapper) else errors), MachineConfig
        )
    console.print(f"{key!r} validated successfully.", style="bright_green")
    if debug:
        console.print(f"Type: {type(validated_value)}", style="bright_green")
        console.print(f"{validated_value!r}", style="bright_green")
    return validated_value


def populate_field(key: str, field: ModelField, debug: bool = False) -> Any:
    """
    General function for inputting and validating the value of a single field against
    its Pydantic model.
    """

    # Display information on the field to be filled
    print_field_info(field)
    message = "Please provide a value (press Enter to leave it blank as '')."
    while True:
        # Get value
        answer = prompt(message, style="yellow")
        # Translate empty string into None for fields that take Path values
        value = (
            None
            if (not answer and machine_config_types.get(key) in (Path, Optional[Path]))
            else answer
        )

        # Validate and return
        try:
            return validate_value(value, key, field, debug)
        except ValidationError as error:
            if debug:
                console.print(error, style="red")
            console.print(f"Invalid input for {key!r}. Please try again")
            continue


def add_calibrations(
    key: str, field: ModelField, debug: bool = False
) -> dict[str, dict]:
    """
    Populate the 'calibrations' field with dictionaries.
    """
    # Known calibrations and what to call their keys and values
    known_calibrations: dict[str, tuple[str, str]] = {
        # Calibration type | Key name | Value name
        "magnification": ("magnification", "pixel size (in angstroms)")
    }

    # Start of add_calibrations
    print_field_info(field)
    category = "calibration setting"
    calibrations: dict = {}
    add_calibration = ask_for_input(category, False)
    while add_calibration is True:
        calibration_type = prompt(
            "What type of calibration settings are you providing?",
            style="yellow",
        ).lower()
        # Check if it's a known type of calibration
        if calibration_type not in known_calibrations.keys():
            console.print(
                f"{calibration_type} is not a known type of calibration",
                style="red",
            )
            add_calibration = ask_for_input(category, True)
            continue
        # Handle duplicate keys
        if calibration_type in calibrations.keys():
            if confirm_overwrite(calibration_type) is False:
                add_calibration = ask_for_input(category, True)
                continue
        # Skip failed inputs
        calibration_values = construct_dict(
            f"{calibration_type} calibration",
            known_calibrations[calibration_type][0],
            known_calibrations[calibration_type][1],
            allow_empty_key=False,
            allow_empty_value=False,
            allow_eval=True,
            sort_keys=True,
        )
        if not calibration_values:
            add_calibration = ask_for_input(category, True)
            continue

        # Add calibration to master dict
        calibrations[calibration_type] = calibration_values
        console.print(
            f"Added {calibration_type} to the calibrations field",
            style="bright_green",
        )
        if debug:
            console.print(f"{calibration_values}", style="bright_green")

        # Check if any more calibrations need to be added
        add_calibration = ask_for_input("calibration setting", again=True)

    # Validate the nested dictionary structure
    try:
        return validate_value(calibrations, key, field, debug)
    except ValidationError as error:
        if debug:
            console.print(error, style="red")
        console.print(f"Failed to validate {key!r}.", style="red")
        if ask_for_input(category, True) is True:
            return add_calibrations(key, field, debug)
        console.print("Returning an empty dictionary", style="red")
        return {}


def add_software_packages(config: dict, debug: bool = False) -> dict[str, Any]:
    def get_software_name() -> str:
        """
        Function to interactively generate, validate, and return the name of a
        supported software package.
        """
        message = (
            "What is the name of the software package? Supported options: 'autotem', "
            "'epu', 'leica', 'serialem', 'tomo'"
        )
        name = prompt(message, style="yellow").lower().strip()
        # Validate name against "acquisition_software" field
        try:
            field = MachineConfig.__fields__["acquisition_software"]
            return validate_value([name], "acquisition_software", field, False)[0]
        except ValidationError:
            console.print("Invalid software name.", style="red")
            if ask_for_input("software package", True) is True:
                return get_software_name()
            console.print("Returning an empty string.", style="red")
            return ""

    def ask_about_settings_file() -> bool:
        message = (
            "Does this software package have a settings file that needs modification? "
            "(y/n)"
        )
        return ask_for_permission(message)

    def get_settings_tree_path() -> str:
        message = "What is the path through the XML file to the node to overwrite?"
        xml_tree_path = prompt(message, style="yellow").strip()
        # TODO: Currently no test cases for this method
        return xml_tree_path

    """
    Start of add_software_packages
    """
    console.print(
        "Acquisition Software (acquisition_software)",
        style="bold bright_cyan",
    )
    console.print(
        "This is where aquisition software packages present on the instrument machine "
        "can be specified, along with the output file names and extensions that are of "
        "interest.",
        style="italic bright_cyan",
    )
    package_info: dict = {}
    category = "software package"
    add_input = ask_for_input(category, again=False)
    while add_input:
        # Collect software name
        console.print(
            "Acquisition Software (acquisition_software)",
            style="bold bright_cyan",
        )
        console.print(
            "Name of the acquisition software installed on this instrument.",
            style="italic bright_cyan",
        )
        console.print(
            "Options: 'autotem', 'epu', 'leica', 'serialem', 'tomo'",
            style="bright_cyan",
        )
        name = get_software_name()
        if name in package_info.keys():
            if confirm_overwrite(name) is False:
                add_input = ask_for_input(category, False)
                continue

        # Collect version info
        console.print(
            "Software Versions (software_versions)",
            style="bold bright_cyan",
        )
        version = prompt(
            "What is the version number of this software package? Press Enter to leave "
            "it blank if you're unsure.",
            style="yellow",
        )

        # Collect settings files and modifications
        console.print(
            "Software Settings Output Directories (software_settings_output_directories)",
            style="bold bright_cyan",
        )
        console.print(
            "Some software packages will have settings files that require modification "
            "in order to ensure files are saved to the desired folders. The paths to "
            "the files and the path to the nodes in the settings files both need to be "
            "provided.",
            style="italic bright_cyan",
        )
        settings_file: Optional[Path] = (
            get_file_path(
                "What is the full path to the settings file? This is usually an XML file."
            )
            if ask_about_settings_file() is True
            else None
        )
        settings_tree_path = (
            get_settings_tree_path().split("/") if settings_file else []
        )

        # Collect extensions and filename substrings
        console.print(
            "Data Required Substrings (data_required_substrings)",
            style="bold bright_cyan",
        )
        console.print(
            "Different software packages will generate different output files. Only "
            "files with certain extensions and keywords in their filenames are needed "
            "for data processing. They are listed out here.",
            style="italic bright_cyan",
        )
        extensions_and_substrings: dict[str, list[str]] = construct_dict(
            dict_name="file extension configuration",
            key_name="file extension",
            value_name="file substrings",
            value_method=construct_list,
            value_method_args={
                "value_name": "file substring",
                "allow_empty": False,
                "allow_eval": False,
                "many_types": False,
                "restrict_to_types": str,
                "sort_values": True,
            },
            allow_empty_key=False,
            allow_empty_value=False,
            allow_eval=False,
            sort_keys=True,
            restrict_to_types=list,
        )

        # Compile keys for this package as a dict
        package_info[name] = {
            "version": version,
            "settings_file": settings_file,
            "settings_tree_path": settings_tree_path,
            "extensions_and_substrings": extensions_and_substrings,
        }
        add_input = ask_for_input(category, again=True)
        continue

    # Re-pack keys and values according to the current config field structures
    console.print("Compiling and validating inputs...")
    acquisition_software: list = []
    software_versions: dict = {}
    software_settings_output_directories: dict = {}
    data_required_substrings: dict = {}

    # Add keys after sorting
    for key in sorted(package_info.keys()):
        acquisition_software.append(key)
        if package_info[key]["version"]:
            software_versions[key] = package_info[key]["version"]
        if package_info[key]["settings_file"]:
            software_settings_output_directories[
                str(package_info[key]["settings_file"])
            ] = package_info[key]["settings_tree_path"]
        if package_info[key]["extensions_and_substrings"]:
            data_required_substrings[key] = package_info[key][
                "extensions_and_substrings"
            ]

    # Validate against their respective fields
    to_validate = (
        ("acquisition_software", acquisition_software),
        ("software_versions", software_versions),
        ("software_settings_output_directories", software_settings_output_directories),
        ("data_required_substrings", data_required_substrings),
    )
    for field_name, value in to_validate:
        try:
            field = MachineConfig.__fields__[field_name]
            config[field_name] = validate_value(value, field_name, field, debug)
        except ValidationError as error:
            if debug:
                console.print(error, style="red")
            console.print(f"Failed to validate {field_name!r}", style="red")
            if ask_for_input("software package configuration", True) is True:
                return add_software_packages(config)
            console.print(f"Skipped adding {field_name!r}.", style="red")

    # Return updated dictionary
    return config


def add_data_directories(
    key: str, field: ModelField, debug: bool = False
) -> dict[str, str]:
    """
    Function to facilitate populating the data_directories field.
    """
    print_field_info(field)
    category = "data directory"
    data_directories: dict[str, str] = construct_dict(
        category,
        "full file path to the data directory",
        "data type",
        allow_empty_key=False,
        allow_empty_value=False,
        allow_eval=False,
        sort_keys=True,
        restrict_to_types=str,
    )

    # Validate and return
    try:
        return validate_value(data_directories, key, field, debug)
    except ValidationError as error:
        if debug:
            console.print(error, style="red")
        console.print(f"Failed to validate {key!r}.", style="red")
        if ask_for_input(category, True) is True:
            return add_data_directories(key, field, debug)
        console.print("Returning an empty dictionary.", style="red")
        return {}


def add_create_directories(
    key: str, field: ModelField, debug: bool = False
) -> dict[str, str]:
    """
    Function to populate the create_directories field.
    """
    print_field_info(field)
    category = "folder for Murfey to create"
    folders_to_create: dict[str, str] = construct_dict(
        dict_name=category,
        key_name="folder alias",
        value_name="folder name",
        key_method=get_folder_name,
        key_method_args={
            "message": "Please enter the name Murfey should remember the folder as.",
        },
        value_method=get_folder_name,
        value_method_args={
            "message": "Please enter the name of the folder for Murfey to create.",
        },
        allow_empty_key=False,
        allow_empty_value=False,
        allow_eval=False,
        sort_keys=True,
        restrict_to_types=str,
    )

    # Validate and return
    try:
        return validate_value(folders_to_create, key, field, debug)
    except ValidationError as error:
        if debug:
            console.print(error, style="red")
        console.print(f"Failed to validate {key!r}.", style="red")
        if ask_for_input(category, True) is True:
            return add_create_directories(key, field, debug)
        console.print("Returning an empty dictionary.", style="red")
        return {}


def add_analyse_created_directories(
    key: str, field: ModelField, debug: bool = False
) -> list[str]:
    """
    Function to populate the analyse_created_directories field
    """
    print_field_info(field)
    category = "folder for Murfey to analyse"

    folders_to_analyse: list[str] = construct_list(
        value_name=category,
        value_method=get_folder_name,
        value_method_args={
            "message": "Please enter the name of the folder that Murfey is to analyse."
        },
        allow_empty=False,
        allow_eval=False,
        many_types=False,
        restrict_to_types=str,
        sort_values=True,
    )

    # Validate and return
    try:
        return sorted(validate_value(folders_to_analyse, key, field, debug))
    except ValidationError as error:
        if debug:
            console.print(error, style="red")
        console.print(f"Failed to validate {key!r}.", style="red")
        if ask_for_input(category, True) is True:
            return add_analyse_created_directories(key, field, debug)
        console.print("Returning an empty list.", style="red")
        return []


def set_up_data_transfer(config: dict, debug: bool = False) -> dict:
    """
    Helper function to set up the data transfer fields in the configuration
    """

    def get_upstream_data_directories(
        key: str, field: ModelField, debug: bool = False
    ) -> list[Path]:
        print_field_info(field)
        category = "upstream data directory"
        upstream_data_directories = construct_list(
            category,
            value_method=get_folder_path,
            value_method_args={
                "message": (
                    "Please enter the full path to the data directory "
                    "you wish to search for files in."
                ),
            },
            allow_empty=False,
            allow_eval=False,
            many_types=False,
            restrict_to_types=Path,
            sort_values=True,
        )
        try:
            return validate_value(upstream_data_directories, key, field, debug)
        except ValidationError as error:
            if debug:
                console.print(error, style="red")
            console.print(f"Failed to validate {key!r}.", style="red")
            if ask_for_input(category, True) is True:
                return get_upstream_data_directories(key, field, debug)
            console.print("Returning an empty list.", style="red")
            return []

    def get_upstream_data_tiff_locations(
        key: str, field: ModelField, debug: bool = False
    ) -> list[str]:
        print_field_info(field)
        category = "remote folder containing TIFF files"
        upstream_data_tiff_locations = construct_list(
            category,
            value_method=get_folder_name,
            value_method_args={
                "message": (
                    "Please enter the name of the folder on the remote machines "
                    "in which to search for TIFF files."
                )
            },
            allow_empty=False,
            allow_eval=False,
            many_types=False,
            restrict_to_types=str,
            sort_values=True,
        )
        try:
            return validate_value(upstream_data_tiff_locations, key, field, debug)
        except ValidationError as error:
            if debug:
                console.print(error, style="red")
            console.print(f"Failed to validate {key!r}.", style="red")
            if ask_for_input(category, True) is True:
                return get_upstream_data_tiff_locations(key, field, debug)
            console.print("Returning an empty list.", style="red")
            return []

    """
    Start of set_up_data_transfer
    """
    for key in (
        "data_transfer_enabled",
        "rsync_basepath",
        "rsync_module",
        "allow_removal",
        "upstream_data_directories",
        "upstream_data_download_directory",
        "upstream_data_tiff_locations",
    ):
        field = MachineConfig.__fields__[key]
        # Construct more complicated data structures
        if key == "upstream_data_directories":
            validated_value: Any = get_upstream_data_directories(key, field, debug)
        elif key == "upstream_data_tiff_locations":
            validated_value = get_upstream_data_tiff_locations(key, field, debug)
        # Use populate field to process simpler keys
        else:
            validated_value = populate_field(key, field, debug)

        # Add to config
        config[key] = validated_value

    return config


def set_up_data_processing(config: dict, debug: bool = False) -> dict:
    return config


def set_up_external_executables(config: dict, debug: bool = False) -> dict:
    return config


def set_up_machine_config(debug: bool = False):
    """
    Main function which runs through the setup process.
    """
    new_config: dict = {}
    for key, field in MachineConfig.__fields__.items():
        """
        Logic for complicated or related fields
        """
        if key == "superres":
            camera: str = new_config["camera"]
            new_config[key] = True if camera.lower().startswith("gatan") else False
            continue
        if key == "calibrations":
            new_config[key] = add_calibrations(key, field, debug)
            continue

        # Acquisition software block
        if key == "acquisition_software":
            new_config = add_software_packages(new_config, debug)
            continue
        if key in (
            "software_versions",
            "software_settings_output_directories",
            "data_required_substrings",
        ):
            continue
        # End of software block

        if key == "data_directories":
            new_config[key] = add_data_directories(key, field, debug)
            continue
        if key == "create_directories":
            new_config[key] = add_create_directories(key, field, debug)
            continue
        if key == "analyse_created_directories":
            new_config[key] = add_analyse_created_directories(key, field, debug)
            continue

        # Data transfer block
        if key == "data_transfer_enabled":
            new_config = set_up_data_transfer(new_config, debug)
            continue
        if key in (
            "allow_removal",
            "rsync_basepath",
            "rsync_module",
            "upstream_data_directories",
            "upstream_data_download_directory",
            "upstream_data_tiff_locations",
        ):
            continue
        # End of data transfer block

        # Data processing block
        if key == "processing_enabled":
            new_config = set_up_data_processing(new_config, debug)
            continue
        if key in (
            "process_by_default",
            "gain_directory_name",
            "processed_directory_name",
            "processed_extra_directory",
            "recipes",
            "modular_spa",
            "default_model",
            "model_search_directory",
            "initial_model_search_directory",
        ):
            continue
        # End of data processing block

        # External plugins and executables block
        if key == "external_executables":
            # TODO: Set up external plugins and exectuables
            new_config = set_up_external_executables(new_config, debug)
            continue
        if key in ("external_executables_eer", "external_environment"):
            continue
        # End of external executables block

        if key == "plugin_packages":
            # TODO
            continue

        """
        Standard method of inputting values
        """
        new_config[key] = populate_field(key, field, debug)

    # Validate the entire config again and convert into JSON/YAML-safe dict
    try:
        new_config_safe: dict = json.loads(MachineConfig(**new_config).json())
    except ValidationError as exception:
        # Print out validation errors found
        console.print("Validation failed", style="red")
        for error in exception.errors():
            console.print(f"{error}", style="red")
        # Offer to redo the setup, otherwise quit setup
        if ask_for_input("machine configuration", True) is True:
            return set_up_machine_config(debug)
        return False

    # Save config under its instrument name
    master_config: dict[str, dict] = {
        new_config_safe["instrument_name"]: new_config_safe
    }

    # Create save path for config
    console.print("Machine config successfully validated.", style="green")
    config_name = prompt(
        "What would you like to name the file? (E.g. 'my_machine_config')",
        style="yellow",
    )
    config_path = Path(
        prompt("Where would you like to save this config?", style="yellow")
    )
    config_file = config_path / f"{config_name}.yaml"
    config_path.mkdir(parents=True, exist_ok=True)

    # Check if config file already exists at the location
    if config_file.exists():
        with open(config_file) as existing_file:
            try:
                old_config: dict[str, dict] = yaml.safe_load(existing_file)
            except yaml.YAMLError as error:
                console.print(error, style="red")
                # Provide option to quit or try again
                if ask_for_input("machine configuration", True) is True:
                    return set_up_machine_config(debug)
                console.print("Exiting machine configuration setup guide")
                exit()
        # Check if settings already exist for this machine
        for key in master_config.keys():
            # Check if overwriting of existing config is needed
            if key in old_config.keys() and confirm_overwrite(key) is False:
                old_config[key].update(master_config[key])
            # Add new machine config
            else:
                old_config[key] = master_config[key]
        # Overwrite
        master_config = old_config
    with open(config_file, "w") as save_file:
        yaml.dump(master_config, save_file, default_flow_style=False)
    console.print(
        f"Machine configuration for {new_config_safe['instrument_name']!r} "
        f"successfully saved as {str(config_file)!r}",
        style="bright_green",
    )
    console.print("Machine configuration complete", style="bright_green")

    # Provide option to set up another machine configuration
    if ask_for_input("machine configuration", True) is True:
        return set_up_machine_config(debug)
    console.print("Exiting machine configuration setup guide", style="bright_green")
    return True


def run():
    # Set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Prints additional messages to show setup progress.",
    )
    args = parser.parse_args()

    set_up_machine_config(args.debug)
