import argparse
import json
import re
from pathlib import Path
from typing import Optional, get_type_hints

import yaml
from pydantic import ValidationError
from pydantic.fields import ModelField, UndefinedType
from rich.console import Console

from murfey.util.config import MachineConfig

# Create a console object for pretty printing
console = Console()

# Compile types for each key present in MachineConfig
machine_config_types: dict = get_type_hints(MachineConfig)


def prompt(message: str, style: str = "") -> str:
    """
    Helper function to pretty print the prompt message and add the actual prompt on a
    newline.
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


def ask_for_input(category: str, again: bool = False):
    """
    Perform a Boolean check to see if another value is to be appended to the current
    parameter being set up.
    """
    message = (
        "Would you like to add " + ("another" if again else "a") + f" {category}? (y/n)"
    )
    while True:
        answer = prompt(message, style="yellow").lower().strip()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        console.print("Invalid input. Please try again.", style="red")


def confirm_overwrite(key: str):
    """
    Check whether a key should be overwritten if a duplicate is detected.
    """
    message = f"{key!r} already exists; do you wish to overwrite it? (y/n)"
    while True:
        answer = prompt(message, style="yellow").lower().strip()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        console.print("Invalid input. Please try again.", style="red")


def populate_field(key: str, field: ModelField, debug: bool = False):
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

        validated_value, error = field.validate(value, {}, loc=key)
        if not error:
            console.print(f"{key!r} successfully validated", style="bright_green")
            if debug:
                console.print(
                    f"{type(validated_value)}\n{validated_value!r}",
                    style="bright_green",
                )
            return validated_value
        else:
            console.print("Invalid input. Please try again.", style="red")


def add_calibrations(key: str, field: ModelField, debug: bool = False) -> dict:
    """
    Populate the 'calibrations' field with dictionaries.
    """

    def get_calibration():
        # Request for a file to read settings from
        calibration_file = Path(
            prompt(
                "What is the full file path to the calibration file? This should be a "
                "JSON file.",
                style="yellow",
            )
        )
        try:
            with open(calibration_file, "r") as file:
                calibration_values: dict = json.load(file)
                return calibration_values
        except Exception as e:
            console.print(
                f"Error opening the provided file: {e}",
                style="red",
            )
            if ask_for_input("calibration file", True) is True:
                return get_calibration()
            else:
                return {}

    # Settings
    known_calibraions = ("magnification",)

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
        if calibration_type not in known_calibraions:
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
        calibration_values = get_calibration()
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
        add_calibration = ask_for_input(category="calibration setting", again=True)

    # Validate the nested dictionary structure
    validated_calibrations, error = field.validate(calibrations, {}, loc=field)
    if not error:
        console.print(f"{key!r} validated successfully", style="bright_green")
        if debug:
            console.print(
                f"{type(validated_calibrations)}\n{validated_calibrations!r}",
                style="bright_green",
            )
        return validated_calibrations
    else:
        console.print(
            f"Failed to validate the provided calibrations: {error}", style="red"
        )
        console.print("Returning an empty dictionary", style="red")
        return {}


def add_software_packages(config: dict, debug: bool = False):
    def ask_about_xml_path() -> bool:
        message = (
            "Does this software package have a settings file that needs modification? "
            "(y/n)"
        )
        answer = prompt(message, style="yellow").lower().strip()

        # Validate
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        console.print("Invalid input.", style="red")
        return ask_about_xml_path()

    def get_software_name() -> str:
        name = (
            prompt(
                "What is the name of the software package? Supported options: 'autotem', "
                "'epu', 'leica', 'serialem', 'tomo'",
            )
            .lower()
            .strip()
        )
        # Validate name against "acquisition_software" field
        field = MachineConfig.__fields__["acquisition_software"]
        validated_name, error = field.validate([name], {}, loc="acquisition_software")
        if not error:
            return validated_name[0]
        console.print(
            "Invalid software name.",
            style="red",
        )
        if ask_for_input("software package", True) is True:
            return get_software_name()
        return ""

    def get_xml_file() -> Optional[Path]:
        xml_file = Path(
            prompt(
                "What is the full file path of the settings file? This should be an "
                "XML file.",
                style="yellow",
            )
        )
        # Validate
        if xml_file.suffix:
            return xml_file
        console.print(
            "The path entered does not point to a file.",
            style="red",
        )
        if ask_for_input("settings file", True) is True:
            return get_xml_file()
        return None

    def get_xml_tree_path() -> str:
        xml_tree_path = prompt(
            "What is the path through the XML file to the node to overwrite?",
            style="yellow",
        )
        # Possibly some validation checks later
        return xml_tree_path

    def get_extensions_and_substrings() -> dict[str, list[str]]:
        def get_file_extension() -> str:
            extension = prompt(
                "Please enter the extension of a file produced by this package "
                "that is to be analysed (e.g., '.tiff', '.eer', etc.).",
                style="yellow",
            ).strip()
            # Validate
            if not (extension.startswith(".") and extension.replace(".", "").isalnum()):
                console.print(
                    "This is an invalid file extension. Please try again. ",
                    style="red",
                )
                return get_file_extension()
            if extension in unsorted_dict.keys():
                console.print("This extension has already been provided")
                return ""
            return extension

        def get_file_substring() -> str:
            substring = prompt(
                "Please enter a keyword that will be present in files with this "
                "extension. This field is case-sensitive.",
                style="yellow",
            ).strip()
            # Validate
            if bool(re.fullmatch(r"[\w\s\-]*", substring)) is False:
                console.print(
                    "Invalid characters are present in this substring. Please "
                    "try again. ",
                    style="red",
                )
                return get_file_substring()
            if substring in substrings:
                console.print("This substring has already been provided.")
                return ""
            return substring

        # Start of get_extensions_and_substrings
        unsorted_dict: dict = {}
        add_extension = ask_for_input("file extension", False)
        while add_extension is True:
            extension = get_file_extension()
            if not extension:
                add_extension = ask_for_input("file extension", True)
                continue
            substrings: list[str] = []
            add_substring = ask_for_input("file substring", False)
            while add_substring is True:
                substring = get_file_substring()
                if not substring:
                    add_substring = ask_for_input("file substring", True)
                    continue
                substrings.append(substring)
                add_substring = ask_for_input("file substring", True)
            unsorted_dict[extension] = sorted(substrings)
            add_extension = ask_for_input("file extension", True)

        sorted_dict: dict = {}
        for key in sorted(unsorted_dict.keys()):
            sorted_dict[key] = unsorted_dict[key]
        return sorted_dict

    # Start of add_software_packages
    console.print("acquisition_software", style="bold bright_cyan")
    console.print(
        "This is where aquisition software packages present on the instrument "
        "machine can be set.",
        style="bright_cyan",
    )
    console.print(
        "Options: 'epu', 'tomo', 'serialem', 'autotem', 'leica'",
        style="bright_cyan",
    )
    package_info: dict = {}
    category = "software package"
    add_input = ask_for_input(category, again=False)
    while add_input:
        # Collect inputs
        console.print("acquisition_software", style="bold bright_cyan")
        name = get_software_name()
        if name in package_info.keys():
            if confirm_overwrite(name) is False:
                add_input = ask_for_input(category, False)
                continue

        version = prompt(
            "What is the version number of this software package? Press Enter to leave "
            "it blank if you're unsure.",
            style="yellow",
        )

        console.print("software_settings_output_directories", style="bold bright_cyan")
        console.print(
            "Some software packages will have settings files that require modification "
            "in order to ensure files are saved to the desired folders.",
            style="bright_cyan",
        )
        if ask_about_xml_path() is True:
            xml_file = get_xml_file()
            xml_tree_path = get_xml_tree_path()
        else:
            xml_file = None
            xml_tree_path = ""

        console.print("data_required_substrings", style="bold bright_cyan")
        console.print(
            "Different software packages will generate different output files. Only "
            "files with certain extensions and keywords in their filenames are needed "
            "for data processing. They are listed out here.",
            style="bright_cyan",
        )
        file_ext_ss = get_extensions_and_substrings()

        # Compile keys for this package as a dict
        package_info[name] = {
            "version": version,
            "xml_file": xml_file,
            "xml_tree_path": xml_tree_path,
            "extensions_and_substrings": file_ext_ss,
        }
        add_input = ask_for_input(category, again=True)

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
        if package_info[key]["xml_file"]:
            software_settings_output_directories[str(package_info[key]["xml_file"])] = (
                package_info[key]["xml_tree_path"]
            )
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
        field = MachineConfig.__fields__[field_name]
        validated_value, error = field.validate(value, {}, loc=field_name)
        if not error:
            config[field_name] = validated_value
            console.print(
                f"{field_name!r} validated successfully", style="bright_green"
            )
            if debug:
                console.print(
                    f"{type(validated_value)}\n{validated_value!r}",
                    style="bright_green",
                )
        else:
            console.print(
                f"Validation failed due to the following error: {error}",
                style="red",
            )
            console.print("Please try again.", style="red")
            return add_software_packages(config)

    # Return updated dictionary
    return config


def set_up_data_transfer(config: dict, debug: bool = False) -> dict:
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
            # TODO
            continue
        if key == "create_directories":
            # TODO
            continue
        if key == "analyse_created_directories":
            # TODO
            continue

        # Data transfer block
        if key == "data_transfer_enabled":
            # TODO: Set up data transfer settings in a separate function
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
