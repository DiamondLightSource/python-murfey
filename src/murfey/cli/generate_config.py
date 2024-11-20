import argparse
import json
import re
from ast import literal_eval
from pathlib import Path
from typing import Any, Optional, get_type_hints

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


def construct_list(
    list_name: str,
    prompt_message: str,
    allow_empty: bool = False,
    allow_eval: bool = True,
    many_types: bool = True,
    debug: bool = False,
) -> list[Any]:
    """
    Helper function to facilitate interactive construction of a list to be stored
    under the current parameter.
    """
    lst: list = []
    add_entry = ask_for_input(list_name, False)
    message = prompt_message
    while add_entry is True:
        value = prompt(message, style="yellow").strip()
        # Reject empty inputs if set
        if not value and not allow_empty:
            console.print("No value provided.", style="red")
            add_entry = ask_for_input(list_name, True)
            continue
        # Convert numericals if set
        try:
            eval_value = (
                literal_eval(value)
                if allow_eval and isinstance(literal_eval(value), (int, float, complex))
                else value
            )
        except Exception:
            eval_value = value
        # Confirm if duplicate entry should be added
        if eval_value in lst and confirm_duplicate(str(eval_value)) is False:
            add_entry = ask_for_input(list_name, True)
            continue
        lst.append(eval_value)
        # Reject list with multiple types if set
        if not many_types and len({type(item) for item in lst}) > 1:
            console.print(
                "The provided value is of a different type to the other members. \n"
                "It won't be added to the list.",
                style="red",
            )
            lst = lst[:-1]
        add_entry = ask_for_input(list_name, True)
        continue
    return lst


def construct_dict(
    dict_name: str,
    key_name: str,
    value_name: str,
    allow_empty_key: bool = True,
    allow_empty_value: bool = True,
    allow_eval: bool = True,
    sort_keys: bool = True,
    debug: bool = False,
) -> dict[str, Any]:
    """
    Helper function to facilitate interative construction of a dictionary.
    """
    dct: dict = {}
    add_entry = ask_for_input(dict_name, False)
    key_message = f"Please enter the {key_name}"
    value_message = f"Please enter the {value_name}"
    while add_entry is True:
        key = prompt(key_message, style="yellow").strip().lower()
        # Reject empty keys if set
        if not allow_empty_key and not key:
            console.print(f"No {key_name} provided.")
            add_entry = ask_for_input(dict_name, True)
            continue
        # Confirm overwrite key on duplicate
        if key in dct.keys():
            if confirm_overwrite(key) is False:
                add_entry = ask_for_input(dict_name, True)
                continue
        value = prompt(value_message, style="yellow").strip()
        # Reject empty values if set
        if not allow_empty_value and not value:
            console.print("No value provided", style="red")
            add_entry = ask_for_input(dict_name, True)
            continue
        # Convert values to numericals if set
        try:
            eval_value = (
                literal_eval(value)
                if allow_eval and isinstance(literal_eval(value), (int, float, complex))
                else value
            )
        except Exception:
            eval_value = value
        dct[key] = eval_value
        add_entry = ask_for_input(dict_name, True)
        continue

    # Sort keys if set
    dct = {key: dct[key] for key in sorted(dct.keys())} if sort_keys else dct
    return dct


def validate_value(value: Any, key: str, field: ModelField, debug: bool = False) -> Any:
    """
    Helper function to validate the value of the desired field for a Pydantic model.
    """
    validated_value, errors = field.validate(value, {}, loc=key)
    if errors:
        raise ValidationError(errors, MachineConfig)
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
            f"{calibration_type} setting",
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
        console.print(f"Failed to validate {key!r}", style="red")
        console.print("Returning an empty dictionary", style="red")
        return {}


def add_software_packages(config: dict, debug: bool = False) -> dict[str, Any]:
    def get_software_name() -> str:
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
            return ""

    def ask_about_xml_path() -> bool:
        message = (
            "Does this software package have a settings file that needs modification? "
            "(y/n)"
        )
        while True:
            answer = prompt(message, style="yellow").lower().strip()
            # Validate
            if answer in ("y", "yes"):
                return True
            if answer in ("n", "no"):
                return False
            console.print("Invalid input.", style="red")

    def get_xml_file() -> Optional[Path]:
        message = (
            "What is the full file path of the settings file? This should be an "
            "XML file."
        )
        xml_file = Path(prompt(message, style="yellow").strip())
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
        message = "What is the path through the XML file to the node to overwrite?"
        xml_tree_path = prompt(message, style="yellow").strip()
        # TODO: Currently no test cases for this method
        return xml_tree_path

    def get_extensions_and_substrings() -> dict[str, list[str]]:
        def get_file_extension() -> str:
            message = (
                "Please enter the extension of a file produced by this package "
                "that is to be analysed (e.g., '.tiff', '.eer', etc.)."
            )
            extension = prompt(message, style="yellow").strip().lower()
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
            message = (
                "Please enter a keyword that will be present in files with this "
                "extension. This field is case-sensitive."
            )
            substring = prompt(message, style="yellow").strip()
            # Validate
            if bool(re.fullmatch(r"[\w\s\-]*", substring)) is False:
                console.print(
                    "Unsafe characters are present in this substring. Please "
                    "try again. ",
                    style="red",
                )
                return get_file_substring()
            if substring in substrings:
                console.print("This substring has already been provided.")
                return ""
            return substring

        """
        Start of get_extensions_and_substrings
        """
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
        # Collect inputs
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

        console.print(
            "Software Versions (software_versions)",
            style="bold bright_cyan",
        )
        version = prompt(
            "What is the version number of this software package? Press Enter to leave "
            "it blank if you're unsure.",
            style="yellow",
        )

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
        if ask_about_xml_path() is True:
            xml_file = get_xml_file()
            xml_tree_path = get_xml_tree_path()
        else:
            xml_file = None
            xml_tree_path = ""

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
        try:
            field = MachineConfig.__fields__[field_name]
            config[field_name] = validate_value(value, field_name, field, debug)
        except ValidationError as error:
            if debug:
                console.print(error, style="red")
            console.print(f"Failed to validate {field_name!r}", style="red")
            console.print("Please try again.", style="red")
            return add_software_packages(config)

    # Return updated dictionary
    return config


def add_data_directories(
    key: str, field: ModelField, debug: bool = False
) -> dict[str, str]:
    def get_directory() -> Optional[Path]:
        message = "What is the full file path to the data directory you wish to add?"
        answer = prompt(message, style="yellow").strip()
        # Convert "" into None
        if not answer:
            return None
        return Path(answer)

    def get_directory_type():
        message = (
            "What type of data is stored in this directory? Options: 'microscope', "
            "'detector'"
        )
        answer = prompt(message, style="yellow").lower().strip()
        if answer not in ("microscope", "detector"):
            console.print("Invalid directory type.", style="red")
            if ask_for_input("directory type", True) is True:
                return get_directory_type()
            return ""
        return answer

    """
    Start of add_data_directories
    """
    print_field_info(field)
    data_directories: dict[str, str] = {}
    category = "data directory"
    add_directory = ask_for_input(category, False)
    while add_directory is True:
        directory = get_directory()
        # Move on to next loop or exit if no directory provided
        if not directory:
            console.print("No directory added", style="red")
            add_directory = ask_for_input(category, True)
            continue

        # Get the directory type
        directory_type = get_directory_type()
        if not directory_type:
            console.print("No directory type provided", style="red")

        # Add to dictionary
        data_directories[str(directory)] = directory_type

        # Check if more need to be added
        add_directory = ask_for_input(category, True)
        continue

    # Validate and return
    try:
        return validate_value(data_directories, key, field, debug)
    except ValidationError as error:
        if debug:
            console.print(error, style="red")
        console.print(f"Failed to validate {key!r}", style="red")
        if ask_for_input(category, True) is True:
            return add_data_directories(key, field, debug)
        return {}


def add_create_directories(
    key: str, field: ModelField, debug: bool = False
) -> dict[str, str]:
    def get_folder() -> str:
        message = "Please enter the name of the folder for Murfey to create."
        answer = prompt(message, style="yellow").lower().strip()
        if bool(re.fullmatch(r"[\w\s\-]*", answer)) is False:
            console.print(
                "There are unsafe characters present in this folder name. Please "
                "use a different one.",
                style="red",
            )
            if ask_for_input("folder name", True) is True:
                return get_folder()
            return ""
        return answer

    def get_folder_alias() -> str:
        message = "Please enter the name Murfey should map this folder to."
        answer = prompt(message, style="yellow").lower().strip()
        if bool(re.fullmatch(r"[\w\s\-]*", answer)) is False:
            console.print(
                "There are unsafe characters present in this folder name. Please "
                "use a different one.",
                style="red",
            )
            if ask_for_input("folder alias", True) is True:
                return get_folder_alias()
            return ""
        return answer

    """
    Start of add_create_directories
    """
    print_field_info(field)
    folders_to_create: dict[str, str] = {}
    category = "folder for Murfey to create"
    add_directory: bool = ask_for_input(category, False)
    while add_directory is True:
        folder_name = get_folder()
        if not folder_name:
            console.print(
                "No folder name provided",
                style="red",
            )
            add_directory = ask_for_input(category, True)
            continue
        folder_alias = get_folder_alias()
        if not folder_alias:
            console.print(
                "No folder alias provided",
                style="red",
            )
            add_directory = ask_for_input(category, True)
            continue
        folders_to_create[folder_alias] = folder_name
        add_directory = ask_for_input(category, True)
        continue

    # Validate and return
    try:
        return validate_value(folders_to_create, key, field, debug)
    except ValidationError as error:
        if debug:
            console.print(error, style="red")
        console.print(f"Failed to validate {key!r}", style="red")
        if ask_for_input(category, True) is True:
            return add_create_directories(key, field, debug)
        return {}


def add_analyse_created_directories(
    key: str, field: ModelField, debug: bool = False
) -> list[str]:
    def get_folder() -> str:
        message = "Please enter the name of the folder that Murfey is to analyse."
        answer = prompt(message, style="yellow").lower().strip()
        if bool(re.fullmatch(r"[\w\s\-]*", answer)) is False:
            console.print(
                "There are unsafe characters present in the folder name. Please "
                "use a different folder.",
                style="red",
            )
            if ask_for_input("folder name", True) is True:
                return get_folder()
            return ""
        return answer

    """
    Start of add_analyse_created_directories
    """
    folders_to_analyse: list[str] = []
    category = "folder for Murfey to analyse"
    add_folder = ask_for_input(category, False)
    while add_folder is True:
        folder_name = get_folder()
        if not folder_name:
            console.print("No folder name provided", style="red")
            add_folder = ask_for_input(category, True)
            continue
        folders_to_analyse.append(folder_name)
        add_folder = ask_for_input(category, True)
        continue

    # Validate and return
    try:
        return sorted(validate_value(folders_to_analyse, key, field, debug))
    except ValidationError as error:
        if debug:
            console.print(error, style="red")
        console.print(f"Failed to validate {key!r}", style="red")
        if ask_for_input(category, True) is True:
            return add_analyse_created_directories(key, field, debug)
        return []


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
            new_config[key] = add_data_directories(key, field, debug)
            continue
        if key == "create_directories":
            new_config[key] = add_create_directories(key, field, debug)
            continue
        if key == "analyse_created_directories":
            new_config[key] = add_analyse_created_directories(key, field, debug)
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
