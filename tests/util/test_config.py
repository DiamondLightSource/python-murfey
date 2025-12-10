from pathlib import Path
from typing import Any

import pytest
import yaml
from pytest_mock import MockerFixture

from murfey.util.config import Settings, get_machine_config


@pytest.fixture
def mock_general_config():
    # Most extra keys go in this category
    return {
        "pkg_2": {
            "url": "https://some-url.some.org",
            "token": "pneumonoultrasmicroscopicsilicovolcanoconiosis",
        }
    }


@pytest.fixture
def mock_tem_shared_config():
    return {
        # Hardware and software
        "acquisition_software": ["epu", "tomo", "serialem"],
        "software_versions": {"tomo": "5.12"},
        "data_required_substrings": {
            "epu": {
                ".mrc": ["fractions", "Fractions"],
                ".tiff": ["fractions", "Fractions"],
                ".eer": ["EER"],
            },
            "tomo": {
                ".mrc": ["fractions", "Fractions"],
                ".tiff": ["fractions", "Fractions"],
                ".eer": ["EER"],
            },
        },
        # Client directory setup
        "analyse_created_directories": ["atlas"],
        "gain_reference_directory": "C:/ProgramData/Gatan/Reference Images/",
        # Data transfer keys
        "data_transfer_enabled": True,
        "substrings_blacklist": {
            "directories": ["some_str"],
            "files": ["some_str"],
        },
        "rsync_module": "rsync",
        "allow_removal": True,
        "upstream_data_directories": {
            "upstream_instrument": "/path/to/upstream_instrument",
        },
        "upstream_data_download_directory": "/path/to/download/directory",
        "upstream_data_search_strings": {
            "upstream_instrument": ["some_string"],
        },
        # Data processing keys
        "processing_enabled": True,
        "gain_directory_name": "some_directory",
        "processed_directory_name": "some_directory",
        "processed_extra_directory": "some_directory",
        "recipes": {
            "recipe_1": "recipe_1",
            "recipe_2": "recipe_2",
        },
        "default_model": "some_file",
        "external_executables": {
            "app_1": "/path/to/app_1",
            "app_2": "/path/to/app_2",
            "app_3": "/path/to/app_3",
        },
        "external_executables_eer": {
            "app_1": "/path/to/app_1",
            "app_2": "/path/to/app_2",
            "app_3": "/path/to/app_3",
        },
        "external_environment": {
            "ENV_1": "/path/to/env_1",
            "ENV_2": "/path/to/env_2",
        },
        "plugin_packages": {
            "pkg_1": "/path/to/pkg_1",
            "pkg_2": "/path/to/pkg_2",
        },
        # Extra keys
        "pkg_1": {
            "file_path": "",
            "command": [
                "/path/to/executable",
                "--some_arg",
                "-a",
                "./path/to/file",
            ],
            "step_size": 100,
        },
    }


@pytest.fixture
def mock_instrument_config():
    return {
        # Extra key to point to hierarchical dictionary to use
        "instrument_type": "tem",
        # General information
        "display_name": "Some TEM",
        "image_path": "/path/to/tem.jpg",
        # Hardware and software
        "camera": "Some camera",
        "superres": True,
        "calibrations": {
            "magnification": {
                100: 0.1,
                200: 0.05,
                400: 0.025,
            },
        },
        # Client directory setup
        "data_directories": ["C:"],
        # Data transfer keys
        "rsync_basepath": "/path/to/data",
        "rsync_url": "http://123.45.678.90:8000",
        # Server and network keys
        "security_configuration_path": "/path/to/security-config.yaml",
        "murfey_url": "https://www.murfey.com",
        "instrument_server_url": "http://10.123.4.5:8000",
        "node_creator_queue": "node_creator",
        # Extra keys
        "pkg_1": {
            "file_path": "/path/to/pkg_1/file.txt",
        },
    }


@pytest.fixture
def mock_hierarchical_machine_config_yaml(
    mock_general_config: dict[str, Any],
    mock_tem_shared_config: dict[str, Any],
    mock_instrument_config: dict[str, Any],
    tmp_path: Path,
):
    # Create machine config (with all currently supported keys) for the instrument
    hierarchical_config = {
        "general": mock_general_config,
        "tem": mock_tem_shared_config,
        "m01": mock_instrument_config,
        "m02": mock_instrument_config,
    }
    config_file = tmp_path / "config" / "murfey-machine-config-hierarchical.yaml"
    config_file.parent.mkdir(parents=True, exist_ok=True)
    with open(config_file, "w") as file:
        yaml.safe_dump(hierarchical_config, file, indent=2)
    return config_file


@pytest.fixture
def mock_standard_machine_config_yaml(
    mock_general_config: dict[str, Any],
    mock_tem_shared_config: dict[str, Any],
    mock_instrument_config: dict[str, Any],
    tmp_path: Path,
):
    # Compile the different dictionaries into one dictionary for the instrument
    machine_config = {
        key: value
        for config in (
            mock_general_config,
            mock_tem_shared_config,
            mock_instrument_config,
        )
        for key, value in config.items()
    }

    # Correct nested dicts that would have been partially overwritten
    machine_config["pkg_1"] = {
        "file_path": "/path/to/pkg_1/file.txt",
        "command": [
            "/path/to/executable",
            "--some_arg",
            "-a",
            "./path/to/file",
        ],
        "step_size": 100,
    }

    # Remove 'instrument_type' value (not needed in standard config)
    machine_config["instrument_type"] = ""

    master_config = {
        "m01": machine_config,
        "m02": machine_config,
    }
    config_file = tmp_path / "config" / "murfey-machine-config-standard.yaml"
    config_file.parent.mkdir(parents=True, exist_ok=True)
    with open(config_file, "w") as file:
        yaml.safe_dump(master_config, file, indent=2)
    return config_file


get_machine_config_test_matrix: tuple[tuple[str, list[str]], ...] = (
    # Config to test | Instrument names to pass to function
    ("hierarchical", ["", "m01", "m02"]),
    ("standard", ["", "m01", "m02"]),
)


@pytest.mark.parametrize("test_params", get_machine_config_test_matrix)
def test_get_machine_config(
    mocker: MockerFixture,
    mock_general_config: dict[str, Any],
    mock_tem_shared_config: dict[str, Any],
    mock_instrument_config: dict[str, Any],
    mock_hierarchical_machine_config_yaml: Path,
    mock_standard_machine_config_yaml: Path,
    test_params: tuple[str, list[str]],
):
    # Unpack test params
    config_to_test, instrument_names = test_params

    # Set up mocks
    mock_settings = mocker.patch("murfey.util.config.settings", spec=Settings)

    # Run 'get_machine_config' using different instrument name parameters
    for i in instrument_names:
        # Patch the 'settings' environment variable with the YAML file to test
        mock_settings.murfey_machine_configuration = (
            str(mock_hierarchical_machine_config_yaml)
            if config_to_test == "hierarchical"
            else str(mock_standard_machine_config_yaml)
        )
        # Run the function
        config = get_machine_config(i)

        # Validate that the config was loaded correctly
        assert config

        # Multiple configs should be returned if instrument name was ""
        assert len(config) == 2 if i == "" else len(config) == 1

        # When getting the config for individual microscopes, validate key-by-key
        if i != "":
            # General info
            assert config[i].display_name == mock_instrument_config["display_name"]
            assert config[i].image_path == Path(mock_instrument_config["image_path"])
            assert (
                config[i].instrument_type == mock_instrument_config["instrument_type"]
                if config_to_test == "hierarchical"
                else not config[i].instrument_type
            )
            # Hardware & software
            assert config[i].camera == mock_instrument_config["camera"]
            assert config[i].superres == mock_instrument_config["superres"]
            assert config[i].calibrations == mock_instrument_config["calibrations"]
            assert (
                config[i].acquisition_software
                == mock_tem_shared_config["acquisition_software"]
            )
            assert (
                config[i].software_versions
                == mock_tem_shared_config["software_versions"]
            )
            assert (
                config[i].data_required_substrings
                == mock_tem_shared_config["data_required_substrings"]
            )
            # Client directory setup
            assert config[i].data_directories == [
                Path(p) for p in mock_instrument_config["data_directories"]
            ]
            assert (
                config[i].analyse_created_directories
                == mock_tem_shared_config["analyse_created_directories"]
            )
            assert config[i].gain_reference_directory == Path(
                mock_tem_shared_config["gain_reference_directory"]
            )
            # Data transfer setup
            assert (
                config[i].data_transfer_enabled
                == mock_tem_shared_config["data_transfer_enabled"]
            )
            assert (
                config[i].substrings_blacklist
                == mock_tem_shared_config["substrings_blacklist"]
            )
            assert config[i].rsync_url == mock_instrument_config["rsync_url"]
            assert config[i].rsync_basepath == Path(
                mock_instrument_config["rsync_basepath"]
            )
            assert config[i].rsync_module == mock_tem_shared_config["rsync_module"]
            assert config[i].allow_removal == mock_tem_shared_config["allow_removal"]
            assert config[i].upstream_data_directories == {
                key: Path(value)
                for key, value in mock_tem_shared_config[
                    "upstream_data_directories"
                ].items()
            }
            assert config[i].upstream_data_download_directory == Path(
                mock_tem_shared_config["upstream_data_download_directory"]
            )
            assert (
                config[i].upstream_data_search_strings
                == mock_tem_shared_config["upstream_data_search_strings"]
            )
            # Data processing setup
            assert (
                config[i].processing_enabled
                == mock_tem_shared_config["processing_enabled"]
            )
            assert (
                config[i].gain_directory_name
                == mock_tem_shared_config["gain_directory_name"]
            )
            assert (
                config[i].processed_directory_name
                == mock_tem_shared_config["processed_directory_name"]
            )
            assert (
                config[i].processed_extra_directory
                == mock_tem_shared_config["processed_extra_directory"]
            )
            assert config[i].recipes == mock_tem_shared_config["recipes"]
            assert config[i].default_model == Path(
                mock_tem_shared_config["default_model"]
            )
            assert (
                config[i].external_executables
                == mock_tem_shared_config["external_executables"]
            )
            assert (
                config[i].external_executables_eer
                == mock_tem_shared_config["external_executables_eer"]
            )
            assert (
                config[i].external_environment
                == mock_tem_shared_config["external_environment"]
            )
            assert config[i].plugin_packages == {
                key: Path(value)
                for key, value in mock_tem_shared_config["plugin_packages"].items()
            }
            # Server and network setup
            assert config[i].security_configuration_path == Path(
                mock_instrument_config["security_configuration_path"]
            )
            assert config[i].murfey_url == mock_instrument_config["murfey_url"]
            assert (
                config[i].instrument_server_url
                == mock_instrument_config["instrument_server_url"]
            )
            assert (
                config[i].node_creator_queue
                == mock_instrument_config["node_creator_queue"]
            )
            # Extra keys
            assert config[i].pkg_1 == {
                "file_path": "/path/to/pkg_1/file.txt",
                "command": [
                    "/path/to/executable",
                    "--some_arg",
                    "-a",
                    "./path/to/file",
                ],
                "step_size": 100,
            }
            assert config[i].pkg_2 == mock_general_config["pkg_2"]
