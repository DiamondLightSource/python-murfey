import json
import os
from pathlib import Path
from unittest import mock

import pytest

from murfey.util.client import read_config, set_default_acquisition_output

test_read_config_params_matrix = (
    # Environment variable to set | Append to tmp_path
    (
        "MURFEY_CLIENT_CONFIGURATION",
        "config/murfey-client-config.cfg",
    ),
    (
        "MURFEY_CLIENT_CONFIG_HOME",
        "config",
    ),
    (
        "",
        "",
    ),  # Test default home directory
)


@pytest.mark.parametrize("test_params", test_read_config_params_matrix)
def test_read_config(
    test_params: tuple[str, str],
    tmp_path,
    mock_client_configuration,
):
    # Unpack test params
    env_var, partial_path = test_params

    # Construct the environment variable and the expected config file path
    env_var_dict: dict[str, str] = {}
    if env_var:
        full_path = tmp_path / partial_path
        env_var_dict[env_var] = str(full_path)
        file_path = full_path if full_path.suffix else full_path / ".murfey"
    else:
        file_path = Path().home() / ".murfey"

    # Make directories all the way to the requested place
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Write the client config fixture to the specified file
    with open(file_path, "w") as file:
        mock_client_configuration.write(file)

    # Patch the OS environment variable and run the function
    with mock.patch.dict(os.environ, env_var_dict, clear=False):
        config = read_config()

    # Compare returned config with mock one
    assert dict(config["Murfey"]) == dict(mock_client_configuration["Murfey"])


def test_set_default_acquisition_output_normal_operation(tmp_path):
    output_dir = tmp_path / "settings.json"
    settings_json = {
        "a": {
            "b": {"data_dir": str(tmp_path)},
            "c": {
                "d": 1,
            },
        }
    }
    with open(output_dir, "w") as sf:
        json.dump(settings_json, sf)
    set_default_acquisition_output(
        tmp_path / "visit", {str(tmp_path / "settings.json"): ["a", "b", "data_dir"]}
    )
    assert (tmp_path / "_murfey_settings.json").is_file()
    with open(output_dir, "r") as sf:
        data = json.load(sf)
    assert data["a"]["b"]["data_dir"] == str(tmp_path / "visit")
    assert data["a"]["c"]["d"] == 1
    with open(output_dir.parent / "_murfey_settings.json", "r") as sf:
        data = json.load(sf)
    assert data["a"]["b"]["data_dir"] == str(tmp_path)
    assert data["a"]["c"]["d"] == 1


def test_set_default_acquisition_output_no_file_copy(tmp_path):
    output_dir = tmp_path / "settings.json"
    settings_json = {
        "a": {
            "b": {"data_dir": str(tmp_path)},
            "c": {
                "d": 1,
            },
        }
    }
    with open(output_dir, "w") as sf:
        json.dump(settings_json, sf)
    set_default_acquisition_output(
        tmp_path / "visit",
        {str(tmp_path / "settings.json"): ["a", "b", "data_dir"]},
        safe=False,
    )
    assert not (tmp_path / "_murfey_settings.json").is_file()
    with open(output_dir, "r") as sf:
        data = json.load(sf)
    assert data["a"]["b"]["data_dir"] == str(tmp_path / "visit")
    assert data["a"]["c"]["d"] == 1
