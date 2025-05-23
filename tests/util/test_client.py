import json
import os
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import urlparse

from pytest import mark

from murfey.client import _get_visit_list
from murfey.util.client import read_config, set_default_acquisition_output
from murfey.util.models import Visit

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


@mark.parametrize("test_params", test_read_config_params_matrix)
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
    with patch.dict(os.environ, env_var_dict, clear=False):
        config = read_config()

    # Compare returned config with mock one
    assert dict(config["Murfey"]) == dict(mock_client_configuration["Murfey"])


test_get_visit_list_params_matrix = (
    ("http://0.0.0.0:8000",),
    ("http://0.0.0.0:8000/api",),
    ("http://murfey_server",),
    ("http://murfey_server/api",),
    ("http://murfey_server.com",),
)


@mark.parametrize("test_params", test_get_visit_list_params_matrix)
@patch("murfey.client.requests")
def test_get_visit_list(
    mock_request,
    test_params: tuple[str],
    mock_client_configuration,
):
    # Unpack test params and set up other params
    (server_url,) = test_params
    instrument_name = mock_client_configuration["Murfey"]["instrument_name"]

    # Construct the expected request response
    example_visits = [
        {
            "start": "1999-09-09T09:00:00",
            "end": "1999-09-11T09:00:00",
            "session_id": 123456789,
            "name": "cm12345-0",
            "beamline": "murfey",
            "proposal_title": "Commissioning Session 1",
        },
        {
            "start": "1999-09-09T09:00:00",
            "end": "1999-09-11T09:00:00",
            "session_id": 246913578,
            "name": "cm23456-1",
            "beamline": "murfey",
            "proposal_title": "Cryo-cycle 1999",
        },
    ]
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = example_visits
    mock_request.get.return_value = mock_response

    # read_config() has to be patched using fixture, so has to be done in function
    with patch("murfey.util.client.read_config", mock_client_configuration):
        visits = _get_visit_list(urlparse(server_url), instrument_name)

    # Check that request was sent with the correct URL
    expected_url = (
        f"{server_url}/session_control/instruments/{instrument_name}/visits_raw"
    )
    mock_request.get.assert_called_once_with(expected_url)

    # Check that expected outputs are correct (order-sensitive)
    for v, visit in enumerate(visits):
        assert visit.dict() == Visit.parse_obj(example_visits[v]).dict()


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
