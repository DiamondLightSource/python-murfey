from pathlib import Path
from typing import Optional
from unittest.mock import ANY, Mock, patch
from urllib.parse import urlparse

from pytest import mark

from murfey.instrument_server.api import (
    GainReference,
    _get_murfey_url,
    upload_gain_reference,
)
from murfey.util import posix_path
from murfey.util.api import url_path_for

test_get_murfey_url_params_matrix = (
    # Server URL to use
    ("default",),
    ("0.0.0.0:8000",),
    ("murfey_server",),
    ("http://murfey_server:8000",),
    ("http://murfey_server:8080/api",),
)


@mark.parametrize("test_params", test_get_murfey_url_params_matrix)
def test_get_murfey_url(
    test_params: tuple[str],
    mock_client_configuration,  # From conftest.py
):
    # Unpack test_params
    (server_url_to_test,) = test_params

    # Replace the server URL from the fixture with other ones for testing
    if server_url_to_test != "default":
        mock_client_configuration["Murfey"]["server"] = server_url_to_test

    # Mock the module-wide config variable with the fixture value
    # The fixture is only loaded within the test function, so this patch
    # has to happen inside the function instead of as a decorator
    with patch("murfey.instrument_server.api.config", mock_client_configuration):
        known_server = _get_murfey_url()

    # Prepend 'http://' to config URLs that don't have it for the comparison
    # Otherwise, urlparse stores it under the 'path' attribute
    original_url = str(mock_client_configuration["Murfey"].get("server"))
    if not original_url.startswith(("http://", "https://")):
        original_url = f"http://{original_url}"

    # Check that the components of the result match those in the config
    parsed_original = urlparse(original_url)
    parsed_server = urlparse(known_server)
    assert parsed_server.scheme in ("http", "https")
    assert parsed_server.hostname == parsed_original.hostname
    assert parsed_server.port == parsed_original.port
    assert parsed_server.netloc == parsed_original.netloc
    assert parsed_server.path == parsed_original.path


test_upload_gain_reference_params_matrix = (
    # Rsync URL settings
    ("http://1.1.1.1",),  # When rsync_url is provided
    ("",),  # When rsync_url is blank
    (None,),  # When rsync_url not provided
)


@mark.parametrize("test_params", test_upload_gain_reference_params_matrix)
@patch("murfey.instrument_server.api.subprocess")
@patch("murfey.instrument_server.api.tokens")
@patch("murfey.instrument_server.api._get_murfey_url")
@patch("murfey.instrument_server.api.requests")
def test_upload_gain_reference(
    mock_request,
    mock_get_server_url,
    mock_tokens,
    mock_subprocess,
    test_params: tuple[Optional[str]],
):

    # Unpack test parameters and define other ones
    (rsync_url_setting,) = test_params
    server_url = "http://0.0.0.0:8000"
    instrument_name = "murfey"
    session_id = 1

    # Create a mock machine config base on the test params
    rsync_module = "data"
    gain_ref_dir = "C:/ProgramData/Gatan/Gain Reference"
    mock_machine_config = {
        "rsync_module": rsync_module,
        "gain_reference_directory": gain_ref_dir,
    }
    if rsync_url_setting is not None:
        mock_machine_config["rsync_url"] = rsync_url_setting

    # Assign expected values to the mock objects
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_machine_config
    mock_request.get.return_value = mock_response
    mock_get_server_url.return_value = server_url
    mock_subprocess.run.return_value = Mock(returncode=0)

    # Construct payload and pass request to function
    gain_ref_file = f"{gain_ref_dir}/gain.mrc"
    visit_path = "2025/aa00000-0"
    gain_dest_dir = "processing"
    payload = {
        "gain_path": gain_ref_file,
        "visit_path": visit_path,
        "gain_destination_dir": gain_dest_dir,
    }
    result = upload_gain_reference(
        instrument_name=instrument_name,
        session_id=session_id,
        gain_reference=GainReference(
            **payload,
        ),
    )

    # Check that the machine config request was called
    machine_config_url = f"{server_url}{url_path_for('session_control.router', 'machine_info_by_instrument', instrument_name=instrument_name)}"
    mock_request.get.assert_called_once_with(
        machine_config_url,
        headers={"Authorization": ANY},
    )

    # Check that the subprocess was run with the expected arguments
    # If no rsync_url key is provided, or rsync_url key is empty,
    # It should default to the server URL
    expected_rsync_url = (
        urlparse(server_url) if not rsync_url_setting else urlparse(rsync_url_setting)
    )
    expected_rsync_path = f"{expected_rsync_url.hostname}::{rsync_module}/{visit_path}/{gain_dest_dir}/gain.mrc"
    expected_rsync_cmd = [
        "rsync",
        posix_path(Path(gain_ref_file)),
        expected_rsync_path,
    ]
    mock_subprocess.run.assert_called_once_with(
        expected_rsync_cmd,
        capture_output=True,
        text=True,
    )

    # Check that the function ran through to completion successfully
    assert result == {"success": True}
