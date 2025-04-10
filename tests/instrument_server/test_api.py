from pathlib import Path
from typing import Optional
from unittest.mock import Mock, patch
from urllib.parse import urlparse

from pytest import mark

from murfey.instrument_server.api import GainReference, upload_gain_reference
from murfey.util import posix_path

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
    mock_subprocess.run.return_value = Mock(
        returncode=0, stderr="An error has occurred."
    )
    mock_tokens = {
        session_id: "hello",
    }
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
    machine_config_url = f"{server_url}/instruments/{instrument_name}/machine"
    mock_request.get.assert_called_once_with(
        machine_config_url,
        headers={"Authorization": f"Bearer {mock_tokens[session_id]}"},
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
