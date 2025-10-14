from pathlib import Path
from typing import Optional
from unittest.mock import ANY, MagicMock, patch
from urllib.parse import urlparse

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from murfey.instrument_server.api import (
    _get_murfey_url,
    router as client_router,
    validate_session_token,
)
from murfey.util import posix_path
from murfey.util.api import url_path_for


def set_up_test_client(session_id: Optional[int] = None):
    """
    Helper function to set up a test client for the instrument server with validation
    checks disabled.
    """
    # Set up the instrument server
    client_app = FastAPI()
    if session_id:
        client_app.dependency_overrides[validate_session_token] = lambda: session_id
    client_app.include_router(client_router)
    return TestClient(client_app)


test_get_murfey_url_params_matrix = (
    # Server URL to use
    ("default",),
    ("0.0.0.0:8000",),
    ("murfey_server",),
    ("http://murfey_server:8000",),
    ("http://murfey_server:8080/api",),
)


@pytest.mark.parametrize("test_params", test_get_murfey_url_params_matrix)
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


def test_check_multigrid_controller_status(mocker: MockerFixture):
    session_id = 1

    # Patch out the multigrid controllers that have been stored in memory
    mock_controller = MagicMock()
    mock_controller.dormant = False
    mock_controller.finalising = False
    mocker.patch(
        "murfey.instrument_server.api.controllers", {session_id: mock_controller}
    )

    # Set up the test client
    client_server = set_up_test_client(session_id=session_id)
    url_path = url_path_for(
        "api.router", "check_multigrid_controller_status", session_id=session_id
    )
    response = client_server.get(url_path)

    # Check that the result is as expected
    assert response.status_code == 200
    assert response.json() == {
        "dormant": False,
        "exists": True,
        "finalising": False,
    }


test_upload_gain_reference_params_matrix = (
    # Rsync URL settings
    ("http://1.1.1.1",),  # When rsync_url is provided
    ("",),  # When rsync_url is blank
    (None,),  # When rsync_url not provided
)


@pytest.mark.parametrize("test_params", test_upload_gain_reference_params_matrix)
def test_upload_gain_reference(
    mocker: MockerFixture,
    test_params: tuple[Optional[str]],
):
    # Unpack test parameters and define other ones
    (rsync_url_setting,) = test_params
    server_url = "https://murfey.server.test"
    instrument_name = "murfey"
    session_id = 1

    # Mock out objects
    mock_request = mocker.patch("murfey.instrument_server.api.requests")
    mock_get_server_url = mocker.patch("murfey.instrument_server.api._get_murfey_url")
    mock_subprocess = mocker.patch("murfey.instrument_server.api.subprocess")
    mocker.patch("murfey.instrument_server.api.tokens", {session_id: ANY})

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
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_machine_config
    mock_request.get.return_value = mock_response
    mock_get_server_url.return_value = server_url
    mock_subprocess.run.return_value = MagicMock(returncode=0)

    # Construct payload and pass request to function
    gain_ref_file = f"{gain_ref_dir}/gain.mrc"
    visit_path = "2025/aa00000-0"
    gain_dest_dir = "processing"
    payload = {
        "gain_path": gain_ref_file,
        "visit_path": visit_path,
        "gain_destination_dir": gain_dest_dir,
    }

    # Set up instrument server test client
    client_server = set_up_test_client(session_id=session_id)

    # Poke the endpoint with the expected data
    url_path = url_path_for(
        "api.router",
        "upload_gain_reference",
        instrument_name=instrument_name,
        session_id=session_id,
    )
    response = client_server.post(url_path, json=payload)

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
    assert response.json() == {"success": True}
