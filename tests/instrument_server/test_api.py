from typing import Optional
from unittest.mock import Mock, patch
from urllib.parse import urlparse

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest import mark

from murfey.instrument_server.api import router

app = FastAPI()
app.include_router(router)
client = TestClient(app)


test_upload_gain_reference_params_matrix = (
    # Rsync URL | Rsync module | Gain reference directory |
    (
        "http://1.1.1.1",
        "data",
        "/c/ProgramData/Gatan/Gain Reference",
    ),  # When rsync_url is provided
    (
        "",
        "data",
        "/c/ProgramData/Gatan/Gain Reference",
    ),  # When rsync_url is blank
    (
        None,
        "data",
        "/c/ProgramData/Gatan/Gain Reference",
    ),  # When rsync_url not provided
)


@mark.parametrize("test_params", test_upload_gain_reference_params_matrix)
@patch("murfey.instrument_server.api.subprocess.run")
@patch("murfey.instrument_server.api.urlparse", wraps=urlparse)
@patch("murfey.instrument_server.api._get_murfey_url")
@patch("murfey.instrument_server.api.requests.get")
def test_upload_gain_reference(
    mock_get,
    mock_server_url,
    spy_parse,
    mock_run,
    test_params: tuple[Optional[str], str, str],
):

    # Create a mock machine config base on the test params
    rsync_url, rsync_module, gain_ref_dir = test_params
    server_url = "http://0.0.0.0:8000"
    mock_machine_config = {
        "rsync_module": rsync_module,
        "gain_reference_directory": gain_ref_dir,
    }
    if rsync_url is not None:
        mock_machine_config["rsync_url"] = rsync_url

    # Assign expected values to the mock objects
    mock_get.return_value = Mock(status_code=200, json=lambda: mock_machine_config)
    mock_server_url.return_value = server_url
    mock_run.return_value = Mock(returncode=0)

    # Construct payload and submit post request
    payload = {
        "gain_path": f"{gain_ref_dir}/gain.mrc",
        "visit_path": "2025/aa00000-0",
        "gain_destination_dir": "processing",
    }
    response = client.post(
        "/instruments/m02/session/1/upload_gain_reference", json=payload
    )

    # Check that the machine config request was called
    mock_get.assert_called_once()

    # If no rsync_url key is provided, or rsync_url key is empty,
    # This should default to the Murfey URL
    if not rsync_url:
        assert spy_parse.return_value == urlparse(server_url)
    else:
        assert spy_parse.return_value == urlparse(rsync_url)

    # Check that the subprocess was run
    mock_run.assert_called_once()

    # Check that the endpoint function ran through to completion successfully
    assert response.status_code == 200
    assert response.json() == {"success": True}
