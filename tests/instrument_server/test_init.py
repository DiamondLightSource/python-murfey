import sys
from typing import Optional
from urllib.parse import urlparse

import pytest
import uvicorn
from fastapi import FastAPI
from fastapi.testclient import TestClient
from packaging.version import Version
from pytest_mock import MockerFixture

import murfey
from murfey.client.update import UPDATE_SUCCESS
from murfey.instrument_server import check_for_updates, start_instrument_server
from murfey.server.api.bootstrap import pypi as pypi_router
from murfey.server.api.bootstrap import version as version_router
from murfey.util.api import url_path_for

# Set up a test router with only the essential endpoints
app = FastAPI()
for router in [pypi_router, version_router]:
    app.include_router(router)
client = TestClient(app)
base_url = str(client.base_url)


check_for_updates_test_matrix = (
    # Downgrade, upgrade, or keep client version?
    ("downgrade",),
    ("upgrade",),
    ("keep",),
)


@pytest.mark.parametrize("test_params", check_for_updates_test_matrix)
def test_check_for_updates(
    test_params: tuple[str],
    mocker: MockerFixture,
):

    # Unpack test params
    (bump_client_version,) = test_params

    # Modify client version as needed
    current_version = murfey.__version__
    supported_client_version = murfey.__supported_client_version__

    major, minor, patch = Version(current_version).release

    # Adjust the perceived client version in the function being tested
    if bump_client_version == "downgrade":
        support_client_version_parts = Version(supported_client_version).release
        if patch == 0:
            if minor == 0:
                if major == 0:
                    # This can't be downgraded, so skip
                    pytest.skip("This version can't be downgraded anymore; skipping")
                else:
                    major = support_client_version_parts[0] - 1
                    print(f"Downgraded major version to {major}")
            else:
                minor = support_client_version_parts[1] - 1
                print(f"Downgraded minor version to {minor}")
        else:
            patch = support_client_version_parts[2] - 1
            print(f"Downgraded patch version to {patch}")
    elif bump_client_version == "upgrade":
        patch += 1
        print(f"Bumped patch version to {patch}")
    mock_client_version = f"{major}.{minor}.{patch}"

    # Run the version check query and get a response to patch in later
    api_base = urlparse(base_url, allow_fragments=False)
    proxy_path = api_base.path.rstrip("/")
    version_check_path = url_path_for("bootstrap.version", "get_version")
    version_check_query = f"client_version={mock_client_version}"
    version_check_url = api_base._replace(
        path=f"{proxy_path}{version_check_path}",
        query=version_check_query,
    )
    version_check_response = client.get(f"{version_check_path}?{version_check_query}")

    # Check that the endpoint works as expected
    assert version_check_response.status_code == 200
    assert version_check_response.json() == {
        "server": current_version,
        "oldest-supported-client": supported_client_version,
        "client-needs-update": True if bump_client_version == "downgrade" else False,
        "client-needs-downgrade": True if bump_client_version == "upgrade" else False,
    }

    # Patch the URL parse result
    mock_parse = mocker.patch("murfey.instrument_server.urlparse")
    mock_parse.return_value = api_base

    # Patch the result of get
    mock_get = mocker.patch("murfey.client.update.requests.get")
    mock_get.return_value = version_check_response

    # Patch the installation function
    mock_install = mocker.patch("murfey.client.update.install_murfey")

    # Patch the perceived client version
    mocker.patch("murfey.client.update.murfey.__version__", new=mock_client_version)

    # If changing the client version, check that 'install_murfey' and 'exit' are called
    if bump_client_version in ("upgrade", "downgrade"):
        with pytest.raises(SystemExit) as exc_info:
            check_for_updates()
        mock_install.assert_called_once()
        # Check that 'exit' is called with the correct message
        assert exc_info.value.code == UPDATE_SUCCESS
    # If client version is the same, 'install_murfey' shouldn't be called
    else:
        check_for_updates()
        mock_install.assert_not_called()

    # Check that the query URL is correct
    mock_get.assert_called_once_with(version_check_url.geturl())


start_instrument_server_test_matrix = (
    # Host | Port
    (
        None,
        None,
    ),  # Test default values
    (
        "127.0.0.1",
        8000,
    ),  # Test manually included values
)


@pytest.mark.parametrize("test_params", start_instrument_server_test_matrix)
def test_start_instrument_server(
    mocker: MockerFixture, test_params: tuple[Optional[str], Optional[int]]
):

    # Unpack test params
    host, port = test_params

    # Patch the Uvicorn Server instance
    mock_server = mocker.patch("uvicorn.Server")
    # Disable 'run'; we just want to confirm it's called correctly
    mock_server.run.return_value = lambda: None

    # Patch the websocket instance
    mock_wsapp = mocker.patch("murfey.client.websocket.WSApp")
    mock_wsapp.return_value = mocker.Mock()  # Disable functionality

    # Construct the expected Uvicorn Config object and save it as a dict
    expected_config = vars(
        uvicorn.Config(
            "murfey.instrument_server.main:app",
            host=host if host is not None else "0.0.0.0",
            port=port if port is not None else 8001,
            log_config=None,
            ws_ping_interval=300,
            ws_ping_timeout=300,
        )
    )

    # Construct the arguments to pass to the instrument server
    sys.argv = [
        "murfey.instrument_server",
    ]

    # Add host and port if they're present
    if host is not None:
        sys.argv.extend(["--host", host])
    if port is not None:
        sys.argv.extend(["--port", str(port)])

    # Run the function
    start_instrument_server()

    # Check that the server was called with the correct arguments
    args, kwargs = mock_server.call_args
    actual_config = vars(kwargs["config"])
    assert expected_config == actual_config
