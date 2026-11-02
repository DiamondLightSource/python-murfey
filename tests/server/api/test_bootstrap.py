from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from murfey.server.api.bootstrap import plugins as plugins_router
from murfey.util.api import url_path_for
from murfey.util.config import MachineConfig


def set_up_test_backend_client():
    """
    Helper function to set up a test backend server whose response can be inspected
    to check that the endpoint function works as expected
    """
    # Set up the backend server
    backend_app = FastAPI()
    backend_app.include_router(plugins_router)
    return TestClient(backend_app)


@pytest.mark.parametrize("packages", ([], ["package_a"], ["package_a", "package_b"]))
def test_show_plugin_wheels(
    mocker: MockerFixture,
    packages: list[str],
    tmp_path: Path,
):
    # Set up test parameters
    instrument_name = "murfey-test"

    # Mock the 'get_machine_config' return value
    plugin_packages = {pkg: tmp_path / pkg for pkg in packages}
    config = MachineConfig(plugin_packages=plugin_packages)
    mock_get_machine_config = mocker.patch(
        "murfey.server.api.bootstrap.get_machine_config",
        return_value={instrument_name: config},
    )

    # Set up the test backend client and the URL to poke
    backend_server = set_up_test_backend_client()
    backend_url_path = url_path_for(
        "api.bootstrap.plugins",
        "show_plugin_wheels",
        instrument_name=instrument_name,
    )

    # Poke it and check that the calls and response are as expected
    response = backend_server.get(backend_url_path)
    mock_get_machine_config.assert_called_once_with(instrument_name=instrument_name)
    assert response.status_code == 200

    # Manually construct the HTML page
    links = "\n".join(f'<li><a href="{pkg}">{pkg}</a></li>' for pkg in packages)
    html_page = f"""
    <!DOCTYPE html>
    <html>
        <head>
            <title>Packages</title>
        </head>
        <body>
            <h1>Available packages</h1>
            <ul>
                {links}
            </ul>
        </body>
    </html>
    """
    # Check that it was constructed correctly
    assert response.content.decode() == html_page


@pytest.mark.parametrize("package_found", (True, False))
def test_get_plugin_wheel(
    mocker: MockerFixture,
    package_found: bool,
    tmp_path: Path,
):
    # Set up test parameters
    instrument_name = "murfey-test"
    package_name = "package_a"

    # Create a test file
    test_package = tmp_path / package_name
    test_package.touch(exist_ok=True)

    # Mock the 'get_machine_config' return value
    plugin_packages = {"package_a": test_package}
    config = MachineConfig(plugin_packages=plugin_packages)
    mock_get_machine_config = mocker.patch(
        "murfey.server.api.bootstrap.get_machine_config",
        return_value={instrument_name: config},
    )

    # Set up the test backend client and the URL to poke
    backend_server = set_up_test_backend_client()
    backend_url_path = url_path_for(
        "api.bootstrap.plugins",
        "get_plugin_wheel",
        instrument_name=instrument_name,
        package="package_a" if package_found else "package_b",
    )

    # Poke it and check that the calls and response are as expected
    response = backend_server.get(backend_url_path)
    mock_get_machine_config.assert_called_once_with(instrument_name=instrument_name)
    assert response.status_code == 200 if package_found else 404
