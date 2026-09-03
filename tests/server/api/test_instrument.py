from datetime import datetime
from pathlib import Path
from typing import Callable, Literal
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from murfey.server.api.auth import (
    validate_frontend_session_access,
    validate_token,
    validate_user_instrument_access,
)
from murfey.server.api.instrument import router as backend_router
from murfey.server.murfey_db import murfey_db_session
from murfey.util.api import url_path_for
from murfey.util.config import MachineConfig


def mock_aiohttp_clientsession(
    mocker: MockerFixture,
    method: Literal["get", "post", "delete"] = "get",
    json_data={},
    status=200,
):
    """
    Helper function to patch a aiohttp.ClientSession GET request. This returns a
    mocked async context manager with a mocked response that, in turn, returns
    the given JSON data and status.

    Returns the mocked ClientSession, which can then be inspected to assert that
    the expected calls were made.
    """

    # Mock out the async response
    mock_response = MagicMock()
    mock_response.json = AsyncMock(return_value=json_data)
    mock_response.status = status

    # Mock out the context manager returned by clientsession.get()
    mock_context_manager = MagicMock()
    mock_context_manager.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context_manager.__aexit__ = AsyncMock(return_value=None)

    # Mock the client session
    mock_clientsession = MagicMock()
    mock_clientsession.__aenter__ = AsyncMock(return_value=mock_clientsession)
    mock_clientsession.__aexit__ = AsyncMock(return_value=None)

    # Assign the context manager to the request method being tested
    getattr(mock_clientsession, method.lower()).return_value = mock_context_manager

    # Patch 'aiohttp.ClientSession' to return the mocked client session
    mocker.patch(
        "murfey.server.api.instrument.aiohttp.ClientSession",
        return_value=mock_clientsession,
    )

    return mock_clientsession, mock_response


def set_up_test_backend_client(
    session_id: int, instrument_name: str, mock_db_session: Callable
):
    """
    Helper function to set up a test backend server whose response can be inspected
    to check that the endpoint function works as expected
    """
    # Set up the backend server
    backend_app = FastAPI()

    # Override validation and database dependencies
    backend_app.dependency_overrides[validate_token] = lambda: None
    backend_app.dependency_overrides[validate_user_instrument_access] = (
        lambda: instrument_name
    )
    backend_app.dependency_overrides[validate_frontend_session_access] = (
        lambda: session_id
    )
    backend_app.dependency_overrides[murfey_db_session] = mock_db_session
    backend_app.include_router(backend_router)
    return TestClient(backend_app)


def test_check_multigrid_controller_status(mocker: MockerFixture):
    # Set up the objects to mock
    instrument_name = "test"
    session_id = 1
    instrument_server_url = "https://murfey.instrument-server.test"

    # Override the database session generator
    mock_session = MagicMock()
    mock_session.instrument_name = instrument_name
    mock_query_result = MagicMock()
    mock_query_result.one.return_value = mock_session
    mock_db_session = MagicMock()
    mock_db_session.exec.return_value = mock_query_result

    def mock_get_db_session():
        yield mock_db_session

    # Mock the machine config
    mock_machine_config = MagicMock()
    mock_machine_config.instrument_server_url = instrument_server_url
    mock_get_machine_config = mocker.patch(
        "murfey.server.api.instrument.get_machine_config"
    )
    mock_get_machine_config.return_value = {
        instrument_name: mock_machine_config,
    }

    # Mock the instrument server tokens dictionary
    mock_tokens = mocker.patch(
        "murfey.server.api.instrument.instrument_server_tokens",
        {session_id: {"access_token": mock.sentinel}},
    )

    # Mock out the async GET request in the endpoint
    mock_clientsession, _ = mock_aiohttp_clientsession(
        mocker,
        method="get",
        json_data={"exists": True},
        status=200,
    )

    # Set up the backend server
    backend_server = set_up_test_backend_client(
        session_id=session_id,
        instrument_name=instrument_name,
        mock_db_session=mock_get_db_session,
    )

    # Construct the URL paths for poking and sending to
    backend_url_path = url_path_for(
        "api.instrument.router",
        "check_multigrid_controller_status",
        session_id=session_id,
    )
    client_url_path = url_path_for(
        "api.router",
        "check_multigrid_controller_status",
        session_id=session_id,
    )

    # Poke the backend
    response = backend_server.get(backend_url_path)

    # Check that the expected calls were made
    mock_db_session.exec.assert_called_once()
    mock_get_machine_config.assert_called_once_with(instrument_name=instrument_name)
    mock_clientsession.get.assert_called_once_with(
        f"{instrument_server_url}{client_url_path}",
        headers={"Authorization": f"Bearer {mock_tokens[session_id]['access_token']}"},
    )
    assert response.status_code == 200
    assert response.json() == {"exists": True}


def test_get_possible_otf_dirs(
    mocker: MockerFixture,
):
    instrument_name = "sim"
    session_id = 1
    instrument_server_url = "https://murfey.instrument-server.test"
    access_token = "dummy"

    # Mock the machine config
    mock_machine_config = MachineConfig(instrument_server_url=instrument_server_url)
    mock_get_machine_config = mocker.patch(
        "murfey.server.api.instrument.get_machine_config",
        return_value={instrument_name: mock_machine_config},
    )

    # Mock the instrument server access token
    mocker.patch(
        "murfey.server.api.instrument.instrument_server_tokens",
        {session_id: {"access_token": access_token}},
    )

    # Mock the client session the API is requesting from
    json_data = [
        {
            "name": "dummy",
            "description": "dummy",
            "size": 0,
            "timestamp": "2020-01-01T12:34:56",
            "full_path": "/path/to/dummy",
        }
    ]
    mock_client_session, _ = mock_aiohttp_clientsession(
        mocker,
        method="get",
        json_data=json_data,
    )

    # Set up the backend server
    backend_server = set_up_test_backend_client(
        session_id=session_id,
        instrument_name=instrument_name,
        mock_db_session=lambda: None,
    )

    # Construct the URL paths for poking and sending to
    backend_url_path = url_path_for(
        "api.instrument.router",
        "get_possible_otf_dirs",
        instrument_name=instrument_name,
        session_id=session_id,
    )
    client_url_path = url_path_for(
        "api.router",
        "get_possible_otf_dirs",
        instrument_name=instrument_name,
        session_id=session_id,
    )

    # Poke the backend
    response = backend_server.get(backend_url_path)
    mock_get_machine_config.assert_called_once()
    mock_client_session.get.assert_called_once_with(
        f"{instrument_server_url}{client_url_path}",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    assert response.status_code == 200
    assert response.json() == json_data


def test_request_otf_dir_upload(
    mocker: MockerFixture,
    tmp_path: Path,
):
    # Set reusable variables here
    instrument_name = "sim"
    session_id = 1
    visit_name = "cm12345-6"

    instrument_server_url = "https://murfey.instrument-server.test"
    access_token = "dummy"
    rsync_basepath = tmp_path / "data"
    otf_dir_name = "setup"

    current_year = datetime.now().year

    # Create the visit directory
    visit_dir = rsync_basepath / str(current_year) / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    visit_path = visit_dir.relative_to(rsync_basepath)

    # Create the client-side OTF directory to transfer
    otf_dir_client = tmp_path / "client" / "otfs" / "OTFs-123456"
    otf_dir_client.mkdir(parents=True, exist_ok=True)

    # Mock the machine config
    mock_machine_config = MachineConfig(
        rsync_basepath=rsync_basepath,
        gain_directory_name=otf_dir_name,
        instrument_server_url=instrument_server_url,
    )
    mocker.patch(
        "murfey.server.api.instrument.get_machine_config",
        return_value={instrument_name: mock_machine_config},
    )

    # Mock the instrument server access token
    mocker.patch(
        "murfey.server.api.instrument.instrument_server_tokens",
        {session_id: {"access_token": access_token}},
    )

    # Override the database session generator
    mock_session = MagicMock(instrument_name=instrument_name, visit=visit_name)
    mock_query_result = MagicMock()
    mock_query_result.one.return_value = mock_session
    mock_db_session = MagicMock()
    mock_db_session.exec.return_value = mock_query_result

    def mock_get_db_session():
        yield mock_db_session

    # Mock the client session the API is requesting from
    json_data = {
        "success": True,
        "destination_path": str(visit_dir / otf_dir_name / otf_dir_client.name),
    }
    mock_client_session, _ = mock_aiohttp_clientsession(
        mocker,
        method="post",
        json_data=json_data,
    )

    # Set up the backend server
    backend_server = set_up_test_backend_client(
        session_id=session_id,
        instrument_name=instrument_name,
        mock_db_session=mock_get_db_session,
    )

    # Construct the URL paths for poking and sending to
    backend_url_path = url_path_for(
        "api.instrument.router",
        "request_otf_dir_upload",
        session_id=session_id,
    )
    client_url_path = url_path_for(
        "api.router",
        "upload_otf_dir",
        instrument_name=instrument_name,
        session_id=session_id,
    )

    # Poke the backend
    response = backend_server.post(
        backend_url_path,
        json={"dir_path": str(otf_dir_client)},
    )

    # Check that the server-side OTF directory save location was created
    assert (visit_dir / otf_dir_name).exists()

    # Check that request was sent to instrument server with expected calls
    payload = {
        "dir_path": str(otf_dir_client),
        "visit_path": str(visit_path),
        "destination_dir": otf_dir_name,
    }
    mock_client_session.post.assert_called_once_with(
        f"{instrument_server_url}{client_url_path}",
        json=payload,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    # Check that the status code and returned data are correct
    assert response.status_code == 200
    assert response.json() == json_data
