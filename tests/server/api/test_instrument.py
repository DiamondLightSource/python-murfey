from typing import Literal
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from murfey.server.api.auth import validate_frontend_session_access, validate_token
from murfey.server.api.instrument import router as backend_router
from murfey.server.murfey_db import murfey_db_session
from murfey.util.api import url_path_for


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
    mocker.patch("aiohttp.ClientSession", return_value=mock_clientsession)

    return mock_clientsession, mock_response


def test_check_multigrid_controller_status(mocker: MockerFixture):
    # Set up the objects to mock
    instrument_name = "test"
    session_id = 1
    instrment_server_url = "https://murfey.instrument-server.test"

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
    mock_machine_config.instrument_server_url = instrment_server_url
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
    backend_app = FastAPI()

    # Override validation and database dependencies
    backend_app.dependency_overrides[validate_token] = lambda: None
    backend_app.dependency_overrides[validate_frontend_session_access] = (
        lambda: session_id
    )
    backend_app.dependency_overrides[murfey_db_session] = mock_get_db_session
    backend_app.include_router(backend_router)
    backend_server = TestClient(backend_app)

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
        f"{instrment_server_url}{client_url_path}",
        headers={"Authorization": f"Bearer {mock_tokens[session_id]['access_token']}"},
    )
    assert response.status_code == 200
    assert response.json() == {"exists": True}
