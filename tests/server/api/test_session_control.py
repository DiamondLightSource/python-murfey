from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from murfey.server.api.auth import (
    validate_instrument_server_session_access,
    validate_instrument_token,
)
from murfey.server.api.session_control import spa_router
from murfey.server.murfey_db import murfey_db_session
from murfey.util.api import url_path_for


def test_make_atlas_jpg(mocker: MockerFixture, tmp_path: Path):
    # Set up the objects to mock
    instrument_name = "test"
    visit_name = "test_visit"
    session_id = 1

    # Override the database session generator
    mock_session = MagicMock()
    mock_session.instrument_name = instrument_name
    mock_session.visit = visit_name
    mock_query_result = MagicMock()
    mock_query_result.one.return_value = mock_session
    mock_db_session = MagicMock()
    mock_db_session.exec.return_value = mock_query_result

    def mock_get_db_session():
        yield mock_db_session

    # Mock the instrument server tokens dictionary
    mock_tokens = mocker.patch(
        "murfey.server.api.instrument.instrument_server_tokens",
        {session_id: {"access_token": mock.sentinel}},
    )

    # Mock the called workflow function
    mock_atlas_jpg = mocker.patch(
        "murfey.server.api.session_control.atlas_jpg_from_mrc",
        return_value=None,
    )

    # Set up the test file
    image_dir = tmp_path / instrument_name / "data" / visit_name / "Atlas"
    image_dir.mkdir(parents=True, exist_ok=True)
    test_file = image_dir / "Atlas1.mrc"

    # Set up the backend server
    backend_app = FastAPI()

    # Override validation and database dependencies
    backend_app.dependency_overrides[validate_instrument_token] = lambda: None
    backend_app.dependency_overrides[validate_instrument_server_session_access] = (
        lambda: session_id
    )
    backend_app.dependency_overrides[murfey_db_session] = mock_get_db_session
    backend_app.include_router(spa_router)
    backend_server = TestClient(backend_app)

    atlas_jpg_url = url_path_for(
        "api.session_control.spa_router", "make_atlas_jpg", session_id=session_id
    )
    response = backend_server.post(
        atlas_jpg_url,
        json={"path": str(test_file)},
        headers={"Authorization": f"Bearer {mock_tokens[session_id]['access_token']}"},
    )

    # Check that the expected calls were made
    mock_atlas_jpg.assert_called_once_with(instrument_name, visit_name, test_file)
    assert response.status_code == 200
