from pathlib import Path
from typing import Any
from unittest import mock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from murfey.server.api.auth import (
    validate_instrument_server_session_access,
    validate_instrument_token,
)
from murfey.server.api.session_control import gather_upstream_files, spa_router
from murfey.server.murfey_db import murfey_db_session
from murfey.util.api import url_path_for
from murfey.util.models import UpstreamFileRequestInfo


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


@pytest.mark.parametrize(
    "search_strings",
    (
        ["dummy"],
        [],
        None,
    ),
)
@pytest.mark.asyncio
async def test_gather_upstream_files(
    mocker: MockerFixture,
    tmp_path: Path,
    search_strings: list[str] | None,
):
    # Construct dictionary to pass to Pydantic model
    session_id = 1
    upstream_instrument = "dummy"
    upstream_visit_path = str(tmp_path / "dummy")
    params_dict: dict[str, Any] = {
        "upstream_instrument": upstream_instrument,
        "upstream_visit_path": upstream_visit_path,
    }
    if search_strings is not None:
        params_dict["search_strings"] = search_strings

    # Validate the incoming message
    params = UpstreamFileRequestInfo(**params_dict)

    # Patch the actual 'gather_upstream_files' function
    mock_gather = mocker.patch(
        "murfey.server.api.session_control._gather_upstream_files"
    )

    # Create a mock database session
    mock_db = MagicMock()

    # Run the function and check that the expected calls were made:
    await gather_upstream_files(
        visit_name="dummy",
        session_id=session_id,
        upstream_file_request=params,
        db=mock_db,
    )
    mock_gather.assert_called_with(
        session_id=session_id,
        upstream_instrument=upstream_instrument,
        upstream_visit_path=Path(upstream_visit_path),
        search_strings=search_strings,
        db=mock_db,
    )
