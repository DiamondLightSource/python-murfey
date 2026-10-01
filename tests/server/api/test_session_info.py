from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlmodel import Session as SQLModelSession, select

import murfey.util.db as MurfeyDB
from murfey.server.api.auth import (
    validate_frontend_session_access,
    validate_token,
    validate_user_instrument_access,
)
from murfey.server.api.session_info import gather_upstream_files, router
from murfey.server.murfey_db import murfey_db_session
from murfey.util.api import url_path_for
from murfey.util.models import UpstreamFileRequestInfo
from tests.conftest import ExampleVisit

instrument_name = ExampleVisit.instrument_name


def set_up_test_backend_client(
    router: APIRouter,
    session_id: int | None = None,
    instrument_name: str | None = None,
    mock_db_session: Callable | None = None,
):
    """
    Helper function to set up a test backend server whose response can be inspected
    to check that the endpoint function works as expected
    """
    # Set up the backend server
    backend_app = FastAPI()

    # Override validation and database dependencies as needed
    backend_app.dependency_overrides[validate_token] = lambda: None
    if instrument_name:
        backend_app.dependency_overrides[validate_user_instrument_access] = (
            lambda: instrument_name
        )
    if session_id:
        backend_app.dependency_overrides[validate_frontend_session_access] = (
            lambda: session_id
        )
    if mock_db_session:
        backend_app.dependency_overrides[murfey_db_session] = mock_db_session

    # Attach router, initiate object, and return it
    backend_app.include_router(router)
    return TestClient(backend_app)


def test_create_session_with_db(murfey_db_session: SQLModelSession):
    visit_name = "cm23456-7"
    visit_end_time = "2026-10-01T11:13:00"

    # Set up a mock Murfey database session
    def mock_get_db_session():
        yield murfey_db_session

    # Set up the backend server
    backend_server = set_up_test_backend_client(
        router=router,
        mock_db_session=mock_get_db_session,
    )
    # Construct the URL path to poke
    backend_url_path = url_path_for(
        "api.session_info.router",
        "create_session",
        instrument_name=instrument_name,
    )
    # Poke the backend
    response = backend_server.post(
        backend_url_path,
        json={
            "visit": visit_name,
            "name": "Some string",
            "end_time": visit_end_time,
        },
    )
    assert response.status_code == 200

    # Check that the database insert happened correctly
    murfey_session = murfey_db_session.exec(
        select(MurfeyDB.Session).where(MurfeyDB.Session.visit == visit_name)
    ).one()
    assert murfey_session is not None
    assert murfey_session.name == "Some string"
    assert murfey_session.visit_end_time == datetime.fromisoformat(visit_end_time)


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
    mock_gather = mocker.patch("murfey.server.api.session_info._gather_upstream_files")

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
