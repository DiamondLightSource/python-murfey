from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Sequence
from unittest.mock import MagicMock

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlmodel import Session as SQLModelSession, insert, select

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


@pytest.mark.parametrize(
    "test_params",
    (  # Visit name | Cleaned visit | Session name | Cleaned name | End time
        ("cm23456-7", "cm23456-7", "Some string", "Some string", "2026-10-01T11:13:00"),
        ("cm34567-8", "cm34567-8", "New\r\n visit", "New visit", None),
        ("cm45678-9", "cm45678-9", "New\n visit", "New visit", None),
        (
            "a[s]d<f>g!h@j#k$l%^&*()+|`~",
            "asdfghjkl",
            "Quick; {brown} \\f\\o\\x (jumps) [over] 'lazy' $dog$",
            "Quick brown fox (jumps) [over] 'lazy' dog",
            None,
        ),
    ),
)
def test_create_session_with_db(
    test_params: tuple[str, str, str, str, str | None],
    murfey_db_session: SQLModelSession,
):
    visit_name, visit_clean, session_name, name_clean, visit_end_time = test_params

    # Set up a mock Murfey database session
    def mock_get_db_session():
        yield murfey_db_session

    # Set up the backend server
    backend_server = set_up_test_backend_client(
        router=router,
        instrument_name=instrument_name,
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
            "name": session_name,
            "end_time": visit_end_time,
        },
    )
    assert response.status_code == 200

    # Check that the database insert happened correctly
    murfey_session = murfey_db_session.exec(
        select(MurfeyDB.Session).where(MurfeyDB.Session.visit == visit_clean)
    ).one()
    assert murfey_session.name == name_clean
    if visit_end_time is not None:
        assert murfey_session.visit_end_time == datetime.fromisoformat(visit_end_time)
    else:
        assert murfey_session.visit_end_time is None


def test_get_preprocessed_movie_count(
    tmp_path: Path,
    murfey_db_session: SQLModelSession,
):
    # Set up parameters
    session_id = ExampleVisit.murfey_session_id

    # Create some DataCollectionGroups, DataCollections, and Movies
    num_dcgs = 3
    num_dcs = 3
    num_movies = 4

    # Insert all DataCollectionGroup rows and return inserted rows
    dcgs: Sequence[MurfeyDB.DataCollectionGroup] = murfey_db_session.exec(
        insert(MurfeyDB.DataCollectionGroup)
        .values(
            [
                {
                    "session_id": session_id,
                    "tag": f"dcg_{i}",
                }
                for i in range(num_dcgs)
            ]
        )
        .returning(MurfeyDB.DataCollectionGroup)
    ).all()

    # Insert all DataCollection rows and return inserted rows
    dcs: Sequence[MurfeyDB.DataCollection] = murfey_db_session.exec(
        insert(MurfeyDB.DataCollection)
        .values(
            [
                {
                    "dataCollectionGroupId": dcg.id,
                    "tag": dcg.tag,
                }
                for dcg in dcgs
                for _ in range(num_dcs)
            ]
        )
        .returning(MurfeyDB.DataCollection)
    ).all()

    for n, dc in enumerate(dcs):
        # Insert all ProcessingJob rows and return inserted rows
        pjs: Sequence[MurfeyDB.ProcessingJob] = murfey_db_session.exec(
            insert(MurfeyDB.ProcessingJob)
            .values(
                [
                    {
                        "dataCollectionId": dc.id,
                        "recipe": "dummy",
                    }
                    for _ in range(num_movies)
                ]
            )
            .returning(MurfeyDB.ProcessingJob)
        ).all()

        # Insert all ProcessingJob rows and return inserted rows
        apps: Sequence[MurfeyDB.AutoProcProgram] = murfey_db_session.exec(
            insert(MurfeyDB.AutoProcProgram)
            .values(
                [
                    {
                        "pj_id": pj.id,
                    }
                    for pj in pjs
                ]
            )
            .returning(MurfeyDB.AutoProcProgram)
        ).all()

        ledgers: Sequence[MurfeyDB.MurfeyLedger] = murfey_db_session.exec(
            insert(MurfeyDB.MurfeyLedger)
            .values(
                [
                    {
                        "app_id": app.id,
                    }
                    for app in apps
                ]
            )
            .returning(MurfeyDB.MurfeyLedger)
        ).all()

        # Insert all Movie rows
        murfey_db_session.exec(
            insert(MurfeyDB.Movie).values(
                [
                    {
                        "movieId": ledger.id,
                        "dataCollectionId": dc.id,
                        "path": str(tmp_path)
                        / dc.tag
                        / f"movie_{(n * num_movies) + l}.mrc",
                        "tag": dc.tag,
                        "preprocessed": bool(n % 2),
                    }
                    for l, ledger in enumerate(ledgers)
                ]
            )
        )

    # Set up a mock Murfey database session
    def mock_get_db_session():
        yield murfey_db_session

    # Set up the backend server
    backend_server = set_up_test_backend_client(
        router=router,
        instrument_name=instrument_name,
        session_id=session_id,
        mock_db_session=mock_get_db_session,
    )
    # Construct the URL path to poke
    backend_url_path = url_path_for(
        "api.session_info.router",
        "get_preprocessed_movie_count",
        session_id=session_id,
    )
    # Poke the backend
    response = backend_server.get(
        backend_url_path,
    )
    assert response.status_code == 200
    data: dict[str, int] = response.json()
    assert data["complete"] == 16
    assert data["incomplete"] == 20


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
