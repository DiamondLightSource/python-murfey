from pathlib import Path
from typing import Callable

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session as SQLModelSession

import murfey.util.db as MurfeyDB
from murfey.server.api.auth import (
    validate_frontend_session_access,
    validate_instrument_token,
    validate_token,
    validate_user_instrument_access,
)
from murfey.server.api.prometheus import router
from murfey.server.murfey_db import murfey_db_session
from murfey.util.api import url_path_for
from tests.conftest import ExampleVisit

visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
instrument_name = ExampleVisit.instrument_name
session_id = ExampleVisit.murfey_session_id


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
    backend_app.dependency_overrides[validate_instrument_token] = lambda: None
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


def test_increment_rsync_file_count(tmp_path: Path, murfey_db_session: SQLModelSession):
    src = str(tmp_path / "client" / visit_name / "raw")
    dst = str(tmp_path / "data" / visit_name / "raw")
    file_count = 1
    file_bytes = 1024
    data_count = 1
    data_bytes = 1024

    # Register an RsyncInstance table entry
    rsync_instance = MurfeyDB.RsyncInstance(
        source=src,
        destination=dst,
        session_id=session_id,
        transferring=True,
    )
    murfey_db_session.add(rsync_instance)
    murfey_db_session.commit()

    # Create a function that returns the database fixture
    def mock_get_db_session():
        yield murfey_db_session

    backend_client = set_up_test_backend_client(
        router=router,
        mock_db_session=mock_get_db_session,
    )
    backend_url_path = url_path_for(
        "api.prometheus.router",
        "increment_rsync_file_count",
        session_id=session_id,
    )
    response = backend_client.post(
        backend_url_path,
        json={
            "source": src,
            "destination": dst,
            "increment_count": file_count,
            "bytes": file_bytes,
            "increment_data_count": data_count,
            "data_bytes": data_bytes,
        },
    )
    assert response.status_code == 200
