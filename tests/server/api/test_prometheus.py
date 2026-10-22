from pathlib import Path
from typing import Callable

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session as SQLModelSession, select

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


@pytest.mark.parametrize(
    "test_params",
    (  # File count | File bytes | Data count | Data bytes | Iterations
        (1, 1024, 1, 1024, 1),
        (1, 1024, 1, 1024, 3),
    ),
)
def test_increment_rsync_file_count(
    test_params: tuple[int, int, int, int, int],
    tmp_path: Path,
    murfey_db_session: SQLModelSession,
):
    # Unpack and set up test parameters
    src = str(tmp_path / "client" / visit_name / "raw")
    dst = str(tmp_path / "data" / visit_name / "raw")
    file_count, file_bytes, data_count, data_bytes, iterations = test_params

    # Register an RsyncInstance table entry
    rsync_instance = MurfeyDB.RsyncInstance(
        source=src,
        destination=dst,
        session_id=session_id,
        transferring=True,
    )
    murfey_db_session.add(rsync_instance)
    murfey_db_session.commit()

    # Set up the backend test client
    def mock_get_db_session():
        yield murfey_db_session

    backend_client = set_up_test_backend_client(
        router=router,
        mock_db_session=mock_get_db_session,
    )

    # Submit posts to the endpoint
    backend_url_path = url_path_for(
        "api.prometheus.router",
        "increment_rsync_file_count",
        session_id=session_id,
    )
    for _ in range(iterations):
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

    # Check that the database entry was updated correctly
    assert (
        rsync_instance := murfey_db_session.exec(
            select(MurfeyDB.RsyncInstance)
            .where(MurfeyDB.RsyncInstance.session_id == session_id)
            .where(MurfeyDB.RsyncInstance.source == src)
            .where(MurfeyDB.RsyncInstance.destination == dst)
        ).one_or_none()
    ) is not None
    assert rsync_instance.files_counted == file_count * iterations

    # Check that the Prometheus objects were updated correctly
    import murfey.server.prometheus as prom

    # Check 'seen_files'
    seen_files_samples = prom.seen_files.collect()[0].samples
    seen_files_sample = next(
        s
        for s in seen_files_samples
        if s.labels
        == {
            "microscope": instrument_name,
            "rsync_source": src,
            "visit": visit_name,
        }
    )
    assert seen_files_sample.value == file_count * iterations

    # Check 'seen_data_files'
    seen_data_files_samples = prom.seen_data_files.collect()[0].samples
    seen_data_files_sample = next(
        s
        for s in seen_data_files_samples
        if s.labels
        == {
            "microscope": instrument_name,
            "rsync_source": src,
            "visit": visit_name,
        }
    )
    assert seen_data_files_sample.value == data_count * iterations
