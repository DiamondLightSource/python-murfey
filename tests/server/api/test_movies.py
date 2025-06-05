from unittest.mock import ANY

from fastapi.testclient import TestClient
from pytest import fixture
from sqlmodel import Session

from murfey.server.api.auth import validate_instrument_token
from murfey.server.main import app
from murfey.server.murfey_db import murfey_db
from murfey.util.api import url_path_for
from murfey.util.db import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    Movie,
    MurfeyLedger,
    ProcessingJob,
)
from tests.conftest import ExampleVisit, get_or_create_db_entry

# @pytest.fixture(scope="module")
# def test_user():
#     return {"username": "testuser", "password": "testpass"}


# def movies_return():
#     return [("Supervisor_1", 2)]


# expression = Mock()
# expression.all = movies_return

# mock_session = create_autospec(Session, instance=True)
# mock_session.exec.return_value = expression


# def override_murfey_db():
#     try:
#         db = mock_session
#         yield db
#     finally:
#         db.close()


# app.dependency_overrides[murfey_db] = override_murfey_db


@fixture(scope="module")
def fastapi_client(murfey_db_session):
    # Replace the murfey_db instance in endpoint with properly initialised pytest one
    app.dependency_overrides[murfey_db] = murfey_db_session
    # Disable instrument token validation
    app.dependency_overrides[validate_instrument_token] = lambda: None

    with TestClient(app) as client:
        yield client


def test_movie_count(
    fastapi_client: TestClient,
    murfey_db_session: Session,  # From conftest.py
):

    # Insert table dependencies
    dcg_entry: DataCollectionGroup = get_or_create_db_entry(
        murfey_db_session,
        DataCollectionGroup,
        lookup_kwargs={
            "id": 0,
            "session_id": ExampleVisit.murfey_session_id,
            "tag": "test_dcg",
        },
    )
    dc_entry: DataCollection = get_or_create_db_entry(
        murfey_db_session,
        DataCollection,
        lookup_kwargs={
            "id": 0,
            "tag": "test_dc",
            "dcg_id": dcg_entry.id,
        },
    )
    processing_job_entry: ProcessingJob = get_or_create_db_entry(
        murfey_db_session,
        ProcessingJob,
        lookup_kwargs={
            "id": 0,
            "recipe": "test_recipe",
            "dc_id": dc_entry.id,
        },
    )
    autoproc_entry: AutoProcProgram = get_or_create_db_entry(
        murfey_db_session,
        AutoProcProgram,
        lookup_kwargs={
            "id": 0,
            "pj_id": processing_job_entry.id,
        },
    )

    # Insert test movies and one-to-one dependencies into Murfey DB
    tag = "test_movie"
    num_movies = 5
    murfey_db_session
    for i in range(num_movies):
        murfey_ledger_entry: MurfeyLedger = get_or_create_db_entry(
            murfey_db_session,
            MurfeyLedger,
            lookup_kwargs={
                "id": i,
                "app_id": autoproc_entry.id,
            },
        )
        _: Movie = get_or_create_db_entry(
            murfey_db_session,
            Movie,
            lookup_kwargs={
                "murfey_id": murfey_ledger_entry.id,
                "path": "/some/path",
                "image_number": i,
                "tag": tag,
            },
        )

    response = fastapi_client.get(
        f"{url_path_for('session_control.router', 'count_number_of_movies')}",
        headers={"Authorization": f"Bearer {ANY}"},
    )
    assert response.json() == {tag: num_movies}
