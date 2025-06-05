from sqlmodel import Session

from murfey.server.api.session_control import count_number_of_movies
from murfey.util.db import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    Movie,
    MurfeyLedger,
    ProcessingJob,
)
from tests.conftest import ExampleVisit, get_or_create_db_entry


def test_movie_count(
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

    # Insert test movies and one-to-one dependencies
    tag = "test_movie"
    num_movies = 5
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

    # Run function and evaluate result
    result = count_number_of_movies(murfey_db_session)
    assert result == {tag: num_movies}
