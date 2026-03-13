from unittest import mock

from sqlmodel import Session, select

from murfey.util.db import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    TiltSeries,
)
from murfey.workflows.sxt import process_sxt_tilt_series
from tests.conftest import ExampleVisit, get_or_create_db_entry


def set_up_db(murfey_db_session: Session):
    # Insert common elements needed in all picking tests
    dcg_entry: DataCollectionGroup = get_or_create_db_entry(
        murfey_db_session,
        DataCollectionGroup,
        lookup_kwargs={
            "id": 0,
            "session_id": ExampleVisit.murfey_session_id,
            "tag": "/path/to/tomogram_source",
        },
    )
    dc_entry: DataCollection = get_or_create_db_entry(
        murfey_db_session,
        DataCollection,
        lookup_kwargs={
            "id": 0,
            "tag": "tomogram_tag",
            "dcg_id": dcg_entry.id,
        },
    )
    processing_job_entry: ProcessingJob = get_or_create_db_entry(
        murfey_db_session,
        ProcessingJob,
        lookup_kwargs={
            "id": 1,
            "recipe": "sxt-tomo-align",
            "dc_id": dc_entry.id,
        },
    )
    auto_proc_entry = get_or_create_db_entry(
        murfey_db_session,
        AutoProcProgram,
        lookup_kwargs={
            "id": 0,
            "pj_id": processing_job_entry.id,
        },
    )
    return dcg_entry.id, dc_entry.id, processing_job_entry.id, auto_proc_entry.id


@mock.patch("murfey.workflows.sxt.process_sxt_tilt_series._transport_object")
def test_process_new_sxt_tilt_series(
    mock_transport, murfey_db_session: Session, tmp_path
):
    """Run the picker feedback with less particles than needed for classification"""
    dcg_id, dc_id, pj_id, app_id = set_up_db(murfey_db_session)

    new_parameters = process_sxt_tilt_series.SXTTiltSeriesInfo(
        session_id=ExampleVisit.murfey_session_id,
        tag="tomogram_tag",
        source="/path/to/tomogram_source",
        txrm=f"{tmp_path}/cm12345-6/raw/tomogram_tag.txrm",
        tilt_series_length=5,
        pixel_size=100,
        tilt_offset=1,
    )

    # Run the registration
    process_sxt_tilt_series.process_sxt_tilt_series_workflow(
        "cm12345-6",
        ExampleVisit.murfey_session_id,
        new_parameters,
        murfey_db_session,
    )

    # Check the processing message
    mock_transport.send.assert_any_call(
        "processing_recipe",
        {
            "parameters": {
                "txrm_file": f"{tmp_path}/cm12345-6/raw/tomogram_tag.txrm",
                "dcid": dc_id,
                "appid": app_id,
                "stack_file": f"{tmp_path}/cm123456-7/processed/raw/relion_murfey/Tomograms/tomogram_tag_stack.mrc",
                "tilt_axis": 0,
                "pixel_size": 100,
                "manual_tilt_offset": -1,
                "node_creator_queue": "node_creator",
            },
            "recipes": ["sxt-tomo-align"],
        },
        new_connection=True,
    )

    # Check the database insert
    tilt_series_entry = murfey_db_session.exec(select(TiltSeries)).one()
    assert tilt_series_entry.session_id == ExampleVisit.murfey_session_id
    assert tilt_series_entry.tag == "tomogram_tag"
    assert tilt_series_entry.rsync_source == "/path/to/tomogram_source"
    assert tilt_series_entry.tilt_series_length == 5
    assert tilt_series_entry.processing_requested
