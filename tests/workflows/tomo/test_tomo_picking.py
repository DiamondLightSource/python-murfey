from unittest import mock

from sqlmodel import Session, select

from murfey.util.db import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    ParticleSizes,
    ProcessingJob,
    TomogramPicks,
    TomographyProcessingParameters,
)
from murfey.workflows.tomo import picking
from tests.conftest import ExampleVisit, get_or_create_db_entry


@mock.patch("murfey.workflows.tomo.picking._transport_object")
@mock.patch("murfey.workflows.tomo.picking._ids_tomo_classification")
def test_picked_tomogram_not_run_class2d(
    mock_ids, mock_transport, murfey_db_session: Session, tmp_path
):
    """Run the picker feedback with less particles than needed for classification"""
    mock_ids.return_value = [2, 1]

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
    get_or_create_db_entry(
        murfey_db_session,
        ProcessingJob,
        lookup_kwargs={
            "id": 1,
            "recipe": "test_recipe",
            "dc_id": dc_entry.id,
        },
    )

    message = {
        "program_id": 0,
        "cbox_3d": f"{tmp_path}/AutoPick/job007/CBOX_3d/sample.cbox",
        "particle_count": 2,
        "particle_diameters": [10.1, 20.2],
        "pixel_size": 5.3,
        "register": "picked_tomogram",
        "tomogram": f"{tmp_path}/Tomograms/job006/tomograms/sample.mrc",
    }

    picking._register_picked_tomogram_use_diameter(message, murfey_db_session)

    mock_ids.assert_called_once_with(0, "em-tomo-class2d", murfey_db_session)

    tomograms_db = murfey_db_session.exec(
        select(TomogramPicks).where(TomogramPicks.pj_id == 1)
    ).one()
    assert tomograms_db.tomogram == message["tomogram"]
    assert tomograms_db.cbox_3d == message["cbox_3d"]
    assert tomograms_db.particle_count == 2
    assert tomograms_db.tomogram_pixel_size == 5.3

    added_picks = murfey_db_session.exec(
        select(ParticleSizes).where(ParticleSizes.pj_id == 1)
    ).all()
    assert len(added_picks) == 2
    assert added_picks[0].particle_size == 10.1
    assert added_picks[1].particle_size == 20.2

    mock_transport.send.assert_not_called()


@mock.patch("murfey.workflows.tomo.picking._transport_object")
@mock.patch("murfey.workflows.tomo.picking._ids_tomo_classification")
def test_picked_tomogram_run_class2d(
    mock_ids, mock_transport, murfey_db_session: Session, tmp_path
):
    """Run the picker feedback with less particles than needed for classification"""
    mock_transport.feedback_queue = "murfey_feedback"

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
            "id": 1,
            "recipe": "test_recipe",
            "dc_id": dc_entry.id,
        },
    )
    get_or_create_db_entry(
        murfey_db_session,
        AutoProcProgram,
        lookup_kwargs={
            "id": 0,
            "pj_id": processing_job_entry.id,
        },
    )
    get_or_create_db_entry(
        murfey_db_session,
        TomographyProcessingParameters,
        lookup_kwargs={
            "dcg_id": dcg_entry.id,
            "pixel_size": 1.34,
            "dose_per_frame": 1,
            "frame_count": 5,
            "tilt_axis": 0,
            "voltage": 300,
            "particle_diameter": 200,
        },
    )
    for particle in range(10001):
        get_or_create_db_entry(
            murfey_db_session,
            ParticleSizes,
            lookup_kwargs={
                "id": particle,
                "pj_id": processing_job_entry.id,
                "particle_size": 100,
            },
        )

    mock_ids.return_value = [dcg_entry.id, 1]

    message = {
        "session_id": 1,
        "program_id": 0,
        "cbox_3d": f"{tmp_path}/AutoPick/job007/CBOX_3d/sample.cbox",
        "particle_count": 2,
        "particle_diameters": [10.1, 20.2],
        "pixel_size": 5.3,
        "register": "picked_tomogram",
        "tomogram": f"{tmp_path}/Tomograms/job006/tomograms/sample.mrc",
    }

    # Create a data collection group for lookups
    grid_square = DataCollectionGroup(
        id=1,
        session_id=ExampleVisit.murfey_session_id,
        tag="session_tag",
        atlas_id=90,
    )
    murfey_db_session.add(grid_square)
    murfey_db_session.commit()

    picking._register_picked_tomogram_use_diameter(message, murfey_db_session)

    mock_ids.assert_called_once_with(0, "em-tomo-class2d", murfey_db_session)

    tomograms_db = murfey_db_session.exec(
        select(TomogramPicks).where(TomogramPicks.pj_id == 1)
    ).one()
    assert tomograms_db.tomogram == message["tomogram"]
    assert tomograms_db.cbox_3d == message["cbox_3d"]
    assert tomograms_db.particle_count == 2
    assert tomograms_db.tomogram_pixel_size == 5.3

    mock_transport.send.assert_called_once_with(
        "processing_recipe",
        {
            "parameters": {
                "tomogram": message["tomogram"],
                "cbox_3d": message["cbox_3d"],
                "pixel_size": message["pixel_size"],
                "particle_diameter": 200.0,
                "kv": 300,
                "node_creator_queue": "node_creator",
                "session_id": message["session_id"],
                "autoproc_program_id": 0,
                "batch_size": 10000,
                "nr_classes": 5,
                "picker_id": None,
                "class2d_grp_uuid": 6,
                "class_uuids": {str(i): i for i in range(1, 6)},
                "feedback_queue": "murfey_feedback",
            },
            "recipes": ["em-tomo-class2d"],
        },
        new_connection=True,
    )
