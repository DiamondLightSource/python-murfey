from unittest import mock

from sqlmodel import Session, select

from murfey.util.db import (
    AutoProcProgram,
    ClassificationFeedbackParameters,
    DataCollection,
    DataCollectionGroup,
    ParticleSizes,
    ProcessingJob,
    TomogramPicks,
    TomographyProcessingParameters,
)
from murfey.workflows.tomo import picking
from tests.conftest import ExampleVisit, get_or_create_db_entry


def set_up_picking_db(murfey_db_session: Session):
    # Insert common elements needed in all picking tests
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
        ClassificationFeedbackParameters,
        lookup_kwargs={
            "pj_id": processing_job_entry.id,
            "estimate_particle_diameter": True,
            "hold_class2d": False,
            "hold_class3d": False,
            "class_selection_score": 0,
            "star_combination_job": 0,
            "initial_model": "",
            "next_job": 0,
        },
    )
    return dcg_entry.id, dc_entry.id, processing_job_entry.id


def test_ids_tomo_classification(murfey_db_session: Session):
    dcg_id, first_dc_id, first_pj_id = set_up_picking_db(murfey_db_session)

    # Insert a second data collection, processing job and autoproc program
    second_dc: DataCollection = get_or_create_db_entry(
        murfey_db_session,
        DataCollection,
        lookup_kwargs={
            "id": 1,
            "tag": "second_dc",
            "dcg_id": dcg_id,
        },
    )
    second_pj: ProcessingJob = get_or_create_db_entry(
        murfey_db_session,
        ProcessingJob,
        lookup_kwargs={
            "id": 10,
            "recipe": "second_recipe",
            "dc_id": second_dc.id,
        },
    )
    get_or_create_db_entry(
        murfey_db_session,
        AutoProcProgram,
        lookup_kwargs={
            "id": 11,
            "pj_id": second_pj.id,
        },
    )

    returned_ids = picking._ids_tomo_classification(
        11, "test_recipe", murfey_db_session
    )
    assert returned_ids[0] == dcg_id
    assert returned_ids[1] == first_pj_id


@mock.patch("murfey.workflows.tomo.picking._transport_object")
@mock.patch("murfey.workflows.tomo.picking._ids_tomo_classification")
def test_picked_tomogram_not_run_class2d(
    mock_ids, mock_transport, murfey_db_session: Session, tmp_path
):
    """Run the picker feedback with less particles than needed for classification"""
    mock_ids.return_value = [2, 1]

    # Insert table dependencies
    set_up_picking_db(murfey_db_session)

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
def test_picked_tomogram_run_class2d_with_diameter(
    mock_ids, mock_transport, murfey_db_session: Session, tmp_path
):
    """Run the picker feedback with a pre-determined particle diameter"""
    mock_transport.feedback_queue = "murfey_feedback"

    # Insert table dependencies
    dcg_id, dc_id, pj_id = set_up_picking_db(murfey_db_session)
    get_or_create_db_entry(
        murfey_db_session,
        TomographyProcessingParameters,
        lookup_kwargs={
            "dcg_id": dcg_id,
            "pixel_size": 1.34,
            "dose_per_frame": 1,
            "frame_count": 5,
            "tilt_axis": 0,
            "voltage": 300,
            "particle_diameter": 200,
        },
    )
    for particle in range(5001):
        get_or_create_db_entry(
            murfey_db_session,
            ParticleSizes,
            lookup_kwargs={
                "id": particle,
                "pj_id": pj_id,
                "particle_size": 100,
            },
        )

    mock_ids.return_value = [dcg_id, 1]

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
                "batch_size": 5000,
                "nr_classes": 5,
                "picker_id": None,
                "class2d_grp_uuid": 6,
                "class_uuids": {str(i): i for i in range(1, 6)},
                "next_job": 9,
                "feedback_queue": "murfey_feedback",
            },
            "recipes": ["em-tomo-class2d"],
        },
        new_connection=True,
    )


@mock.patch("murfey.workflows.tomo.picking._transport_object")
@mock.patch("murfey.workflows.tomo.picking._ids_tomo_classification")
def test_picked_tomogram_run_class2d_estimate_diameter(
    mock_ids, mock_transport, murfey_db_session: Session, tmp_path
):
    """Run the picker feedback for Class2D, including diameter estimation"""
    mock_transport.feedback_queue = "murfey_feedback"

    # Insert table dependencies
    dcg_id, dc_id, pj_id = set_up_picking_db(murfey_db_session)
    get_or_create_db_entry(
        murfey_db_session,
        TomographyProcessingParameters,
        lookup_kwargs={
            "dcg_id": dcg_id,
            "pixel_size": 1.34,
            "dose_per_frame": 1,
            "frame_count": 5,
            "tilt_axis": 0,
            "voltage": 300,
            "particle_diameter": None,
        },
    )
    for particle in range(5001):
        get_or_create_db_entry(
            murfey_db_session,
            ParticleSizes,
            lookup_kwargs={
                "id": particle,
                "pj_id": pj_id,
                "particle_size": 100,
            },
        )
    # Insert one existing tomogram which should get flushed out
    get_or_create_db_entry(
        murfey_db_session,
        TomogramPicks,
        lookup_kwargs={
            "pj_id": pj_id,
            "tomogram": f"{tmp_path}/Tomograms/job006/tomograms/tomogram1.mrc",
            "cbox_3d": f"{tmp_path}/AutoPick/job007/CBOX_3d/tomogram1_picks.cbox",
            "particle_count": 10,
            "tomogram_pixel_size": 5.3,
        },
    )

    mock_ids.return_value = [dcg_id, 1]

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
    picking._register_picked_tomogram_use_diameter(message, murfey_db_session)

    mock_ids.assert_called_once_with(0, "em-tomo-class2d", murfey_db_session)

    # Two mock calls - one flushed tomogram and one new
    assert mock_transport.send.call_count == 2
    mock_transport.send.assert_any_call(
        "processing_recipe",
        {
            "parameters": {
                "tomogram": f"{tmp_path}/Tomograms/job006/tomograms/tomogram1.mrc",
                "cbox_3d": f"{tmp_path}/AutoPick/job007/CBOX_3d/tomogram1_picks.cbox",
                "pixel_size": 5.3,
                "particle_diameter": 100.0,
                "kv": 300,
                "node_creator_queue": "node_creator",
                "session_id": message["session_id"],
                "autoproc_program_id": 0,
                "batch_size": 5000,
                "nr_classes": 5,
                "picker_id": None,
                "class2d_grp_uuid": 12,
                "class_uuids": {str(i): i + 6 for i in range(1, 6)},
                "next_job": 9,
                "feedback_queue": "murfey_feedback",
            },
            "recipes": ["em-tomo-class2d"],
        },
        new_connection=True,
    )
    mock_transport.send.assert_any_call(
        "processing_recipe",
        {
            "parameters": {
                "tomogram": message["tomogram"],
                "cbox_3d": message["cbox_3d"],
                "pixel_size": message["pixel_size"],
                "particle_diameter": 100.0,
                "kv": 300,
                "node_creator_queue": "node_creator",
                "session_id": message["session_id"],
                "autoproc_program_id": 0,
                "batch_size": 5000,
                "nr_classes": 5,
                "picker_id": None,
                "class2d_grp_uuid": 18,
                "class_uuids": {str(i): i + 12 for i in range(1, 6)},
                "next_job": 11,
                "feedback_queue": "murfey_feedback",
            },
            "recipes": ["em-tomo-class2d"],
        },
        new_connection=True,
    )
