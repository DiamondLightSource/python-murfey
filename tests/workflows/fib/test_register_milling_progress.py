from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import ispyb.sqlalchemy._auto_db_schema as ISPyBDB
from pytest_mock import MockerFixture
from sqlalchemy import select as sa_select
from sqlalchemy.orm import Session as SQLAlchemySession
from sqlmodel import Session as SQLModelSession, select as sm_select

import murfey.util.db as MurfeyDB
from murfey.workflows.fib.register_milling_progress import run
from tests.conftest import ExampleVisit

# Module-wide variables
session_id = 10
visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
instrument_name = ExampleVisit.instrument_name


# Construct test FIB AutoTEM site info for reading (copied and adapted from actual session)
site_info = {
    "project_name": visit_name,
    "site_name": "Lamella",
    "site_number": 1,
    "stage_info": {
        "preparation_site": {
            "x": -0.0031091224743875403,
            "y": 0.00420867925495798,
            "z": 0.0323644854106331,
            "rotation": 285.003247202109,
            "tilt_alpha": 25.9996646026832,
            "slot_number": 1,
        },
        "chunk_site": {
            "x": -0.0030037500000000003,
            "y": 0.004293,
            "z": 0.032350405092592606,
            "rotation": 285.003247202109,
            "tilt_alpha": -0.000134158926728586,
            "slot_number": 1,
        },
        "thinning_site": {
            "x": -0.0030037500000000003,
            "y": 0.004293,
            "z": 0.032350405092592606,
            "rotation": 285.003247202109,
            "tilt_alpha": -0.000134158926728586,
            "slot_number": 1,
        },
        "chunk_coincidence_params": {
            "x": -0.0030048260286678298,
            "y": 0.004308828126160981,
            "z": 0.0323400707790533,
            "rotation": 285.003247202109,
            "tilt_alpha": -0.000134158926728586,
            "slot_number": 1,
        },
        "thinning_params": {
            "x": -0.0030037500000000003,
            "y": 0.004293,
            "z": 0.032350405092592606,
            "rotation": 285.003247202109,
            "tilt_alpha": -0.000134158926728586,
            "slot_number": 1,
        },
    },
    "steps": {
        "eucentric_tilt": {
            "step_name": "Eucentric Tilt",
            "recipe_name": "Preparation",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 328.6657458,
        },
        "artificial_features": {
            "step_name": "Artificial Features",
            "recipe_name": "Preparation",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
            "beam_type": "Ion",
            "voltage": 30000.0,
            "current": 5e-10,
        },
        "milling_angle": {
            "step_name": "Milling Angle",
            "recipe_name": "Preparation",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 208.4942922,
            "milling_angle": 12.0,
        },
        "image_acquisition": {
            "step_name": "Image Acquisition",
            "recipe_name": "Preparation",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 19.7872887,
            "site_location_type": "Chunk",
        },
        "lamella_placement": {
            "step_name": "Lamella Placement",
            "recipe_name": "Preparation",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
        },
        "delay_1": {
            "step_name": "Delay",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
        },
        "reference_definition": {
            "step_name": "Reference Definition",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 75.6320442,
            "site_location_type": "Chunk",
        },
        "reference_definition_electron": {
            "step_name": "Electron Reference Definition",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 131.7462301,
        },
        "stress_relief_cuts": {
            "step_name": "Stress Relief Cuts",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
            "beam_type": "Ion",
            "voltage": 30000.0,
            "current": 1e-09,
            "depth_correction": 3.0,
        },
        "reference_redefinition_1": {
            "step_name": "Reference Redefinition 1",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
            "site_location_type": "Chunk",
        },
        "rough_milling": {
            "step_name": "Rough Milling",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 1.5929074719,
            "beam_type": "Ion",
            "voltage": 30000.0,
            "current": 1e-09,
            "depth_correction": 3.0,
            "lamella_offset": 2e-06,
            "trench_height_front": 5.6338138462765e-06,
            "trench_height_rear": 8.60473362403628e-06,
            "width_overlap_front_left": 2e-06,
            "width_overlap_front_right": 2e-06,
            "width_overlap_rear_left": 2e-06,
            "width_overlap_rear_right": 2e-06,
        },
        "rough_milling_electron": {
            "step_name": "Rough Milling - Electron Image",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
            "site_location_type": "Chunk",
            "beam_type": "Electron",
            "voltage": 2000.0,
            "current": 1.25e-11,
        },
        "reference_redefinition_2": {
            "step_name": "Reference Redefinition 2",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 105.1459769,
            "site_location_type": "Chunk",
        },
        "medium_milling": {
            "step_name": "Medium Milling",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 298.1471377,
            "beam_type": "Ion",
            "voltage": 30000.0,
            "current": 5e-10,
            "depth_correction": 2.0,
            "lamella_offset": 1.5e-06,
            "width_overlap_front_left": 1.5e-06,
            "width_overlap_front_right": 1.5e-06,
            "width_overlap_rear_left": 1.5e-06,
            "width_overlap_rear_right": 1.5e-06,
        },
        "medium_milling_electron": {
            "step_name": "Medium Milling - Electron Image",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
            "site_location_type": "Chunk",
            "beam_type": "Electron",
            "voltage": 2000.0,
            "current": 1.25e-11,
        },
        "fine_milling": {
            "step_name": "Fine Milling",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 397.414388,
            "beam_type": "Ion",
            "voltage": 30000.0,
            "current": 3e-10,
            "depth_correction": 2.0,
            "lamella_offset": 1e-06,
            "width_overlap_front_left": 1e-06,
            "width_overlap_front_right": 1e-06,
            "width_overlap_rear_left": 1e-06,
            "width_overlap_rear_right": 1e-06,
        },
        "fine_milling_electron": {
            "step_name": "Fine Milling - Electron Image",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
            "site_location_type": "Chunk",
            "beam_type": "Electron",
            "voltage": 2000.0,
            "current": 1.25e-11,
        },
        "finer_milling": {
            "step_name": "Finer Milling",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 887.9686893,
            "beam_type": "Ion",
            "voltage": 30000.0,
            "current": 1e-10,
            "depth_correction": 2.0,
            "lamella_offset": 4.0000000000000003e-07,
            "width_overlap_front_left": 5.000000000000001e-07,
            "width_overlap_front_right": 5.000000000000001e-07,
            "width_overlap_rear_left": 5.000000000000001e-07,
            "width_overlap_rear_right": 5.000000000000001e-07,
        },
        "finer_milling_electron": {
            "step_name": "Finer Milling - Electron Image",
            "recipe_name": "Milling",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 59.2296577,
            "site_location_type": "Chunk",
            "beam_type": "Electron",
            "voltage": 2000.0,
            "current": 2.5e-11,
        },
        "delay_2": {
            "step_name": "Delay",
            "recipe_name": "Thinning",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
        },
        "polishing_1": {
            "step_name": "Polishing 1",
            "recipe_name": "Thinning",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 680.5567315,
            "beam_type": "Ion",
            "voltage": 30000.0,
            "current": 5e-11,
            "depth_correction": 2.0,
            "lamella_offset": 2.5000000000000004e-07,
            "width_overlap_front_left": 0.0,
            "width_overlap_front_right": 0.0,
            "width_overlap_rear_left": 0.0,
            "width_overlap_rear_right": 0.0,
        },
        "polishing_1_electron": {
            "step_name": "Polishing 1 - Electron Image",
            "recipe_name": "Thinning",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
            "site_location_type": "Thinning",
            "beam_type": "Electron",
            "voltage": 2000.0,
            "current": 1.25e-11,
        },
        "polishing_2": {
            "step_name": "Polishing 2",
            "recipe_name": "Thinning",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 1.170488927,
            "beam_type": "Ion",
            "voltage": 30000.0,
            "current": 3e-11,
            "depth_correction": 1.5,
            "lamella_offset": 0.0,
            "width_overlap_front_left": 0.0,
            "width_overlap_front_right": 0.0,
            "width_overlap_rear_left": 0.0,
            "width_overlap_rear_right": 0.0,
        },
        "polishing_2_ion": {
            "step_name": "Polishing 2 - Ion Image",
            "recipe_name": "Thinning",
            "is_enabled": False,
            "status": "None",
            "execution_time": 0.0,
            "site_location_type": "Thinning",
            "beam_type": "Ion",
            "voltage": 30000.0,
            "current": 3e-11,
        },
        "polishing_2_electron": {
            "step_name": "Polishing 2 - Electron Image",
            "recipe_name": "Thinning",
            "is_enabled": False,
            "status": "Finished",
            "execution_time": 56.9180832,
            "site_location_type": "Thinning",
            "beam_type": "Electron",
            "voltage": 2000.0,
            "current": 2.5e-11,
        },
    },
}
# Construct the RabbitMQ message received
message = {
    "register": "fib.register_milling_progress",
    "session_id": session_id,
    "site_info": site_info,
}


def test_run_with_db(
    mocker: MockerFixture,
    murfey_db_session: SQLModelSession,
    ispyb_db_session: SQLAlchemySession,
    mock_ispyb_credentials: Path,
):
    # Add a test visit to the database
    if not (
        session_entry := murfey_db_session.exec(
            sm_select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
        ).one_or_none()
    ):
        session_entry = MurfeyDB.Session(id=session_id)
    session_entry.name = visit_name
    session_entry.visit = visit_name
    session_entry.instrument_name = instrument_name

    murfey_db_session.add(session_entry)
    murfey_db_session.commit()

    # Mock the ISPyB connection where the TransportManager class is located
    mock_security_config = MagicMock()
    mock_security_config.ispyb_credentials = mock_ispyb_credentials
    mocker.patch(
        "murfey.server.ispyb.get_security_config",
        return_value=mock_security_config,
    )
    mocker.patch(
        "murfey.server.ispyb.ISPyBSession",
        return_value=ispyb_db_session,
    )

    # Mock the ISPYB connection when registering DataCollectionGroup
    mocker.patch(
        "murfey.workflows.register_data_collection_group.ISPyBSession",
        return_value=ispyb_db_session,
    )

    # Patch the TransportManager object in the workflows called
    from murfey.server.ispyb import TransportManager

    mocker.patch(
        "murfey.workflows.register_data_collection_group._transport_object",
        new=TransportManager("PikaTransport"),
    )
    mocker.patch(
        "murfey.workflows.fib.register_milling_progress._transport_object",
        new=TransportManager("PikaTransport"),
    )

    # Run the workflow twice (fresh insert and update existing)
    for i in range(2):
        result = run(
            message=message,
            murfey_db=murfey_db_session,
        )
    assert result.get("success", False)

    # There should be a DataCollectionGroup entry in Murfey
    dcg_murfey = murfey_db_session.exec(
        sm_select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
        .where(
            MurfeyDB.DataCollectionGroup.tag == f"{site_info['project_name']}--slot_1"
        )
    ).one_or_none()
    assert dcg_murfey is not None

    # There should be a DataCollectionGroup entry in ISPyB
    dcg_ispyb = ispyb_db_session.execute(
        sa_select(ISPyBDB.DataCollectionGroup).where(
            ISPyBDB.DataCollectionGroup.dataCollectionGroupId == dcg_murfey.id
        )
    ).scalar_one_or_none()
    assert dcg_ispyb is not None

    # There should be an Atlas in ISPyB
    atlas_ispyb = ispyb_db_session.execute(
        sa_select(ISPyBDB.Atlas).where(
            ISPyBDB.Atlas.dataCollectionGroupId == dcg_ispyb.dataCollectionGroupId
        )
    ).scalar_one_or_none()
    assert atlas_ispyb is not None

    # There should be one GridSquare entry in ISPyB per lamella site tested
    gs_ispyb_rows = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.GridSquare).where(
                ISPyBDB.GridSquare.atlasId == atlas_ispyb.atlasId
            )
        )
        .scalars()
        .all()
    )
    assert len(gs_ispyb_rows) >= 1
    gs_ispyb = gs_ispyb_rows[0]

    steps = cast(dict[str, Any], site_info["steps"])

    # There should be one MillingStep entry in ISPyB for each step in the message
    milling_step_ispyb_rows = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.MillingStep).where(
                ISPyBDB.MillingStep.gridSquareId == gs_ispyb.gridSquareId
            )
        )
        .scalars()
        .all()
    )
    assert len(milling_step_ispyb_rows) == len(steps.keys())

    # There should be the same thing in Murfey
    milling_step_murfey_rows = murfey_db_session.exec(
        sm_select(MurfeyDB.MillingStep).where(
            MurfeyDB.MillingStep.grid_square_id == gs_ispyb.gridSquareId
        )
    ).all()
    assert len(milling_step_murfey_rows) == len(steps.keys())
