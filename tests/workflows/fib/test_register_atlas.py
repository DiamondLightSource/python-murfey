from pathlib import Path
from unittest.mock import MagicMock

import ispyb.sqlalchemy as ISPyBDB
import numpy as np
import PIL.Image
import pytest
from pytest_mock import MockerFixture
from sqlalchemy import select as sa_select
from sqlalchemy.orm import Session as SQLAlchemySession
from sqlmodel import Session as SQLModelSession, select as sm_select

import murfey.util.db as MurfeyDB
from murfey.util.fib import get_slot_number
from murfey.util.models import FIBImageMetadata
from murfey.workflows.fib.register_atlas import run
from tests.conftest import ExampleVisit

session_id = 10
visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
instrument_name = ExampleVisit.instrument_name


@pytest.fixture
def visit_dir(tmp_path: Path):
    visit_dir = tmp_path / "data/2020" / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    return visit_dir


def test_run_with_db(
    mocker: MockerFixture,
    visit_dir: Path,
    murfey_db_session: SQLModelSession,
    ispyb_db_session: SQLAlchemySession,
    mock_ispyb_credentials,
):
    rotation_offset = -75
    test_files = (
        visit_dir / "maps/LayersData/Layer/Electron Snapshot/Electron Snapshot.tiff",
        visit_dir
        / "maps/LayersData/Layer/Electron Snapshot/Electron Snapshot (2).tiff",
    )

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

    # Mock the MachineConfig
    mock_machine_config = MagicMock(
        calibrations={
            "rotation_offset": rotation_offset,
        }
    )
    mocker.patch(
        "murfey.workflows.fib.register_atlas.get_machine_config",
        return_value={
            instrument_name: mock_machine_config,
        },
    )

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

    # Mock the ISPYB connection when registering data collection group
    mocker.patch(
        "murfey.workflows.register_data_collection_group.ISPyBSession",
        return_value=ispyb_db_session,
    )

    # Patch the TransportManager object in the workflows called
    from murfey.server.ispyb import TransportManager

    mocker.patch(
        "murfey.server._transport_object", new=TransportManager("PikaTransport")
    )

    # Mock the metadata returned from the image file
    import murfey.workflows.fib.register_atlas

    extracted = {
        "voltage": 2000,
        "shift_x": 0,
        "shift_y": 0,
        "len_x": 0.003072,
        "len_y": 0.002048,
        "pos_x": 0.003,
        "pos_y": 0.0003,
        "pos_z": 0.01,
        "rotation": -1.309,
        "tilt_alpha": 0.8,
        "tilt_beta": 0,
        "pixels_x": 3072,
        "pixels_y": 2048,
        "pixel_size_x": 1e-6,
        "pixel_size_y": 1e-6,
    }
    extracted["slot_number"] = get_slot_number(
        x=extracted["pos_x"],
        y=extracted["pos_y"],
        rotation=extracted["rotation"],
        rotation_offset=rotation_offset,
    )
    mock_metadata = [
        FIBImageMetadata(
            visit_name=visit_name,
            file=test_file,
            **extracted,
        )
        for test_file in test_files
    ]
    mock_parse = mocker.patch(
        "murfey.workflows.fib.register_atlas.parse_image_metadata",
        return_value=extracted,
    )
    spy_register = mocker.spy(
        murfey.workflows.fib.register_atlas,
        "_register_fib_imaging_site",
    )

    # Mock 'PIL.Image.open' and create a test image
    mocker.patch(
        "murfey.workflows.fib.register_atlas.PIL.Image.open",
        return_value=PIL.Image.fromarray(np.ones((2048, 1152), dtype=np.uint8)),
    )

    # Run the function and check that it's run through to completion
    for test_file in test_files:
        run(
            message={
                "register": "fib.register_atlas",
                "session_id": session_id,
                "atlas_file": str(test_file),
            },
            murfey_db=murfey_db_session,
        )
    assert mock_parse.call_count == len(test_files)
    assert spy_register.call_count == len(test_files)

    # Murfey's ImagingSite should have an entry
    search_results = murfey_db_session.exec(
        sm_select(MurfeyDB.ImagingSite).where(
            MurfeyDB.ImagingSite.session_id == session_id
        )
    ).all()
    assert len(search_results) == 1
    assert search_results[0].image_path == str(mock_metadata[-1].file)

    # Murfey's DataCollectionGroup should have an entry
    murfey_dcg_search = murfey_db_session.exec(
        sm_select(MurfeyDB.DataCollectionGroup).where(
            MurfeyDB.DataCollectionGroup.session_id == session_id
        )
    ).all()
    assert len(murfey_dcg_search) == 1

    # ISPyB's DataCollectionGroup should have an entry
    murfey_dcg = murfey_dcg_search[0]
    ispyb_dcg_search = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.DataCollectionGroup).where(
                ISPyBDB.DataCollectionGroup.dataCollectionGroupId == murfey_dcg.id
            )
        )
        .scalars()
        .all()
    )
    assert len(ispyb_dcg_search) == 1

    # Atlas should have an entry
    ispyb_dcg = ispyb_dcg_search[0]
    ispyb_atlas_search = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.Atlas).where(
                ISPyBDB.Atlas.dataCollectionGroupId == ispyb_dcg.dataCollectionGroupId
            )
        )
        .scalars()
        .all()
    )
    assert len(ispyb_atlas_search) == 1
    ispyb_atlas_entry = ispyb_atlas_search[0]
    assert ispyb_atlas_entry.atlasImage.endswith(
        f"atlas_{str(mock_metadata[-1].slot_number).zfill(2)}.png"
    )
