from pathlib import Path
from typing import Any
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
import murfey.workflows.fib.register_atlas
from murfey.server.ispyb import TransportManager
from murfey.util.fib import get_slot_number, number_from_name
from murfey.util.models import FIBImageMetadata
from murfey.workflows.fib.register_atlas import run
from murfey.workflows.fib.shared import populate_fib_imaging_site_entry
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
    atlas_files = (
        visit_dir / "maps/LayersData/Layer/Electron Snapshot/Electron Snapshot.tiff",
        visit_dir
        / "maps/LayersData/Layer/Electron Snapshot/Electron Snapshot (2).tiff",
    )

    # Mock metadata template to use for the images
    # These will be constant for all metadata
    metadata: dict[str, Any] = {
        "voltage": 2000,
        "shift_x": 0,
        "shift_y": 0,
        "pos_x": 0.003,
        "pos_y": 0.0003,
        "pos_z": 0.01,
        "rotation": -1.309,
        "tilt_alpha": 0.8,
        "tilt_beta": 0,
        "pixels_x": 1500,
        "pixels_y": 1000,
        "thumbnail_pixels_x": 512,
        "thumbnail_pixels_y": 341,
    }
    slot_number = get_slot_number(
        x=metadata["pos_x"],
        y=metadata["pos_y"],
        rotation=metadata["rotation"],
        rotation_offset=rotation_offset,
    )
    metadata["slot_number"] = slot_number

    # Add a test visit to the database
    if not (
        murfey_session := murfey_db_session.exec(
            sm_select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
        ).one_or_none()
    ):
        murfey_session = MurfeyDB.Session(id=session_id)
    murfey_session.name = visit_name
    murfey_session.visit = visit_name
    murfey_session.instrument_name = instrument_name

    murfey_db_session.add(murfey_session)

    # Mock the atlas metadata
    atlas_metadata_dict = metadata.copy()
    atlas_metadata_dict["len_x"] = 0.003000
    atlas_metadata_dict["len_y"] = 0.002000
    atlas_metadata_dict["pixel_size_x"] = (
        atlas_metadata_dict["len_x"] / atlas_metadata_dict["pixels_x"]
    )
    atlas_metadata_dict["pixel_size_y"] = (
        atlas_metadata_dict["len_y"] / atlas_metadata_dict["pixels_y"]
    )
    mock_atlas_metadata = [
        FIBImageMetadata(
            visit_name=visit_name,
            file=file,
            **atlas_metadata_dict,
        )
        for file in atlas_files
    ]

    # Add a test lamella image to the database
    lamella_metadata_dict = metadata.copy()
    lamella_metadata_dict["file"] = (
        visit_dir
        / "autotem"
        / visit_name
        / "Sites"
        / "Lamella"
        / "LamellaEvaluationImages"
        / "2026-04-30-15-11-43_drift_corrected_image_Polishing 2 - Electron Image.png"
    )
    lamella_metadata_dict["thumbnail_path"] = (
        visit_dir
        / "processed"
        / visit_name
        / f"grid_{slot_number}"
        / "lamella_evaluation_images"
        / "2026-04-30-15-11-43_polishing_2.png"
    )
    lamella_metadata_dict["lamella_number"] = 1
    lamella_metadata_dict["len_x"] = 0.000900
    lamella_metadata_dict["len_y"] = 0.000600
    lamella_metadata_dict["pixel_size_x"] = (
        lamella_metadata_dict["len_x"] / lamella_metadata_dict["pixels_x"]
    )
    lamella_metadata_dict["pixel_size_y"] = (
        lamella_metadata_dict["len_y"] / lamella_metadata_dict["pixels_y"]
    )
    lamella_metadata = FIBImageMetadata(visit_name=visit_name, **lamella_metadata_dict)

    lamella_site = MurfeyDB.ImagingSite(
        session_id=session_id,
        site_name=lamella_metadata.site_name,
        data_type="grid_square",
        dcg_name=mock_atlas_metadata[0].site_name,
    )
    lamella_site = populate_fib_imaging_site_entry(
        lamella_site,
        metadata=lamella_metadata,
    )

    murfey_db_session.add(lamella_site)

    # Commit all database changes
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
    mocker.patch(
        "murfey.server._transport_object", new=TransportManager("PikaTransport")
    )

    # Mock the metadata returned from the atlas file
    mock_parse = mocker.patch(
        "murfey.workflows.fib.register_atlas.parse_image_metadata",
        return_value=atlas_metadata_dict,
    )

    # Mock 'PIL.Image.open' and create a test image
    mock_open = mocker.patch("murfey.workflows.fib.register_atlas.PIL.Image.open")
    mock_open.__enter__.return_value = PIL.Image.fromarray(
        np.ones((1500, 1000), dtype=np.uint8)
    )
    # Build the name of the expected test images
    thumbnails: list[Path] = []
    for file in atlas_files:
        image_number = number_from_name(file.stem)
        thumbnail = (
            visit_dir
            / "processed"
            / visit_name
            / f"grid_{slot_number}"
            / "atlas"
            / f"atlas_{str(image_number).zfill(2)}.png"
        )
        thumbnail.parent.mkdir(parents=True, exist_ok=True)
        thumbnail.touch(exist_ok=True)
        thumbnails.append(thumbnail)

    # Set up spies for the different steps in 'run()'
    spy_thumbnail = mocker.spy(
        murfey.workflows.fib.register_atlas,
        "_make_thumbnail",
    )
    spy_register = mocker.spy(
        murfey.workflows.fib.register_atlas,
        "_register_fib_imaging_site",
    )

    # Run the function and check that it's run through to completion
    for test_file in atlas_files:
        run(
            message={
                "register": "fib.register_atlas",
                "session_id": session_id,
                "atlas_file": str(test_file),
            },
            murfey_db=murfey_db_session,
        )
    assert mock_parse.call_count == len(atlas_files)
    assert spy_thumbnail.call_count == len(atlas_files)
    for thumbnail in thumbnails:
        assert thumbnail.is_file()
    assert spy_register.call_count == len(atlas_files)

    # Murfey's ImagingSite table should have an entry
    atlas = murfey_db_session.exec(
        sm_select(MurfeyDB.ImagingSite)
        .where(MurfeyDB.ImagingSite.session_id == session_id)
        .where(MurfeyDB.ImagingSite.data_type == "atlas")
    ).one()
    assert atlas.image_path == str(mock_atlas_metadata[-1].file)

    # Murfey's DataCollectionGroup table should have an entry
    murfey_dcg = murfey_db_session.exec(
        sm_select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == mock_atlas_metadata[-1].site_name)
    ).one()

    # Murfey's GridSquare table should have an entry
    murfey_gs = murfey_db_session.exec(
        sm_select(MurfeyDB.GridSquare)
        .where(MurfeyDB.GridSquare.session_id == session_id)
        .where(MurfeyDB.GridSquare.tag == mock_atlas_metadata[-1].site_name)
    ).one()

    # ISPyB's DataCollectionGroup table should have an entry
    ispyb_dcg = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.DataCollectionGroup).where(
                ISPyBDB.DataCollectionGroup.dataCollectionGroupId == murfey_dcg.id
            )
        )
        .scalars()
        .one()
    )

    # ISPyB's Atlas table should have an entry
    ispyb_atlas = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.Atlas).where(
                ISPyBDB.Atlas.dataCollectionGroupId == ispyb_dcg.dataCollectionGroupId
            )
        )
        .scalars()
        .one()
    )
    assert ispyb_atlas.atlasImage.endswith(
        f"atlas_{str(mock_atlas_metadata[-1].slot_number).zfill(2)}.png"
    )

    # ISPyB's GridSquare table should have an entry
    ispyb_gs = (
        ispyb_db_session.execute(
            sa_select(ISPyBDB.GridSquare).where(
                ISPyBDB.GridSquare.gridSquareId == murfey_gs.id
            )
        )
        .scalars()
        .one()
    )
    assert ispyb_gs.gridSquareImage == str(lamella_metadata_dict["thumbnail_path"])
