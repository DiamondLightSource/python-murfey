from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import PIL.Image
import pytest
from ispyb.sqlalchemy import _auto_db_schema as ISPyBDB
from pytest_mock import MockerFixture
from sqlalchemy import select as sa_select
from sqlalchemy.orm import Session as SQLAlchemySession
from sqlmodel import Session as SQLModelSession, select as sm_select

import murfey.util.db as MurfeyDB
import murfey.workflows.fib.register_lamella_evaluation_image
from murfey.server.ispyb import TransportManager
from murfey.util.config import MachineConfig
from murfey.util.models import FIBImageMetadata
from murfey.workflows.fib.register_lamella_evaluation_image import (
    _register_fib_imaging_site,
    run,
)
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


@pytest.mark.parametrize(
    "test_params",
    (  # Site exists | Newer site
        (True, True),
        (True, False),
        (False, False),
    ),
)
def test_register_fib_imaging_site_with_db(
    test_params: tuple[bool, bool],
    visit_dir: Path,
    murfey_db_session: SQLModelSession,
):
    # Unpack test params
    has_existing_entry, newer_insert = test_params

    # Register a Session for this test
    murfey_session = MurfeyDB.Session(
        id=session_id,
        visit=visit_name,
        name=visit_name,
        instrument_name=instrument_name,
        started=True,
    )
    murfey_db_session.add(murfey_session)
    murfey_db_session.commit()

    # Lamella directory
    lamella_dir = (
        visit_dir
        / "autotem"
        / visit_name
        / "Sites"
        / "Lamella"
        / "LamellaEvaluationImages"
    )
    # Standard metadata to use
    metadata_dict = {
        "voltage": 2000,
        "shift_x": 0,
        "shift_y": 0,
        "len_x": 0.003072,
        "len_y": 0.002048,
        "pos_x": -0.003,
        "pos_y": 0.003,
        "pos_z": 0.01,
        "rotation": 1.833,
        "slot_number": 2,
        "lamella_number": 1,
        "tilt_alpha": 0,
        "tilt_beta": 0,
        "pixels_x": 3072,
        "pixels_y": 2048,
        "pixel_size_x": 1e-6,
        "pixel_size_y": 1e-6,
    }

    # Create older existing entry
    if has_existing_entry:
        existing_timestamp = f"2026-04-16-02-39-{30 if newer_insert else 50}"
        existing_file = (
            lamella_dir
            / f"{existing_timestamp}_drift_corrected_image_Polishing 2 - Electron Image.png"
        )
        existing_metadata = FIBImageMetadata(
            visit_name=visit_name,
            file=existing_file,
            **metadata_dict,
        )
        existing_site = MurfeyDB.ImagingSite(
            session_id=session_id,
            site_name=existing_metadata.site_name,
            data_type="grid_square",
        )
        existing_site = populate_fib_imaging_site_entry(
            existing_site, existing_metadata
        )
        murfey_db_session.add(existing_site)
        murfey_db_session.commit()

    # Create the test image file to register
    file = (
        lamella_dir
        / "2026-04-16-02-39-40_drift_corrected_image_Polishing 2 - Electron Image.png"
    )
    metadata = FIBImageMetadata(
        visit_name=visit_name,
        file=file,
        **metadata_dict,
    )

    # Run the function and check that results are as expected
    _register_fib_imaging_site(
        session_id=session_id,
        metadata=metadata,
        murfey_db=murfey_db_session,
    )

    # Only one entry should exist
    found_sites = murfey_db_session.exec(sm_select(MurfeyDB.ImagingSite)).all()
    assert len(found_sites) == 1

    # Key parameters should be populated
    registered_site = found_sites[0]
    assert registered_site.session_id == session_id
    assert registered_site.data_type == "grid_square"
    assert registered_site.site_name == metadata.site_name
    if has_existing_entry and not newer_insert:
        assert registered_site.image_path != str(file)
    else:
        assert registered_site.image_path == str(file)


@pytest.mark.parametrize(
    "test_params",
    (  # Atlas registered | Slot number | Lamella number
        (True, 1, 1),
        (False, 2, 2),
    ),
)
def test_run_with_db(
    mocker: MockerFixture,
    test_params: tuple[bool, int, int],
    visit_dir: Path,
    murfey_db_session: SQLModelSession,
    ispyb_db_session: SQLAlchemySession,
    mock_ispyb_credentials,
):
    # Unpack test params
    atlas_registered, slot_number, lamella_number = test_params

    # Construct expected tag and site name
    expected_dcg_name = f"{visit_name}/grid_{slot_number}"
    expected_site_name = f"{expected_dcg_name}/lamella_{lamella_number}"

    # Register a Session for this test
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

    # Create placeholder metadata dictionary
    pixel_size = 1e-6
    metadata_dict = {
        "voltage": 2000,
        "shift_x": 0,
        "shift_y": 0,
        "len_x": 0.001500,
        "len_y": 0.001000,
        "pos_x": 0.003 * (-1 if slot_number > 1 else 1),
        "pos_y": 0.003,
        "pos_z": 0.01,
        "rotation": 1.833,
        "slot_number": slot_number,
        "lamella_number": lamella_number,
        "tilt_alpha": 0,
        "tilt_beta": 0,
        "pixels_x": 1500,
        "pixels_y": 1000,
        "pixel_size_x": pixel_size,
        "pixel_size_y": pixel_size,
    }

    # Create and populate an ImagingSite entry for the atlas if toggled
    if atlas_registered:
        atlas_metadata_dict = metadata_dict.copy()
        atlas_metadata_dict["len_x"] = 0.002400
        atlas_metadata_dict["len_y"] = 0.001600
        atlas_metadata_dict["pixels_x"] = int(atlas_metadata_dict["len_x"] / pixel_size)
        atlas_metadata_dict["pixels_y"] = int(atlas_metadata_dict["len_y"] / pixel_size)

        atlas_metadata = FIBImageMetadata(
            visit_name=visit_name,
            file=visit_dir / "some_file.tif",
            **atlas_metadata_dict,
        )

        atlas_entry = MurfeyDB.ImagingSite(
            session_id=session_id,
            site_name=expected_dcg_name,
            image_path=str(atlas_metadata.file),
            dcg_name=expected_dcg_name,
            data_type="atlas",
        )
        populate_fib_imaging_site_entry(atlas_entry, atlas_metadata)
        murfey_db_session.add(atlas_entry)

    # Commit all needed changes
    murfey_db_session.commit()

    # Mock the logger
    mock_logger = mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.logger"
    )

    # Mock the machine config
    machine_config = MachineConfig(
        calibrations={
            "rotation_offset": -75.0,
        }
    )
    mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.get_machine_config",
        return_value={instrument_name: machine_config},
    )

    # Mock the ISPyB connection where the TransportManager class is located
    mocker.patch(
        "murfey.server.ispyb.get_security_config",
        return_value=MagicMock(ispyb_credentials=mock_ispyb_credentials),
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

    # Create the test image files and their thumbnails
    lamella_folder = "Lamella"
    if lamella_number > 1:
        lamella_folder += f" ({lamella_number})"
    raw_lamella_dir = (
        visit_dir
        / "autotem"
        / visit_name
        / "Sites"
        / lamella_folder
        / "LamellaEvaluationImages"
    )
    raw_lamella_dir.mkdir(parents=True, exist_ok=True)
    processed_dir = (
        visit_dir
        / "processed"
        / visit_name
        / f"grid_{slot_number}"
        / "lamella_evaluation_images"
    )
    processed_dir.mkdir(parents=True, exist_ok=True)

    files: list[Path] = []
    thumbnails: list[Path] = []
    for file_name in [
        "2026-04-16-02-39-38_drift_corrected_image_Finer Milling - Electron Image.png",
        "2026-04-16-02-39-40_drift_corrected_image_Polishing 2 - Electron Image.png",
    ]:
        file = raw_lamella_dir / file_name
        file.touch()
        files.append(file)

        timestamp, step_name = file_name.split("_drift_corrected_image_")
        step_name = step_name.split(" - ")[0].replace(" ", "_").lower()
        thumbnail = (
            processed_dir / f"lamella_{lamella_number}_{timestamp}_{step_name}.png"
        )
        thumbnail.touch()
        thumbnails.append(thumbnail)

    # Mock the expected metadata returns
    mock_parse = mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.parse_image_metadata",
        return_value=metadata_dict,
    )

    # Mock 'PIL.Image.open' and create a test image
    mock_open = mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.PIL.Image.open"
    )
    mock_open.__enter__.return_value = PIL.Image.fromarray(
        np.ones((1500, 1000), dtype=np.uint8)
    )

    # Set up spies for the functions called by 'run()'
    spy_thumbnail = mocker.spy(
        murfey.workflows.fib.register_lamella_evaluation_image,
        "_make_thumbnail",
    )
    spy_register = mocker.spy(
        murfey.workflows.fib.register_lamella_evaluation_image,
        "_register_fib_imaging_site",
    )

    # Run function and check that expected calls were made
    for file in files:
        # Construct the message to pass to the function
        message = {
            "register": "fib.register_lamella_evaluation_image",
            "session_id": session_id,
            "lamella_image_file": str(file),
        }
        result = run(message, murfey_db_session)
        assert result["success"]

    # 'PIL.Image.open' should have been called for each image
    assert mock_parse.call_count == len(files)
    assert spy_thumbnail.call_count == len(files)
    assert spy_register.call_count == len(files)

    # Both thumbnails should have been generated
    for thumbnail in thumbnails:
        assert thumbnail.is_file()

    # There should only be one ImagingSite entry associated with the visit
    imaging_sites = murfey_db_session.exec(
        sm_select(MurfeyDB.ImagingSite)
        .where(MurfeyDB.ImagingSite.session_id == session_id)
        .where(MurfeyDB.ImagingSite.data_type == "grid_square")
    ).all()
    assert len(imaging_sites) == 1

    # The later image ("Polishing 2") should have been registered
    imaging_site = imaging_sites[0]
    assert (
        imaging_site.image_path is not None and "Polishing 2" in imaging_site.image_path
    )
    assert (
        imaging_site.thumbnail_path is not None
        and "polishing_2" in imaging_site.thumbnail_path
    )

    # Site name should have been constructed correctly
    assert imaging_site.dcg_name == expected_dcg_name
    assert imaging_site.site_name == expected_site_name

    # Murfey's DataCollectionGroup should have an entry
    murfey_dcg_search = murfey_db_session.exec(
        sm_select(MurfeyDB.DataCollectionGroup).where(
            MurfeyDB.DataCollectionGroup.session_id == session_id
        )
    ).all()
    assert len(murfey_dcg_search) == 1

    # Check that the Murfey DataCollectionGroup entry was populated correctly
    murfey_dcg = murfey_dcg_search[0]
    assert murfey_dcg.tag == expected_dcg_name

    # ISPyB's DataCollectionGroup should have an entry
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

    # Check that the ISPyB DataCollectionGroup entry was populated correctly
    ispyb_dcg = ispyb_dcg_search[0]
    assert ispyb_dcg.experimentTypeId == 46

    # ISPyB's Atlas should have an entry
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

    # GridSquare should be registered if an atlas ImagingSite exists
    if atlas_registered:
        # ISPyB's GridSquare should have an entry
        ispyb_atlas = ispyb_atlas_search[0]
        ispyb_gs_search = (
            ispyb_db_session.execute(
                sa_select(ISPyBDB.GridSquare).where(
                    ISPyBDB.GridSquare.atlasId == ispyb_atlas.atlasId
                )
            )
            .scalars()
            .all()
        )
        assert len(ispyb_gs_search) == 1

        # Murfey's GridSquare should also have an entry
        murfey_gs_search = murfey_db_session.exec(
            sm_select(MurfeyDB.GridSquare).where(
                MurfeyDB.GridSquare.session_id == session_id
            )
        ).all()
        assert len(murfey_gs_search) == 1
        # Check that it's populated correctly
        murfey_gs = murfey_gs_search[0]
        assert murfey_gs.tag == expected_dcg_name
        assert murfey_gs.name == 1
    else:
        mock_logger.info.assert_any_call(
            f"No atlas has been registered for data collection group {expected_dcg_name!r} yet"
        )
