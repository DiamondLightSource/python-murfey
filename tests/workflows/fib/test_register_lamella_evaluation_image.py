import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from sqlmodel import Session as SQLModelSession, select

import murfey.util.db as MurfeyDB
from murfey.util.config import MachineConfig
from murfey.workflows.fib.register_lamella_evaluation_image import (
    FIBImageMetadata,
    FIBLamellaImageInfo,
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
    found_sites = murfey_db_session.exec(select(MurfeyDB.ImagingSite)).all()
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


def test_run(
    mocker: MockerFixture,
    visit_dir: Path,
):
    # Set up parameters

    # Mock the logger
    mock_logger = mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.logger"
    )

    # Mock the database call
    mock_session = MagicMock(visit=visit_name, instrument_name=instrument_name)
    mock_murfey_db = MagicMock()
    mock_murfey_db.exec.return_value.one.return_value = mock_session

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

    # Create the test image file to use
    file = (
        visit_dir
        / "autotem"
        / visit_name
        / "Sites"
        / "Lamella"
        / "LamellaEvaluationImages"
        / "2026-04-16-02-39-40_drift_corrected_image_Polishing 2 - Electron Image.png"
    )

    # Mock the results of 'parse_image_metadata'
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
        "tilt_alpha": 0,
        "tilt_beta": 0,
        "pixels_x": 3072,
        "pixels_y": 2048,
        "pixel_size_x": 1e-6,
        "pixel_size_y": 1e-6,
    }
    metadata = FIBImageMetadata(
        visit_name=visit_name,
        file=file,
        **metadata_dict,
    )
    mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image.parse_image_metadata",
        return_value=metadata_dict,
    )

    # Mock the results of '_register_fib_image_site'
    mock_register_imaging_site = mocker.patch(
        "murfey.workflows.fib.register_lamella_evaluation_image._register_fib_imaging_site",
        return_value=MagicMock(),
    )

    # Construct the message to pass to the function
    message = {
        "register": "fib.register_lamella_evaluation_image",
        "session_id": session_id,
        "lamella_image_file": str(file),
    }
    fib_info = FIBLamellaImageInfo(**message)

    # Run function and check that expected calls were made
    result = run(message, mock_murfey_db)

    # Metadata should have been extracted and logged
    mock_logger.info.assert_any_call(
        "Extracted the following metadata from the image:\n"
        f"{json.dumps(metadata.model_dump(), indent=2, default=str)}"
    )
    # Imaging site registration function should have been called
    mock_register_imaging_site.assert_called_once_with(
        fib_info.session_id,
        metadata,
        mock_murfey_db,
    )
    assert result["success"]
