from pathlib import Path

import numpy as np
import PIL.Image
import pytest
from pytest_mock import MockerFixture
from sqlmodel import Session as SQLModelSession, select

import murfey.util.db as MurfeyDB
from murfey.util.config import MachineConfig
from murfey.workflows.fib.register_lamella_evaluation_image import (
    FIBImageMetadata,
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


def test_run_with_db(
    mocker: MockerFixture,
    visit_dir: Path,
    murfey_db_session: SQLModelSession,
):
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

    # Create the test image files and their thumbnails
    raw_lamella_dir = (
        visit_dir
        / "autotem"
        / visit_name
        / "Sites"
        / "Lamella"
        / "LamellaEvaluationImages"
    )
    raw_lamella_dir.mkdir(parents=True, exist_ok=True)
    processed_dir = (
        visit_dir / "processed" / visit_name / "grid_2" / "lamella_evaluation_images"
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
        thumbnail = processed_dir / f"lamella_1_{timestamp}_{step_name}.png"
        thumbnail.touch()
        thumbnails.append(thumbnail)

    # Mock the expected metadata returns
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
        "pixels_x": 1500,
        "pixels_y": 1000,
        "pixel_size_x": 1e-6,
        "pixel_size_y": 1e-6,
    }
    mocker.patch(
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
    assert mock_open.call_count == len(files)

    # Both thumbnails should have been generated
    for thumbnail in thumbnails:
        assert thumbnail.is_file()

    # There should only be one ImagingSite entry associated with the visit
    imaging_sites = murfey_db_session.exec(
        select(MurfeyDB.ImagingSite)
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
    assert imaging_site.site_name == f"{visit_name}/grid_2/lamella_1"
