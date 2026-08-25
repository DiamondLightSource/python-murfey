import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.util.config import MachineConfig
from murfey.workflows.fib.register_lamella_evaluation_image import (
    FIBImageMetadata,
    run,
)
from tests.conftest import ExampleVisit

session_id = 10
visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
instrument_name = ExampleVisit.instrument_name


@pytest.fixture
def visit_dir(tmp_path: Path):
    visit_dir = tmp_path / "data/2020" / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    return visit_dir


def test_run(
    mocker: MockerFixture,
    visit_dir: Path,
):
    # Set up parameters
    project_name = "some_project"

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
        / project_name
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

    # Construct the message to pass to the function
    message = {
        "register": "fib.register_lamella_evaluation_image",
        "session_id": session_id,
        "lamella_image_file": str(file),
    }

    # Run function and check that expected calls were made
    result = run(message, mock_murfey_db)
    mock_logger.info.assert_called_with(
        "Extracted the following metadata from the image:\n"
        f"{json.dumps(metadata.model_dump(), indent=2, default=str)}"
    )
    assert result["success"]
