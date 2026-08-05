import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.workflow_sim import SIMDataFile, request_sim_processing
from murfey.util.config import MachineConfig

# Global variables
instrument_name = "sim"
visit_name = "cm12345-6"
session_id = 1


@pytest.fixture
def visit_dir(tmp_path: Path):
    visit_dir = tmp_path / "data" / "2020" / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    return visit_dir


@pytest.mark.parametrize("has_transport_object", (True, False))
def test_request_sim_processing(
    mocker: MockerFixture,
    visit_dir: Path,
    has_transport_object: bool,
):
    # Set up the test file and output directory
    test_file = visit_dir / "raw" / "grid_1" / "test_file"
    sim_data = SIMDataFile(**{"file": str(test_file)})
    output_dir = visit_dir / "processed" / "grid_1"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Mock the logger
    mock_logger = mocker.patch("murfey.server.api.workflow_sim.logger")

    # Mock the Murfey DB
    mock_murfey_session = MagicMock(
        instrument_name=instrument_name,
        visit=visit_name,
    )
    mock_db = MagicMock()
    mock_db.exec.return_value.one.return_value = mock_murfey_session

    # Mock the machine config
    blue_params = {
        "wavelength": 452,
        "ls": 0.330,
        "beaddiam": 0.220,
    }
    green_params = {
        "wavelength": 525,
        "ls": 0.394,
    }
    red_params = {
        "wavelength": 605,
        "ls": 0.451,
    }
    far_red_params = {
        "wavelength": 655,
        "ls": 0.521,
    }
    pysimrecon_config = {
        "blue": blue_params,
        "green": green_params,
        "red": red_params,
        "far_red": far_red_params,
    }
    machine_config = MachineConfig(
        calibrations={"pysimrecon_config": pysimrecon_config},
    )
    mocker.patch(
        "murfey.server.api.workflow_sim.get_machine_config",
        return_value={instrument_name: machine_config},
    )

    # Mock the transport object
    if has_transport_object:
        mock_transport_object = MagicMock()
        mock_transport_object.feedback_queue = "dummy"
        mocker.patch(
            "murfey.server.api.workflow_sim._transport_object",
            mock_transport_object,
        )
    else:
        mocker.patch(
            "murfey.server.api.workflow_sim._transport_object",
            None,
        )

    # Run the function and check that the expected calls were made
    request_sim_processing(session_id=session_id, sim_data=sim_data, murfey_db=mock_db)

    # Check that the expected calls were made
    if has_transport_object:
        recipe = {
            "recipes": ["sim-reconstruction"],
            "parameters": {
                "file": f"{str(sim_data.file)}",
                "output_dir": str(output_dir),
                "blue_params": str(pysimrecon_config["blue"]),
                "green_params": str(pysimrecon_config["green"]),
                "red_params": str(pysimrecon_config["red"]),
                "far_red_params": str(pysimrecon_config["far_red"]),
                "session_id": session_id,
                "feedback_queue": "dummy",
            },
        }
        mock_logger.debug.assert_called_with(
            "Will submit the following message to 'processing_recipe':\n"
            f"{json.dumps(recipe, indent=2, default=str)}"
        )
        mock_transport_object.send.assert_called_with(
            queue="processing_recipe", message=recipe, new_connection=True
        )
    else:
        mock_logger.error.assert_called_with("No TransportManager object was set up")
