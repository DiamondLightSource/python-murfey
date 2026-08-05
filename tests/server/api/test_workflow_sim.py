import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.workflow_sim import SIMDataFile, request_sim_reconstruction
from murfey.util import sanitise_path
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


@pytest.mark.parametrize(
    "test_params",
    (  # Transport object | DB query success | PySIMRecon config found | Output dir found
        # Successful case
        (True, True, True, True),
        (False, True, True, True),  # No transport object
        (True, False, True, True),  # DB query failed
        (True, True, False, True),  # No PySIMRecon config
        (True, True, True, False),  # Incorrect output dir
    ),
)
def test_request_sim_reconstruction(
    mocker: MockerFixture,
    tmp_path: Path,
    visit_dir: Path,
    test_params: tuple[bool, bool, bool, bool],
):
    # Unpack test params
    (
        has_transport_object,
        db_query_success,
        pysimrecon_configured,
        output_dir_success,
    ) = test_params

    # Set up the test file and output directory
    test_file = (
        visit_dir / "raw" / "grid_1" / "test_file"
        if output_dir_success
        else tmp_path / "dummy"  # Provide incorrect file path
    )
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
    if db_query_success:
        mock_db.exec.return_value.one.return_value = mock_murfey_session
    else:
        mock_db.exec.return_value.one.side_effect = Exception("Something went wrong")

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
        calibrations={"pysimrecon_config": pysimrecon_config}
        if pysimrecon_configured
        else {},
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
    request_sim_reconstruction(
        session_id=session_id, sim_data=sim_data, murfey_db=mock_db
    )

    # Check that the expected calls were made
    # The parameters are toggled 'False' one at a time
    if not has_transport_object:
        mock_logger.error.assert_called_with("No TransportManager object was set up")
    elif not db_query_success:
        mock_logger.error.assert_called_with(
            "Error querying session information from database", exc_info=True
        )
        mock_transport_object.send.assert_not_called()
    elif not pysimrecon_configured:
        mock_logger.error.assert_called_with(
            f"No PySIMRecon configuration found for {instrument_name}"
        )
        mock_transport_object.send.assert_not_called()
    elif not output_dir_success:
        mock_logger.error.assert_called_with(
            "Could not determine the output directory to save the cryoSIM file "
            f"{sanitise_path(sim_data.file)} to"
        )
        mock_transport_object.send.assert_not_called()
    else:
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
