import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.workflow_sim import SIMDataFile, request_sim_processing


@pytest.mark.parametrize("has_transport_object", (True, False))
def test_request_sim_processing(
    mocker: MockerFixture, tmp_path: Path, has_transport_object: bool
):
    # Set up the variables
    session_id = 1
    sim_data = SIMDataFile(**{"file": str(tmp_path / "dummy")})

    # Mock the logger
    mock_logger = mocker.patch("murfey.server.api.workflow_sim.logger")

    # Mock the transport object
    if has_transport_object:
        mock_transport_object = MagicMock()
        mock_transport_object.feedback_queue = "dummy"
        mocker.patch("murfey.server._transport_object", mock_transport_object)
    else:
        mocker.patch("murfey.server._transport_object", None)

    # Run the function and check that the expected calls were made
    request_sim_processing(
        session_id=session_id,
        sim_data=sim_data,
    )

    # Check that the expected calls were made
    if has_transport_object:
        recipe = {
            "recipes": ["sim-process-data"],
            "parameters": {
                "session_id": session_id,
                "file": f"{sim_data.file}",
                "feedback_queue": "dummy",
            },
        }
        mock_logger.debug.assert_called_with(
            "Will submit the following message to 'processing_recipe':\n"
            f"{json.dumps(recipe, indent=2, default=str)}"
        )
        # mock_transport_object.send.assert_called_with(
        #     queue="processing_recipe", message=recipe, new_connection=True
        # )
    else:
        mock_logger.error.assert_called_with("No TransportManager object was set up")
