from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.workflow_fib import (
    FIBAtlasFile,
    make_gif,
    register_fib_atlas,
)
from murfey.util.models import FIBGIFParameters


@pytest.mark.parametrize(
    "has_transport_object",
    (
        True,
        False,
    ),
)
def test_register_fib_atlas(
    mocker: MockerFixture,
    tmp_path: Path,
    has_transport_object: bool,
):
    # Set up the variables
    session_id = 1
    fib_atlas = FIBAtlasFile(**{"file": str(tmp_path / "dummy")})

    # Mock the logger
    mock_logger = mocker.patch("murfey.server.api.workflow_fib.logger")

    # Mock the transport object
    if has_transport_object:
        mock_transport_object = MagicMock()
        mock_transport_object.feedback_queue = "dummy"
        mocker.patch(
            "murfey.server.api.workflow_fib._transport_object",
            mock_transport_object,
        )
    else:
        mocker.patch(
            "murfey.server.api.workflow_fib._transport_object",
            None,
        )

    # Run the function and check that the expected calls were made
    register_fib_atlas(
        session_id=session_id,
        fib_atlas=fib_atlas,
    )

    # Check that the expected calls were made
    if has_transport_object:
        mock_transport_object.send.assert_called_with(
            "dummy",
            {
                "register": "fib.register_atlas",
                "session_id": session_id,
                "atlas_file": str(fib_atlas.file),
            },
        )
    else:
        mock_logger.error.assert_called_with("No TransportManager object was set up")


@pytest.mark.parametrize(
    "has_transport_object",
    (
        True,
        False,
    ),
)
@pytest.mark.asyncio
async def test_make_gif(
    mocker: MockerFixture,
    tmp_path: Path,
    has_transport_object: bool,
):
    # Set up the variables
    session_id = 1
    gif_params_dict = {
        "lamella_number": 1,
        "images": [
            str(tmp_path / "some_file.png"),
        ],
        "output_file": str(tmp_path / "target_file.gif"),
    }
    gif_params = FIBGIFParameters(**gif_params_dict)

    # Mock the logger
    mock_logger = mocker.patch("murfey.server.api.workflow_fib.logger")

    # Mock the transport object
    if has_transport_object:
        mock_transport_object = MagicMock()
        mock_transport_object.feedback_queue = "dummy"
        mocker.patch(
            "murfey.server.api.workflow_fib._transport_object",
            mock_transport_object,
        )
    else:
        mocker.patch(
            "murfey.server.api.workflow_fib._transport_object",
            None,
        )

    # Run the function and check that the expected calls were made
    await make_gif(
        session_id=session_id,
        gif_params=gif_params,
    )

    if has_transport_object:
        mock_transport_object.send.assert_called_with(
            "dummy",
            {
                "register": "fib.make_milling_gif",
                "session_id": session_id,
                "gif_params": gif_params_dict,
            },
        )
    else:
        mock_logger.error.assert_called_with("No TransportManager object was set up")
