from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import PIL.Image
import pytest
from pytest_mock import MockerFixture

from murfey.server.api.workflow_fib import (
    FIBAtlasFile,
    FIBGIFParameters,
    make_gif,
    register_fib_atlas,
)


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

    # Mock the tranposrt object
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


@pytest.mark.asyncio
async def test_make_gif(
    mocker: MockerFixture,
    tmp_path: Path,
):
    # Set up test variables
    session_id = 10
    instrument_name = "test_instrument"
    rsync_basepath = tmp_path / "data"
    visit_name = "cm12345-6"
    year = 2020
    visit_dir = rsync_basepath / str(year) / visit_name
    lamella_num = 12
    lamella_folder = "Lamella"
    if lamella_num > 1:
        lamella_folder += f" ({lamella_num})"
    output_file = (
        visit_dir
        / "processed"
        / "project_name"
        / "grid_1"
        / "drift_correction"
        / f"lamella_{lamella_num}.gif"
    )

    # Create a list of test image file paths
    raw_images = [
        visit_dir
        / "autotem"
        / visit_name
        / "Sites"
        / lamella_folder
        / "DCImages/DCM_asdfjkl/asdfjkl-Polishing-dc_rescan-image-.png"
    ] * 5
    # Mock the output of PIL.Image.open to always return a NumPY array
    mocker.patch(
        "murfey.server.api.workflow_fib.PIL.Image.open",
        return_value=PIL.Image.fromarray(np.ones((512, 512), dtype=np.uint16)),
    )

    # Create the Pydantic model
    params = FIBGIFParameters(
        lamella_number=lamella_num,
        images=[str(f) for f in raw_images],
        output_file=output_file,
    )

    # Mock the database query
    mock_db = MagicMock()
    mock_db.exec.return_value.one.return_value.instrument_name = instrument_name
    mock_db.exec.return_value.one.return_value.visit = visit_name

    # Mock the machine config and 'get_machine_config'
    mock_machine_config = MagicMock()
    mock_machine_config.mkdir_chmod = 0o2775
    mock_machine_config.rsync_basepath = rsync_basepath
    mocker.patch(
        "murfey.server.api.workflow_fib.get_machine_config",
        return_value={
            instrument_name: mock_machine_config,
        },
    )

    # Run the function and check that the expected outputs are there
    result = await make_gif(
        session_id=session_id,
        gif_params=params,
        db=mock_db,
    )
    assert output_file.exists()
    assert result.get("output_gif") == str(output_file)
