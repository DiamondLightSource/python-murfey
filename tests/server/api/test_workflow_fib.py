from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import PIL.Image
import pytest
from pytest_mock import MockerFixture

from murfey.server.api.workflow_fib import (
    FIBAtlasInfo,
    FIBGIFParameters,
    make_gif,
    register_fib_atlas,
)


def test_register_fib_atlas(
    mocker: MockerFixture,
    tmp_path: Path,
):
    # Mock the databse instance
    mock_db = MagicMock()

    # Patch out the entry point being called
    mock_register_fib_atlas = mocker.patch("murfey.workflows.fib.register_atlas.run")

    session_id = 1
    fib_atlas_info = FIBAtlasInfo(**{"file": str(tmp_path / "dummy")})

    # Run the function and check that the expected calls were made
    register_fib_atlas(
        session_id=session_id,
        fib_atlas_info=fib_atlas_info,
        db=mock_db,
    )
    mock_register_fib_atlas.assert_called_once_with(
        session_id=session_id,
        file=fib_atlas_info.file,
        murfey_db=mock_db,
    )


def test_register_fib_atlas_no_entry_point(
    mocker: MockerFixture,
    tmp_path: Path,
):
    # Mock out entry_points to return an empty list
    mocker.patch("murfey.server.api.workflow_fib.entry_points", return_value=[])

    # Mock the databse instance
    mock_db = MagicMock()

    fib_atlas_info = FIBAtlasInfo(**{"file": str(tmp_path / "dummy")})

    # Patch out the entry point being called
    with pytest.raises(RuntimeError):
        register_fib_atlas(
            session_id=1,
            fib_atlas_info=fib_atlas_info,
            db=mock_db,
        )


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
    mock_machine_config.mkdir_chmod = 0o775
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
