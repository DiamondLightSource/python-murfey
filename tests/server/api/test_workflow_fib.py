from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.workflow_fib import FIBAtlasInfo, register_fib_atlas


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
