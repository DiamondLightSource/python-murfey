from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.file_io_instrument import (
    Dest,
    SuggestedPathParameters,
    make_rsyncer_destination,
    suggest_path,
)
from murfey.util.config import MachineConfig


@pytest.mark.parametrize(
    "test_params",
    (  # Touch | Extra directory | Has raw
        (True, "extra", False),
        (False, "extra", False),
        (True, "", False),
        (False, "", False),
        (True, "extra", True),
        (False, "extra", True),
        (True, "", True),
        (False, "", True),
    ),
)
def test_suggest_path(
    mocker: MockerFixture,
    test_params: tuple[bool, str, bool],
    tmp_path: Path,
):
    # Unpack test params
    touch, extra_dir, has_raw = test_params

    # Set other parameters
    instrument_name = "test"
    year = "2026"
    visit_name = "visit"
    session_id = 1

    rsync_basepath = tmp_path / "data"
    visit_dir = rsync_basepath / year / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    if has_raw:
        (visit_dir / "raw").mkdir(parents=True, exist_ok=True)

    params = SuggestedPathParameters(
        base_path=visit_dir.relative_to(rsync_basepath) / "raw",
        touch=touch,
        extra_directory=extra_dir,
    )

    # Mock the database call
    mock_session = MagicMock()
    mock_session.instrument_name = instrument_name
    mock_db = MagicMock()
    mock_db.exec.return_value.one.return_value = mock_session

    # Mock 'get_machine_config'
    machine_config = MachineConfig(
        **{
            "rsync_basepath": str(rsync_basepath),
            "mkdir_chmod": "0o775",
        }
    )
    mocker.patch(
        "murfey.server.api.file_io_instrument.get_machine_config",
        return_value={
            instrument_name: machine_config,
        },
    )

    # Run the function and check outputs
    result = suggest_path(
        visit_name=visit_name,
        session_id=session_id,
        params=params,
        db=mock_db,
    )

    # Check that the correct suggestion was returned
    dir_name = "raw" if not has_raw else "raw2"
    assert result["suggested_path"] == visit_dir.relative_to(rsync_basepath) / dir_name

    # Check that folders are made only if 'touch' is set
    assert (
        (visit_dir / dir_name).exists()
        if touch
        else not (visit_dir / dir_name).exists()
    )
    if touch and extra_dir:
        assert (visit_dir / dir_name / extra_dir).exists()


@pytest.mark.parametrize(
    "dir_name",
    (
        # General
        "images",
        "screenshots",
        # SPA/Tomo-specific
        "raw",
        "raw2",
        "raw3",
        "atlas",
        # FIB-specific
        "autotem",
        "maps",
        "meteor",
        "extras",
    ),
)
def test_make_rsyncer_destination(
    mocker: MockerFixture,
    dir_name: str,
    tmp_path: Path,
):
    # Set other parameters
    instrument_name = "test"
    year = "2026"
    visit_name = "visit"
    session_id = 1

    rsync_basepath = tmp_path / "data"
    visit_dir = rsync_basepath / year / visit_name
    destination = visit_dir / dir_name

    dest = Dest(destination=destination.relative_to(rsync_basepath))

    # Mock the database call
    mock_session = MagicMock()
    mock_session.instrument_name = instrument_name
    mock_session.visit = visit_name
    mock_db = MagicMock()
    mock_db.exec.return_value.one.return_value = mock_session

    # Mock 'get_machine_config'
    machine_config = MachineConfig(
        **{
            "rsync_basepath": str(rsync_basepath),
            "mkdir_chmod": "0o775",
        }
    )
    mocker.patch(
        "murfey.server.api.file_io_instrument.get_machine_config",
        return_value={
            instrument_name: machine_config,
        },
    )

    # Run the function and check expected outputs
    result = make_rsyncer_destination(
        session_id=session_id,
        destination=dest,
        db=mock_db,
    )
    assert result == dest
    assert destination.exists()
