from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.session_shared import find_upstream_visits, gather_upstream_files
from murfey.util.config import MachineConfig
from tests.conftest import ExampleVisit


def test_find_upstream_visits(
    mocker: MockerFixture,
    tmp_path: Path,
    # murfey_db_session,
):
    # Get the visit, instrument name, and session ID
    visit_name_root = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}"
    visit_name = f"{visit_name_root}-{ExampleVisit.visit_number}"
    instrument_name = ExampleVisit.instrument_name
    session_id = ExampleVisit.murfey_session_id

    # Mock the database call
    mock_murfey_session_row = MagicMock()
    mock_murfey_session_row.visit = visit_name
    mock_murfey_session_row.instrument_name = instrument_name
    mock_murfey_db = MagicMock()
    mock_murfey_db.exec.return_value.one.return_value = mock_murfey_session_row

    # Create mock upstream visit directories and necessary data structures
    upstream_visits = {}
    upstream_data_dirs = {}
    for n in range(10):
        upstream_instrument = f"{instrument_name}{str(n).zfill(2)}"
        upstream_visit = (
            tmp_path / f"{upstream_instrument}/data/2020/{visit_name_root}-{n}"
        )
        # Create some as directories, and some as files
        if n % 2:
            # Only directories should be picked up
            upstream_visit.mkdir(parents=True, exist_ok=True)
            upstream_visits[upstream_instrument] = {upstream_visit.stem: upstream_visit}
            upstream_data_dirs[upstream_instrument] = upstream_visit.parent
        else:
            upstream_visit.parent.mkdir(parents=True, exist_ok=True)
            upstream_visit.touch(exist_ok=True)

    # Mock the MachineConfig for this instrument
    mock_machine_config = MagicMock(spec=MachineConfig)
    mock_machine_config.upstream_data_directories = upstream_data_dirs
    mock_get_machine_config = mocker.patch(
        "murfey.server.api.session_shared.get_machine_config",
    )
    mock_get_machine_config.return_value = {instrument_name: mock_machine_config}

    # Run the function:
    result = find_upstream_visits(session_id=session_id, db=mock_murfey_db)

    # Mock the database call
    assert result == upstream_visits


gather_upstream_files_test_matrix: tuple[
    tuple[tuple[str, list[str], list[str]], ...], ...
] = (
    # CLEM
    (
        # Search strings, files to match, and files to avoid
        (
            "processed/**/composite*.tiff",
            [
                file
                for sublist in [
                    [
                        f"processed/grid1/TileScan1/Position_{n}/composite_BF_FL.tiff"
                        for n in range(5)
                    ],
                ]
                for file in sublist
            ],
            [
                file
                for sublist in [
                    [
                        f"processed/grid1/TileScan1/Position_{n}/{color}.tiff"
                        for n in range(5)
                        for color in ("gray", "green", "red")
                    ],
                ]
                for file in sublist
            ],
        ),
        (
            "screenshots/**/*",
            [
                file
                for sublist in [
                    [f"screenshots/overview_{n}.png" for n in range(10)],
                    [f"screenshots/annotated_{n}.png" for n in range(10)],
                ]
                for file in sublist
            ],
            [],
        ),
    ),
    # FIB
    (
        # Search strings, files to match, and files to avoid
        (
            "maps/**/*",
            [
                file
                for sublist in [
                    [f"maps/data_{n}.txt" for n in range(5)],
                    [f"maps/map/image_{n}.tiff" for n in range(5)],
                ]
                for file in sublist
            ],
            [],
        ),
    ),
)


@pytest.mark.parametrize("test_params", gather_upstream_files_test_matrix)
def test_gather_upstream_files(
    mocker: MockerFixture,
    tmp_path: Path,
    test_params: tuple[tuple[str, list[str], list[str]], ...],
):
    # Get the visit, instrument name, and session ID
    visit_name_root = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}"
    visit_name = f"{visit_name_root}-{ExampleVisit.visit_number}"
    instrument_name = ExampleVisit.instrument_name
    session_id = ExampleVisit.murfey_session_id

    # Unpack the test params
    search_strings = [item[0] for item in test_params]
    upstream_relative_paths = [file for item in test_params for file in item[1]]
    other_relative_paths = [file for item in test_params for file in item[2]]

    # Set the upstream instrument and upstream visit to access
    upstream_instrument = f"{instrument_name}01"
    upstream_visit = f"{visit_name_root}-5"
    upstream_visit_path = tmp_path / f"{upstream_instrument}/data/2020/{upstream_visit}"

    # Construct the files and directories
    upstream_files = [
        upstream_visit_path / relative_path for relative_path in upstream_relative_paths
    ]
    other_files = [
        upstream_visit_path / relative_path for relative_path in other_relative_paths
    ]

    for file in upstream_files:
        if not file.parent.exists():
            file.parent.mkdir(parents=True)
        file.touch(exist_ok=True)
        assert file.is_file()
    for file in other_files:
        if not file.parent.exists():
            file.parent.mkdir(parents=True)
        file.touch(exist_ok=True)
        assert file.is_file()

    # Mock the database call
    mock_murfey_session_row = MagicMock()
    mock_murfey_session_row.visit = visit_name
    mock_murfey_session_row.instrument_name = instrument_name
    mock_murfey_db = MagicMock()
    mock_murfey_db.exec.return_value.one.return_value = mock_murfey_session_row

    # Mock the MachineConfig for this instrument
    mock_machine_config = MagicMock(spec=MachineConfig)
    mock_machine_config.upstream_data_search_strings = {
        upstream_instrument: search_strings
    }
    mock_get_machine_config = mocker.patch(
        "murfey.server.api.session_shared.get_machine_config",
    )
    mock_get_machine_config.return_value = {instrument_name: mock_machine_config}

    assert sorted(
        gather_upstream_files(
            session_id=session_id,
            upstream_instrument=upstream_instrument,
            upstream_visit_path=upstream_visit_path,
            db=mock_murfey_db,
        )
    ) == sorted(upstream_files)
