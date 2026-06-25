from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.session_shared import find_upstream_visits, gather_upstream_files
from murfey.util.config import MachineConfig
from tests.conftest import ExampleVisit


@pytest.mark.parametrize("recurse", (True, False))
def test_find_upstream_visits(
    mocker: MockerFixture,
    tmp_path: Path,
    recurse: bool,
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
        # Create path to visit
        upstream_visit = (
            tmp_path / f"{upstream_instrument}/data/2020/{visit_name_root}-{n}"
        )
        # Create some as directories, and some as files
        if n % 2:
            # Only directories should be picked up
            upstream_visit.mkdir(parents=True, exist_ok=True)
            upstream_visits[upstream_instrument] = {upstream_visit.stem: upstream_visit}
            # Check that the function can cope with recursive searching
            upstream_data_dirs[upstream_instrument] = (
                upstream_visit.parent.parent if recurse else upstream_visit.parent
            )
        else:
            upstream_visit.parent.mkdir(parents=True, exist_ok=True)
            upstream_visit.touch(exist_ok=True)

        # Create junk directories with multiple levels to test recursion logic with
        junk_directories = [
            tmp_path / f"{upstream_instrument}/data/junk/directory/number/{n}"
            for n in range(5)
        ]
        for dirpath in junk_directories:
            dirpath.mkdir(parents=True, exist_ok=True)

    # Mock the MachineConfig for this instrument
    mock_machine_config = MagicMock(spec=MachineConfig)
    mock_machine_config.upstream_data_directories = upstream_data_dirs
    mock_get_machine_config = mocker.patch(
        "murfey.server.api.session_shared.get_machine_config",
    )
    mock_get_machine_config.return_value = {instrument_name: mock_machine_config}

    # Run the function
    result = find_upstream_visits(session_id=session_id, db=mock_murfey_db)

    # Check that the expected directories are returned
    assert result == upstream_visits


def test_find_upstream_visits_permission_error(
    mocker: MockerFixture,
    tmp_path: Path,
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
    upstream_visits: dict[str, dict] = {}
    upstream_data_dirs = {}
    for n in range(10):
        upstream_instrument = f"{instrument_name}{str(n).zfill(2)}"
        upstream_visit = (
            tmp_path / f"{upstream_instrument}/data/2020/{visit_name_root}-{n}"
        )
        # Create some as directories, and some as files
        if n % 2:
            upstream_visit.mkdir(parents=True, exist_ok=True)
            upstream_data_dirs[upstream_instrument] = upstream_visit.parent.parent
            # With os.scandir set to raise PermissionError, dictionaries should be empty
            upstream_visits[upstream_instrument] = {}
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

    # Mock the 'os.scandir' function used
    mocker.patch(
        "murfey.server.api.session_shared.os.scandir",
        side_effect=PermissionError(),
    )

    # Run the function:
    result = find_upstream_visits(session_id=session_id, db=mock_murfey_db)

    # With 'os.scandir' mocked to raise PermissionError, no entries should be returned
    assert result == upstream_visits


# File search strings configured, and the files they will be associated with
clem_upstream_file_dict = {
    "processed/**/composite*.tiff": [
        f"processed/grid1/TileScan1/Position_{n}/composite_BF_FL.tiff" for n in range(5)
    ],
    "processed/**/gray.tiff": [
        f"processed/grid1/TileScan1/Position_{n}/gray.tiff" for n in range(5)
    ],
    "processed/**/red.tiff": [
        f"processed/grid1/TileScan1/Position_{n}/red.tiff" for n in range(5)
    ],
    "processed/**/green.tiff": [
        f"processed/grid1/TileScan1/Position_{n}/green.tiff" for n in range(5)
    ],
    "processed/**/blue.tiff": [
        f"processed/grid1/TileScan1/Position_{n}/blue.tiff" for n in range(5)
    ],
    "processed/**/cyan.tiff": [
        f"processed/grid1/TileScan1/Position_{n}/cyan.tiff" for n in range(5)
    ],
    "processed/**/magenta.tiff": [
        f"processed/grid1/TileScan1/Position_{n}/magenta.tiff" for n in range(5)
    ],
    "processed/**/yellow.tiff": [
        f"processed/grid1/TileScan1/Position_{n}/yellow.tiff" for n in range(5)
    ],
    "screenshots/**/*": [
        *[f"screenshots/overview_{n}.png" for n in range(10)],
        *[f"screenshots/annotated_{n}.png" for n in range(10)],
    ],
}
fib_upstream_file_dict = {
    "maps/**/*": [
        *[f"maps/data_{n}.txt" for n in range(5)],
        *[f"maps/map/image_{n}.tiff" for n in range(5)],
    ],
}


@pytest.mark.parametrize(
    "test_params",
    (
        # Workflow to test | Search strings to use
        (clem_upstream_file_dict, ["processed/**/composite*.tiff"]),
        (clem_upstream_file_dict, ["processed/**/gray.tiff"]),
        (clem_upstream_file_dict, ["processed/**/red.tiff"]),
        (clem_upstream_file_dict, ["processed/**/green.tiff"]),
        (clem_upstream_file_dict, ["processed/**/blue.tiff"]),
        (clem_upstream_file_dict, ["processed/**/cyan.tiff"]),
        (clem_upstream_file_dict, ["processed/**/magenta.tiff"]),
        (clem_upstream_file_dict, ["processed/**/yellow.tiff"]),
        (clem_upstream_file_dict, ["screenshots/**/*"]),
        (
            clem_upstream_file_dict,
            [
                "processed/**/composite*.tiff",
                "processed/**/gray.tiff",
                "screenshots/**/*",
            ],
        ),
        (clem_upstream_file_dict, []),
        (clem_upstream_file_dict, None),
        (fib_upstream_file_dict, ["maps/**/*"]),
        (fib_upstream_file_dict, []),
        (fib_upstream_file_dict, None),
    ),
)
def test_gather_upstream_files(
    mocker: MockerFixture,
    tmp_path: Path,
    test_params: tuple[dict[str, list[str]], list[str] | None],
):
    # Get the visit, instrument name, and session ID
    visit_name_root = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}"
    visit_name = f"{visit_name_root}-{ExampleVisit.visit_number}"
    instrument_name = ExampleVisit.instrument_name
    session_id = ExampleVisit.murfey_session_id

    # Set the upstream instrument and upstream visit to access
    upstream_instrument = f"{instrument_name}01"
    upstream_visit = f"{visit_name_root}-5"
    upstream_visit_path = tmp_path / f"{upstream_instrument}/data/2020/{upstream_visit}"

    # Unpack the test params
    upstream_file_dict, search_strings = test_params

    # Sort files into expected ones and skipped ones
    if search_strings is None:
        expected_files = [
            upstream_visit_path / file
            for file_list in upstream_file_dict.values()
            for file in file_list
        ]
        skipped_files = []
    else:
        expected_files = [
            upstream_visit_path / file
            for search_string in search_strings
            for file in upstream_file_dict[search_string]
        ]
        skipped_files = [
            upstream_visit_path / file
            for search_string, file_list in upstream_file_dict.items()
            for file in file_list
            if search_string not in search_strings
        ]

    # Make files
    for file in expected_files:
        if not file.parent.exists():
            file.parent.mkdir(parents=True)
        file.touch(exist_ok=True)
        assert file.is_file()
    for file in skipped_files:
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
        upstream_instrument: list(upstream_file_dict.keys()),
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
            search_strings=search_strings,
            db=mock_murfey_db,
        )
    ) == sorted(expected_files)
