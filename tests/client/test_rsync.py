import queue
import threading
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.client.rsync import RSyncer
from tests.conftest import ExampleVisit


@pytest.fixture
def rsync_module():
    return "data"


@pytest.fixture
def mock_server_url():
    mock_url = MagicMock()
    mock_url.hostname = "10.0.0.1"
    return mock_url


# Create a dummy callback function
def dummy_callback():
    return None


@pytest.mark.parametrize("is_local", (True, False))
def test_rsyncer_initialises(
    tmp_path: Path,
    rsync_module: str,
    mock_server_url: MagicMock,
    is_local: bool,
):
    # Assign values to parameters
    basepath_local = tmp_path / "local"
    basepath_remote = tmp_path / "remote"
    do_transfer = True
    remove_files = True

    # Create a test substrings blacklist dict
    substrings_blacklist = {
        "directories": ["1", "2", "3"],
        "files": ["a", "b", "c"],
    }

    # Create a timestamp
    timestamp = datetime.now()

    # Initialise the RSyncer
    rsyncer = RSyncer(
        basepath_local=basepath_local,
        basepath_remote=basepath_remote,
        rsync_module=rsync_module,
        server_url=mock_server_url,
        stop_callback=dummy_callback,
        local=is_local,
        do_transfer=do_transfer,
        remove_files=remove_files,
        substrings_blacklist=substrings_blacklist,
        end_time=timestamp,
    )

    # Check that the attributes are as expected
    assert rsyncer._basepath == basepath_local.absolute()
    assert rsyncer._basepath_remote == basepath_remote
    assert rsyncer._rsync_module == rsync_module
    assert rsyncer._server_url == mock_server_url
    assert rsyncer._stop_callback == dummy_callback
    assert rsyncer._local == is_local
    assert rsyncer._do_transfer == do_transfer
    assert rsyncer._remove_files == remove_files
    assert rsyncer._required_substrings_for_removal == []
    assert rsyncer._substrings_blacklist == substrings_blacklist
    assert rsyncer._notify
    assert rsyncer._end_time == timestamp
    assert not rsyncer._finalising
    assert not rsyncer._finalised
    assert rsyncer._skipped_files == []
    assert (
        rsyncer._remote == str(basepath_remote)
        if is_local
        else f"{mock_server_url.hostname}::{rsync_module}/{basepath_remote}"
    )
    assert rsyncer._files_transferred == 0
    assert rsyncer._bytes_transferred == 0
    assert isinstance(rsyncer.queue, queue.Queue)
    assert isinstance(rsyncer.thread, threading.Thread)
    assert not rsyncer._stopping
    assert not rsyncer._halt_thread


@pytest.fixture
def clem_visit_dir(tmp_path: Path):
    visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
    visit_dir = tmp_path / "local" / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    return visit_dir


@pytest.fixture
def clem_test_files(clem_visit_dir: Path):
    # Create test files for the DirWatcher to scan
    file_list: list[Path] = []
    project_dir = clem_visit_dir / "images" / "test_grid"

    # Example atlas collection
    for s in range(20):
        file_list.append(
            project_dir
            / "Overview 1"
            / "Image 1"
            / f"Image 1--Stage{str(s).zfill(2)}.tif"
        )
    file_list.append(
        project_dir / "Overview 1" / "Image 1" / "Metadata" / "Image 1.xlif"
    )

    # Example image stack collection
    for c in range(3):
        for z in range(10):
            file_list.append(
                project_dir
                / "TileScan 1"
                / "Position 1"
                / f"Position 1--C{str(c).zfill(2)}--Z{str(z).zfill(2)}.tif"
            )
    file_list.append(
        project_dir / "TileScan 1" / "Position 1" / "Metadata" / "Position 1.xlif"
    )

    # Create all files and directories specified
    for file in file_list:
        if not file.parent.exists():
            file.parent.mkdir(parents=True)
        if not file.exists():
            file.touch()
    return sorted(file_list)


@pytest.fixture
def clem_junk_files(clem_visit_dir: Path):
    # Create junk files that are to be blacklisted from the CLEM workflow
    file_list: list[Path] = []
    project_dir = clem_visit_dir / "images" / "test_grid"

    # Create junk atlas data
    for n in range(5):
        for s in range(20):
            file_list.append(
                project_dir
                / "Image 1"
                / f"Image 1_pmd_{n}"
                / f"Image 1_pmd_{n}--Stage{str(s).zfill(2)}.tif"
            )
        file_list.append(
            project_dir / "Image 1" / f"Image 1_pmd_{n}" / "Metadata" / "Image 1.xlif"
        )

    # Creat junk image data
    for n in range(5):
        for c in range(3):
            for z in range(10):
                file_list.append(
                    project_dir
                    / "Position 1"
                    / f"Position 1_pmd_{n}"
                    / f"Position 1_pmd_{n}--C{str(c).zfill(2)}--Z{str(z).zfill(2)}.tif"
                )
        file_list.append(
            project_dir
            / "Position 1"
            / f"Position 1_pmd_{n}"
            / "Metadata"
            / "Position 1.xlif"
        )

    # Create remaining junk files
    for file_path in (
        "1.xlef",
        "Metadata/IOManagerConfiguation.xlif",
        "Metadata/Overview 1.xlcf",
        "Metadata/TileScan 1.xlcf",
        "Overview 1/Image 1/Image 1_histo.lof",
        "TileScan 1/Position 1/Position 1_histo.lof",
        "Overview 1/Image 1/Metadata/Image 1_histo.xlif",
        "TileScan 1/Position 1/Metadata/Position 1_histo.xlif",
    ):
        file_list.append(project_dir / file_path)

    # Create files and directoriees
    for file in file_list:
        if not file.parent.exists():
            file.parent.mkdir(parents=True)
        if not file.exists():
            file.touch()
    return sorted(file_list)


clem_substrings_blacklist = {
    "directories": [
        "_pmd_",
    ],
    "files": [
        ".xlef",
        ".xlcf",
        "_histo.lof",
        "_histo.xlif",
        "IOManagerConfiguation.xlif",
    ],
}

rsyncer_finalise_params_matrix: tuple[tuple[str, bool, bool], ...] = (
    # Workflow type | Use thread? | Use callback function?
    ("clem", False, False),
    ("clem", False, True),
    ("clem", True, False),
    ("clem", True, True),
)


@pytest.mark.parametrize("test_params", rsyncer_finalise_params_matrix)
def test_rsyncer_finalise(
    mocker: MockerFixture,
    rsync_module: str,
    mock_server_url: MagicMock,
    clem_visit_dir: Path,
    clem_test_files: list[Path],
    clem_junk_files: list[Path],
    test_params: tuple[str, bool, bool],
):
    # Unpack test params
    workflow_type, use_thread, use_callback = test_params

    # Create a test end time
    timestamp = datetime.now()

    # Mock the class functions/attributes called by the 'finalise' class function
    mock_queue = MagicMock()
    mock_queue.put.return_value = None

    mock_transfer = mocker.patch.object(RSyncer, "_transfer")
    mock_transfer.return_value = True

    mock_stop = mocker.patch.object(RSyncer, "stop")
    mock_stop.return_value = None

    mock_process = mocker.patch.object(RSyncer, "_process")
    mock_process.return_value = None

    mock_callback = MagicMock(return_value=None)

    # Initialise the RSyncer class based on the workflow type being tested
    if workflow_type == "clem":
        rsyncer = RSyncer(
            basepath_local=clem_visit_dir / "images",
            basepath_remote=Path(clem_visit_dir.name) / "images",
            rsync_module=rsync_module,
            server_url=mock_server_url,
            stop_callback=dummy_callback,
            substrings_blacklist=clem_substrings_blacklist,
            end_time=timestamp,
        )
        # Patch the 'queue' attribute with the mocked one
        rsyncer.queue = mock_queue

        # Check the initial state of attributes that will be changed by 'finalise'
        assert not rsyncer._remove_files
        assert rsyncer._notify
        assert rsyncer._end_time == timestamp
        assert not rsyncer._finalising
        assert not rsyncer._finalised

        # Run the 'finalise' class function with the workflow-specific paths
        rsyncer.finalise(
            thread=use_thread,
            callback=mock_callback if use_callback else None,
        )

        # Check that attributes are set correctly at the start of the function
        assert rsyncer._remove_files
        assert not rsyncer._notify
        assert rsyncer._end_time is None
        assert rsyncer._finalising

        # Check that list of files to transfer doesn't include blacklisted files
        if use_thread:
            for file in clem_test_files:
                mock_queue.put.assert_any_call(file)
        else:
            transfer_args = mock_transfer.call_args.args
            assert sorted(transfer_args[0]) == sorted(clem_test_files)

        # Check that the blacklisted files no longer exist
        for file in clem_junk_files:
            assert not file.exists()
        # Transfer is being mocked, so check that files to transfer are all present
        for file in clem_test_files:
            assert file.exists()

        # Check that stop was called the correct number of times depending on the setup
        assert mock_stop.call_count == 2 if use_thread else 1

        # Check that the RSyncer is marked as finalised at the end
        assert rsyncer._finalised

        # Check that the callback was set at the end
        if use_callback:
            mock_callback.assert_called_once()
