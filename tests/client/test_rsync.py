import queue
import threading
from datetime import datetime
from pathlib import Path
from unittest import mock
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
    assert rsyncer._do_transfer
    assert not rsyncer._remove_files
    assert rsyncer._required_substrings_for_removal == []
    assert rsyncer._substrings_blacklist == substrings_blacklist
    assert rsyncer._notify
    assert rsyncer._end_time == timestamp
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
    assert not rsyncer._finalising
    assert not rsyncer._finalised


@pytest.mark.parametrize(
    "test_params",
    (
        # Is stopping? | Is thread alive? | Expected status
        (False, False, "ready"),
        (False, True, "running"),
        (True, True, "stopping"),
        (True, False, "finished"),
    ),
)
def test_rsyncer_status(
    tmp_path: Path,
    mock_server_url: MagicMock,
    test_params: tuple[bool, bool, str],
):
    # Unpack test params
    is_stopping, is_thread_alive, expected_status = test_params

    # Mock the thread
    mock_thread = MagicMock()
    mock_thread.is_alive.return_value = is_thread_alive

    # Initialise the RSyncer and patch the attributes to be tested
    rsyncer = RSyncer(
        basepath_local=tmp_path / "local",
        basepath_remote=tmp_path / "remote",
        rsync_module=mock.ANY,
        server_url=mock_server_url,
    )
    rsyncer.thread = mock_thread
    rsyncer._stopping = is_stopping

    # Check that its status is correct
    assert rsyncer.status == expected_status

    # Check that its canonical representation is correct
    assert str(rsyncer) == f"<RSyncer ({rsyncer._basepath} → {rsyncer._remote})>"


@pytest.mark.parametrize("notify", (True, False))
def test_rsyncer_notify(
    mocker: MockerFixture,
    tmp_path: Path,
    mock_server_url: MagicMock,
    notify: bool,
):
    # Patch the superclass that RSyncer stems from
    mock_notify = mocker.patch("murfey.client.rsync.Observer.notify")
    mock_notify.return_value = None

    # Initialise the RSyncer
    rsyncer = RSyncer(
        basepath_local=tmp_path / "local",
        basepath_remote=tmp_path / "remote",
        rsync_module=mock.ANY,
        server_url=mock_server_url,
        notify=notify,
    )
    # Check that the 'notify' attribute is set correctly
    assert rsyncer._notify == notify

    # Run 'notify' and check that the expected calls were made
    rsyncer.notify("arg1", "arg2", kwarg1="kwarg1", kwarg2="kwarg2")
    if notify:
        mock_notify.assert_called_once_with(
            "arg1",
            "arg2",
            secondary=False,
            kwarg1="kwarg1",
            kwarg2="kwarg2",
        )
    else:
        mock_notify.assert_not_called()


@pytest.mark.parametrize("rsyncer_status", ("default", "is_alive", "stopping"))
def test_rsyncer_start(
    tmp_path: Path,
    mock_server_url: MagicMock,
    rsyncer_status: str,
):
    # Mock the thread attribute so that it doesn't start an actual Thread
    mock_thread = MagicMock()
    mock_thread.start.return_value = None
    mock_thread.is_alive.return_value = rsyncer_status == "is_alive"

    # Initialise the RSyncer and patch the attributes to be tested
    rsyncer = RSyncer(
        basepath_local=tmp_path / "local",
        basepath_remote=tmp_path / "remote",
        rsync_module=mock.ANY,
        server_url=mock_server_url,
    )
    rsyncer.thread = mock_thread
    rsyncer._stopping = rsyncer_status == "stopping"

    # Start the RSyncer
    if rsyncer_status == "default":
        rsyncer.start()
        mock_thread.start.assert_called_once()
    else:
        with pytest.raises(RuntimeError):
            rsyncer.start()


def test_rsyncer_restart(
    mocker: MockerFixture,
    tmp_path: Path,
    mock_server_url: MagicMock,
):
    # Patch the 'start' class method, which is called by 'restart'
    mock_start = mocker.patch.object(RSyncer, "start")
    mock_start.return_value = None

    # Mock the thread and the attributes used
    mock_thread = MagicMock()
    mock_thread.join.return_value = None

    # Initialise the RSyncer and patch the attributes to be tested
    rsyncer = RSyncer(
        basepath_local=tmp_path / "local",
        basepath_remote=tmp_path / "remote",
        rsync_module=mock.ANY,
        server_url=mock_server_url,
    )
    rsyncer.thread = mock_thread

    # Run 'restart'
    rsyncer.restart()

    # Check that the correct calls and attributes are present
    mock_thread.join.assert_called_once()
    assert not rsyncer._halt_thread
    assert isinstance(rsyncer.thread, threading.Thread)
    mock_start.assert_called_once()


@pytest.mark.parametrize("thread_is_alive", (True, False))
def test_rsyncer_stop(
    tmp_path: Path,
    mock_server_url: MagicMock,
    thread_is_alive: bool,
):
    # Mock the thread
    mock_thread = MagicMock()
    mock_thread.is_alive.return_value = thread_is_alive
    mock_thread.join.return_value = None

    # Mock the queue
    mock_queue = MagicMock()
    mock_queue.join.return_value = None
    mock_queue.put.return_value = None

    # Initialise the RSyncer and patch the attributes to be tested
    rsyncer = RSyncer(
        basepath_local=tmp_path / "local",
        basepath_remote=tmp_path / "remote",
        rsync_module=mock.ANY,
        server_url=mock_server_url,
    )
    rsyncer.thread = mock_thread
    rsyncer.queue = mock_queue

    # Check that initial attributes are as expected
    assert not rsyncer._stopping
    assert not rsyncer._halt_thread

    # Run 'stop' and check that the calls are as expected
    rsyncer.stop()

    assert rsyncer._stopping
    assert rsyncer._halt_thread
    if thread_is_alive:
        mock_queue.join.assert_called_once()
        mock_queue.put.assert_called_with(None)
        mock_thread.join.assert_called_once()
    else:
        mock_queue.join.assert_not_called()
        mock_queue.put.assert_not_called()
        mock_thread.join.assert_not_called()


def test_rsyncer_request_stop(
    tmp_path: Path,
    mock_server_url: MagicMock,
):
    # Initialise the RSyncer
    rsyncer = RSyncer(
        basepath_local=tmp_path / "local",
        basepath_remote=tmp_path / "remote",
        rsync_module=mock.ANY,
        server_url=mock_server_url,
    )

    # Check that initial attributes are as expected
    assert not rsyncer._stopping
    assert not rsyncer._halt_thread

    # Run 'request_stop' and check that attributes have changed
    rsyncer.request_stop()
    assert rsyncer._stopping
    assert rsyncer._halt_thread


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


@pytest.mark.parametrize(
    "test_params",
    (
        # Workflow type | Use thread? | Use callback function? | Use blacklist?
        ("clem", False, False, False),
        ("clem", False, False, True),
        ("clem", False, True, False),
        ("clem", False, True, True),
        ("clem", True, False, False),
        ("clem", True, False, True),
        ("clem", True, True, False),
        ("clem", True, True, True),
    ),
)
def test_rsyncer_finalise(
    mocker: MockerFixture,
    rsync_module: str,
    mock_server_url: MagicMock,
    clem_visit_dir: Path,
    clem_test_files: list[Path],
    clem_junk_files: list[Path],
    test_params: tuple[str, bool, bool, bool],
):
    # Unpack test params
    workflow_type, use_thread, use_callback, use_blacklist = test_params

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
            substrings_blacklist=clem_substrings_blacklist if use_blacklist else {},
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

        # Check that list of files with and without using a blacklist are correct
        if use_thread:
            for file in clem_test_files:
                mock_queue.put.assert_any_call(file)
            if not use_blacklist:
                for file in clem_junk_files:
                    mock_queue.put.assert_any_call(file)
        else:
            transfer_args = mock_transfer.call_args.args
            assert sorted(transfer_args[0]) == (
                sorted(clem_test_files)
                if use_blacklist
                else sorted([*clem_test_files, *clem_junk_files])
            )

        # Transfer is being mocked, so check that files to transfer are all present
        for file in clem_test_files:
            assert file.exists()
        for file in clem_junk_files:
            assert not file.exists() if use_blacklist else file.exists()

        # Check that stop was called the correct number of times depending on the setup
        assert mock_stop.call_count == 2 if use_thread else 1

        # Check that the RSyncer is marked as finalised at the end
        assert rsyncer._finalised

        # Check that the callback was set at the end
        if use_callback:
            mock_callback.assert_called_once()


@pytest.mark.parametrize("is_stopping", (True, False))
def test_rsyncer_enqueue(
    tmp_path: Path,
    mock_server_url: MagicMock,
    is_stopping: bool,
):
    # Mock the queue
    mock_queue = MagicMock()
    mock_queue.put.return_value = None

    # Initialise the RSyncer and patch the attributes used by the test
    rsyncer = RSyncer(
        basepath_local=tmp_path / "local",
        basepath_remote=tmp_path / "remote",
        rsync_module=mock.ANY,
        server_url=mock_server_url,
    )
    rsyncer._stopping = is_stopping
    rsyncer.queue = mock_queue

    # Run enqueue with a test file and check that the expected calls were made
    rsyncer.enqueue(Path("test_file"))
    if is_stopping:
        mock_queue.put.assert_not_called()
    else:
        mock_queue.put.assert_called_once_with(
            (tmp_path / "local" / "test_file").absolute()
        )


def test_rsyncer_flush_skipped(
    tmp_path: Path,
    mock_server_url: MagicMock,
):
    # Mock the queue
    mock_queue = MagicMock()
    mock_queue.put.return_value = None

    # Create a list of test files
    skipped_files = [
        tmp_path / "local" / f"file_{str(n).zfill(2)}.txt" for n in range(20)
    ]

    # Initialise the RSyncer and patch the attributes used by the test
    rsyncer = RSyncer(
        basepath_local=tmp_path / "local",
        basepath_remote=tmp_path / "remote",
        rsync_module=mock.ANY,
        server_url=mock_server_url,
    )
    rsyncer.queue = mock_queue
    rsyncer._skipped_files = skipped_files

    # Run 'flush_skipped' and check that it works as intended
    rsyncer.flush_skipped()
    for f in skipped_files:
        mock_queue.put.assert_any_call(f)
    assert rsyncer._skipped_files == []
