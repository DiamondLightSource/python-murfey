import os
import queue
import threading
from pathlib import Path

import pytest

from murfey.client.watchdir import DirWatcher
from tests.conftest import ExampleVisit


@pytest.fixture
def visit_dir(tmp_path: Path):
    visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
    visit_dir = tmp_path / "data" / "2025" / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    return visit_dir


def test_dirwatcher_initialises(visit_dir: Path):
    # Check that the DirWatcher initialises with the default attributes
    watcher = DirWatcher(path=str(visit_dir))
    assert watcher._basepath == os.fspath(visit_dir)
    assert watcher._lastscan == {}
    assert watcher._file_candidates == {}
    assert watcher._statusbar is None
    assert watcher.settling_time == 60
    assert watcher._appearance_time is None
    assert watcher._substrings_blacklist == {}
    assert watcher._transfer_all is True
    assert watcher._modification_overwrite is None
    assert isinstance(watcher._init_time, float)
    assert isinstance(watcher.queue, queue.Queue)
    assert isinstance(watcher.thread, threading.Thread)
    assert watcher._stopping is False
    assert watcher._halt_thread is False

    # Check that the string representation is as expected
    assert str(watcher) == f"<DirWatcher ({os.fspath(str(visit_dir))})>"


@pytest.mark.skip
def test_scan_directory():
    pass
