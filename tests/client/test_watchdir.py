import os
import queue
import threading
from pathlib import Path

import pytest

from murfey.client.watchdir import DirWatcher
from tests.conftest import ExampleVisit


def test_dirwatcher_initialises(tmp_path: Path):
    # Check that the DirWatcher initialises with the default attributes
    watcher = DirWatcher(path=str(tmp_path))
    assert watcher._basepath == os.fspath(str(tmp_path))
    assert watcher._lastscan == {}
    assert watcher._file_candidates == {}
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
    assert str(watcher) == f"<DirWatcher ({os.fspath(str(tmp_path))})>"


@pytest.fixture
def clem_visit_dir(tmp_path: Path):
    visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
    visit_dir = tmp_path / "clem" / "data" / "2025" / visit_name
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


scan_directory_params_matrix: tuple[tuple[str, dict[str, list[str]]], ...] = (
    # Workflow type | Substrings blacklist
    (
        "clem",
        {
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
        },
    ),
)


@pytest.mark.parametrize("test_params", scan_directory_params_matrix)
def test_scan_directory(
    clem_visit_dir: Path,
    clem_test_files: list[Path],
    clem_junk_files: list[Path],
    test_params: tuple[str, dict[str, list[str]]],
):
    # Unpack test params
    workflow_type, substrings_blacklist = test_params

    # Initialise different watchers based on the workflow to test and run the scan
    if workflow_type == "clem":
        watcher = DirWatcher(
            path=str(clem_visit_dir),
            substrings_blacklist=substrings_blacklist,
        )
        result = watcher._scan_directory()

        # Check that the result does not contain the junk files
        assert [str(file) for file in clem_test_files] == sorted(result.keys())
