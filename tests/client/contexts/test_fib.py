from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.client.contexts.fib import (
    FIBContext,
    _file_transferred_to,
    _get_source,
    _number_from_name,
)

# -------------------------------------------------------------------------------------
# FIBContext test utilty functions and fixtures
# -------------------------------------------------------------------------------------
num_lamellae = 5


@pytest.fixture
def visit_dir(tmp_path: Path):
    return tmp_path / "visit"


@pytest.fixture
def fib_autotem_dc_images(visit_dir: Path):
    stages = ("Rough-Milling", "Polishing-1", "Polishing-2")
    images_per_stage = 2

    # Create images as Murfey would expect to find them in the DCImages folder
    image_list = []
    for l in range(num_lamellae):
        for s, stage in enumerate(stages):
            for i in range(images_per_stage):
                lamella_folder = "Lamella"
                if l > 0:
                    lamella_folder += f" ({l + 1})"
                # Continuously increment seconds count between files
                timestamp = (
                    f"2025-05-10-12-34-{str(0 + i + (s * images_per_stage)).zfill(2)}"
                )
                file = (
                    visit_dir
                    / f"autotem/visit/Sites/{lamella_folder}"
                    / "DCImages/DCM_2025-05-10-12-34-00.125"
                    / f"{timestamp}-{stage}-dc_rescan-image-.png"
                )
                if not file.exists():
                    file.parent.mkdir(parents=True, exist_ok=True)
                    file.touch()
                image_list.append(file)
    return image_list


@pytest.fixture
def fib_maps_images(visit_dir: Path):
    image_list = []
    for i in range(4):
        name = "Electron Snapshot"
        if i > 0:
            name += f" ({i + 1})"
        file = visit_dir / "maps/visit/LayersData/Layer" / f"{name}.tiff"
        if not file.exists():
            file.parent.mkdir(parents=True, exist_ok=True)
            file.touch()
        image_list.append(file)
    return image_list


# -------------------------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "test_params",
    (  # File name | Expected number
        # AutoTEM examples
        ("Lamella", 1),
        ("Lamella (2)", 2),
        ("Lamella (12)", 12),
        # Maps examples
        ("Electron Snapshot", 1),
        ("Electron Snapshot (3)", 3),
        ("Electron Snapshot (21)", 21),
    ),
)
def test_number_from_name(test_params: tuple[str, int]):
    name, number = test_params
    assert _number_from_name(name) == number


def test_get_source(
    tmp_path: Path,
    visit_dir: Path,
    fib_maps_images: list[Path],
):
    # Mock the MurfeyInstanceEnvironment
    mock_environment = MagicMock()
    mock_environment.sources = [
        visit_dir,
        tmp_path / "another_dir",
    ]
    # Check that the correct source directory is found
    for file in fib_maps_images:
        assert _get_source(file, mock_environment) == visit_dir


def test_file_transferred_to(
    tmp_path: Path,
    visit_dir: Path,
    fib_maps_images: list[Path],
):
    # Mock the environment
    mock_environment = MagicMock()
    mock_environment.default_destinations = {visit_dir: "current_year"}
    mock_environment.visit = "visit"

    # Iterate across the FIB files to compare against
    destination_dir = tmp_path / "fib" / "data" / "current_year" / "visit"
    for file in fib_maps_images:
        # Work out what the expected destination will be
        assert _file_transferred_to(
            environment=mock_environment,
            source=visit_dir,
            file_path=file,
            rsync_basepath=tmp_path / "fib" / "data",
        ) == destination_dir / file.relative_to(visit_dir)


@pytest.mark.parametrize(
    "test_params",
    (  # File type to test | Use environment? | Find source? | Find destination?
        ("drift_correction", True, True, True),
        ("drift_correction", False, True, True),
        ("drift_correction", True, False, True),
        ("drift_correction", True, True, False),
    ),
)
def test_fib_autotem_context(
    mocker: MockerFixture,
    test_params: tuple[str, bool, bool, bool],
    tmp_path: Path,
    visit_dir: Path,
    fib_autotem_dc_images: list[Path],
):
    # Unpack test params
    file_type, use_env, find_source, find_dst = test_params

    # Mock the environment
    mock_environment = None
    if use_env:
        mock_environment = MagicMock()

    # Mock the logger to check if specific logs are triggered
    mock_logger = mocker.patch("murfey.client.contexts.fib.logger")

    # Create a list of destinations
    destination_dir = tmp_path / "fib" / "data" / "current_year" / "visit"
    destination_files = [
        destination_dir / file.relative_to(visit_dir) for file in fib_autotem_dc_images
    ]

    # Mock the functions used in 'post_transfer'
    mock_get_source = mocker.patch("murfey.client.contexts.fib._get_source")
    mock_file_transferred_to = mocker.patch(
        "murfey.client.contexts.fib._file_transferred_to"
    )
    mock_capture_post = mocker.patch("murfey.client.contexts.fib.capture_post")

    # Initialise the FIBContext
    basepath = tmp_path
    context = FIBContext(
        acquisition_software="autotem",
        basepath=basepath,
        machine_config={},
        token="",
    )

    match file_type:
        case "drift_correction":
            # Add case-specific return values and side-effects to the mocks
            mock_get_source.return_value = tmp_path if find_source else None
            if find_dst:
                mock_file_transferred_to.side_effect = destination_files
            else:
                mock_file_transferred_to.return_value = None

            # Parse images one-by-one and check that expected calls were made
            for file in fib_autotem_dc_images:
                context.post_transfer(file, environment=mock_environment)
            if not use_env:
                mock_logger.warning.assert_called_with("No environment passed in")
            elif not find_source:
                mock_logger.warning.assert_called_with(
                    f"No source found for file {file}"
                )
            elif not find_dst:
                mock_logger.warning.assert_called_with(
                    f"File {file.name!r} not found on storage system"
                )
            else:
                mock_get_source.assert_called_with(file, mock_environment)
                mock_file_transferred_to.assert_called_with(
                    environment=mock_environment,
                    source=basepath,
                    file_path=file,
                    rsync_basepath=Path(""),
                )
                assert mock_capture_post.call_count == len(fib_autotem_dc_images)
                assert len(context._drift_correction_images) == num_lamellae


def test_fib_maps_context(
    mocker: MockerFixture,
    tmp_path: Path,
    visit_dir: Path,
    fib_maps_images: list[Path],
):
    # Mock the environment
    mock_environment = MagicMock()

    # Create a list of destinations
    destination_dir = tmp_path / "fib" / "data" / "current_year" / "visit"
    destination_files = [
        destination_dir / file.relative_to(visit_dir) for file in fib_maps_images
    ]

    # Mock the functions used in 'post_transfer'
    mock_get_source = mocker.patch(
        "murfey.client.contexts.fib._get_source", return_value=tmp_path
    )
    mock_file_transferred_to = mocker.patch(
        "murfey.client.contexts.fib._file_transferred_to", side_effect=destination_files
    )
    mock_capture_post = mocker.patch("murfey.client.contexts.fib.capture_post")

    # Initialise the FIBContext
    basepath = tmp_path
    context = FIBContext(
        acquisition_software="maps",
        basepath=basepath,
        machine_config={},
        token="",
    )

    # Parse images one-by-one
    for f, file in enumerate(fib_maps_images):
        context.post_transfer(file, environment=mock_environment)
        mock_get_source.assert_called_with(file, mock_environment)
        mock_file_transferred_to.assert_called_with(
            environment=mock_environment,
            source=basepath,
            file_path=file,
            rsync_basepath=Path(""),
        )
        mock_capture_post.assert_called_with(
            base_url=mock.ANY,
            router_name="workflow_fib.router",
            function_name="register_fib_atlas",
            token="",
            instrument_name=mock.ANY,
            data={"file": str(destination_files[f])},
            session_id=mock.ANY,
        )


def test_fib_meteor_context():
    pass
