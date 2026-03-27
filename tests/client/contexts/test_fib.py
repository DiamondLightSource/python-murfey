from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.client.contexts.fib import (
    FIBContext,
    _file_transferred_to,
    _get_source,
)
from murfey.util.fib import number_from_name

# -------------------------------------------------------------------------------------
# FIBContext test utilty functions and fixtures
# -------------------------------------------------------------------------------------


@pytest.fixture
def visit_dir(tmp_path: Path):
    return tmp_path / "visit"


@pytest.fixture
def fib_maps_images(visit_dir: Path):
    image_list = []
    for i in range(4):
        name = "Electron Snapshot"
        if i > 0:
            name += f" ({i})"
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
    assert number_from_name(name) == number


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


def test_fib_autotem_context():
    pass


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
    mock_register_fib_atlas = mocker.patch.object(
        FIBContext, "_register_atlas", return_value=True
    )

    # Initialise the FIBContext
    basepath = tmp_path
    context = FIBContext(
        acquisition_software="maps",
        basepath=basepath,
        machine_config={},
        token="",
    )

    # Parse images one-by-one
    for file in fib_maps_images:
        context.post_transfer(file, environment=mock_environment)
    assert mock_get_source.call_count == len(fib_maps_images)
    assert mock_file_transferred_to.call_count == len(fib_maps_images)
    assert mock_register_fib_atlas.call_count == len(fib_maps_images)
    for dst in destination_files:
        mock_register_fib_atlas.assert_any_call(
            dst,
            mock_environment,
        )


def test_fib_meteor_context():
    pass
