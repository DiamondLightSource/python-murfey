from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.client.context import _file_transferred_to, _get_source
from murfey.client.contexts.clem import CLEMContext

instrument_name = "clem"
session_id = 1
visit_name = "cm12345-6"
project_name = "2025_06_30_10_00_00--grid_1001"
example_file_paths = [
    f"{project_name}{path}"
    for path in [
        # TIFF files
        "/TileScan 1/Position 1--Z00.tif",
        "/TileScan 1/Position 1--C00.tif",
        "/TileScan 1/Position 1--Stage00.tif",
        "/TileScan 1/Position 1--Z00--C00.tif",
        "/TileScan 1/Position 1--Stage00--C00.tif",
        "/TileScan 1/Position 1--Stage00--Z00.tif",
        "/TileScan 1/Position 1--Stage00--Z00--C00.tif",
        "/TileScan 1/Position 1_ICC--Z00.tif",
        "/TileScan 1/Position 1_Lng_LVCC--C00.tif",
        "/TileScan 1/Position 1_Lng_SVCC--Stage00.tif",
        "/TileScan 1/Position 1_ICC--Z00--C00.tif",
        "/TileScan 1/Position 1_Lng_LVCC--Stage00--C00.tif",
        "/TileScan 1/Position 1_Lng_SVCC--Stage00--Z00.tif",
        "/TileScan 1/Metadata/Position 1.xlif",
        "/Series001--Z00--C00.tif",
        "/Series001--Stage00--C00.tif",
        "/Series001--Stage00--Z00.tif",
        "/Series001--Stage00--Z00--C00.tif",
        "/Series001_ICC--Z00.tif",
        "/Series001_Lng_LVCC--C00.tif",
        "/Series001_Lng_SVCC--Stage00.tif",
        "/Metadata/Series001_Lng_LVCC.xlif",
        "/Image 1_ICC--Z00--C00.tif",
        "/Image 1_Lng_LVCC--Stage00--C00.tif",
        "/Image 1_Lng_SVCC--Stage00--Z00.tif",
        "/Image 1--Z00.tif",
        "/Image 1--C00.tif",
        "/Image 1--Stage00.tif",
        "/Image 1--Stage00--Z00--C00.tif",
        "/Metadata/Image 1_ICC.xlif",
        # LIF file
        ".lif",
    ]
]


def create_tiff_dataset(
    visit_dir: Path,
    series_name: str,
    num_tiles: int | None = None,
    num_frames: int | None = None,
    num_channels: int | None = None,
):
    """
    Creates mock files mimicking the folder structure and naming pattern of actual
    CLEM TIFF datasets.
    """

    # Construct the TIFF files
    tiff_files = []
    for c in range(num_channels or 1):
        for t in range(num_tiles or 1):
            for z in range(num_frames or 1):
                # Construct name of file based on the presence of tiles, frames, and channels
                file_name = series_name
                if num_tiles is not None:
                    file_name += f"--Stage{str(t).zfill(2)}"
                if num_frames is not None:
                    file_name += f"--Z{str(z).zfill(2)}"
                if num_channels is not None:
                    file_name += f"--C{str(c).zfill(2)}"
                file_name += ".tif"
                file = visit_dir / "images" / file_name
                file.touch(exist_ok=True)
                tiff_files.append(file)

    # Construct the metadata file
    file_name = f"{series_name}.xlif"
    metadata_file = visit_dir / "images" / file_name
    # Insert "Metadata" before the file name
    metadata_file = metadata_file.parent / "Metadata" / metadata_file.name
    metadata_file.touch(exist_ok=True)

    return tiff_files, metadata_file


@pytest.fixture
def visit_dir(
    tmp_path: Path,
):
    visit_dir = tmp_path / visit_name
    visit_dir.mkdir(parents=True, exist_ok=True)
    return visit_dir


@pytest.mark.parametrize("file_path", example_file_paths)
def test_get_source(
    tmp_path: Path,
    visit_dir: Path,
    file_path: str,
):
    # Mock the MurfeyInstanceEnvironment
    mock_environment = MagicMock()
    mock_environment.sources = [
        visit_dir,
        tmp_path / "another_dir",
    ]
    # Check that the correct source directory is found
    assert _get_source(visit_dir / "images" / file_path, mock_environment) == visit_dir


@pytest.mark.parametrize("file_path", example_file_paths)
def test_file_transferred_to(tmp_path: Path, visit_dir: Path, file_path: str):
    # Create the client-side file
    file = visit_dir / "images" / file_path

    # Mock the environment
    mock_environment = MagicMock()
    mock_environment.default_destinations = {visit_dir: "current_year"}
    mock_environment.visit = visit_name

    # Iterate across the FIB files to compare against
    destination_dir = tmp_path / "clem" / "data" / "current_year" / visit_name
    # Work out what the expected destination will be
    assert _file_transferred_to(
        environment=mock_environment,
        source=visit_dir,
        file_path=file,
        rsync_basepath=tmp_path / "clem" / "data",
    ) == destination_dir / file.relative_to(visit_dir)


@pytest.mark.parametrize(
    "test_params",
    (  # Has environment | Has source
        (True, True),
        (False, True),
        (True, False),
    ),
)
def test_post_transfer_lif_data(
    mocker: MockerFixture,
    tmp_path: Path,
    visit_dir: Path,
    test_params: tuple[bool, bool],
):
    # Unpack test params
    has_env, has_src = test_params

    # Create a mock LIF file and its destination path
    rsync_basepath = tmp_path / "data" / "clem"
    src = visit_dir / "images" / f"{project_name}.lif"
    dst = rsync_basepath / "current_year" / src.relative_to(visit_dir.parent)

    # Mock the environment
    mock_environment = (
        MagicMock(instrument_name=instrument_name, murfey_session=session_id)
        if has_env
        else None
    )

    # Mock '_get_source'
    mock_get_source = mocker.patch(
        "murfey.client.contexts.clem._get_source",
        return_value=visit_dir if has_src else None,
    )

    # Mock '_file_transferred_to'
    mock_file_transferred_to = mocker.patch(
        "murfey.client.contexts.clem._file_transferred_to", return_value=dst
    )

    # Mock 'capture_post'
    mock_capture_post = mocker.patch(
        "murfey.client.contexts.clem.capture_post", return_value=True
    )

    # Initialise the CLEMContext
    context = CLEMContext(
        acquisition_software="leica",
        basepath=tmp_path,
        machine_config={"rsync_basepath": str(rsync_basepath)},
        token="dummy",
    )
    # Run the function on the LIF file
    context.post_transfer(
        src,
        environment=mock_environment,
    )

    # Check that the calls were made with the expected parameters
    if not has_env:
        mock_get_source.assert_not_called()
        mock_file_transferred_to.assert_not_called()
        mock_capture_post.assert_not_called()
    else:
        mock_get_source.assert_called_once_with(src, mock_environment)
        if not has_src:
            mock_file_transferred_to.assert_not_called()
            mock_capture_post.assert_not_called()
        else:
            mock_file_transferred_to.assert_called_once_with(
                environment=mock_environment,
                source=visit_dir,
                file_path=src,
                rsync_basepath=rsync_basepath,
            )
            mock_capture_post.assert_called_once_with(
                base_url=mock.ANY,
                router_name="workflow_clem.router",
                function_name="process_raw_lifs",
                token=context._token,
                instrument_name=instrument_name,
                session_id=session_id,
                data={"lif_file": str(dst)},
            )


def test_post_transfer_tiff_data():
    pass
