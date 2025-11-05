from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.workflows.clem.register_preprocessing_results import (
    _register_clem_image_series,
    _register_dcg_and_atlas,
    _register_grid_square,
    run,
)
from tests.conftest import ExampleVisit

visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
processed_dir_name = "processed"
grid_name = "Grid_1"
colors = ("gray", "green", "red")


@pytest.fixture
def preprocessing_messages(tmp_path: Path):
    # Make directory to where data for current grid is stored
    visit_dir = tmp_path / "data" / "2020" / visit_name
    processed_dir = visit_dir / processed_dir_name
    grid_dir = processed_dir / grid_name
    grid_dir.mkdir(parents=True, exist_ok=True)

    # Construct all the datasets to be tested
    datasets: list[tuple[Path, bool, bool, tuple[int, int], float, list[float]]] = [
        (
            grid_dir / "Overview_1" / "Image_1",
            False,
            True,
            (2400, 2400),
            1e-6,
            [0.002, 0.0044, 0.002, 0.0044],
        )
    ]
    # Add on metadata for a few grid squares
    datasets.extend(
        [
            (
                grid_dir / "TileScan_1" / f"Position_{n}",
                True,
                False,
                (2048, 2048),
                1.6e-7,
                [0.003, 0.00332768, 0.003, 0.00332768],
            )
            for n in range(5)
        ]
    )

    messages: list[dict[str, Any]] = []
    for dataset in datasets:
        # Unpack items from list of dataset parameters
        series_path = dataset[0]
        series_name = str(series_path.relative_to(processed_dir)).replace("/", "--")
        metadata = series_path / "metadata" / f"{series_path.stem}.xml"
        metadata.parent.mkdir(parents=True, exist_ok=True)
        metadata.touch(exist_ok=True)
        output_files = {color: str(series_path / f"{color}.tiff") for color in colors}
        for output_file in output_files.values():
            Path(output_file).touch(exist_ok=True)
        is_stack = dataset[1]
        is_montage = dataset[2]
        shape = dataset[3]
        pixel_size = dataset[4]
        extent = dataset[5]

        message = {
            "session_id": ExampleVisit.murfey_session_id,
            "result": {
                "series_name": series_name,
                "number_of_members": 3,
                "is_stack": is_stack,
                "is_montage": is_montage,
                "output_files": output_files,
                "metadata": str(metadata),
                "parent_lif": None,
                "parent_tiffs": {},
                "pixels_x": shape[0],
                "pixels_y": shape[1],
                "units": "m",
                "pixel_size": pixel_size,
                "resolution": 1 / pixel_size,
                "extent": extent,
            },
        }
        messages.append(message)
    return messages


@pytest.mark.skip
def test_register_clem_image_series():
    assert _register_clem_image_series


@pytest.mark.skip
def test_register_dcg_and_atlas():
    assert _register_dcg_and_atlas


@pytest.mark.skip
def test_register_grid_square():
    assert _register_grid_square


def test_run(
    mocker: MockerFixture,
    preprocessing_messages: list[dict[str, Any]],
):
    # Mock the MurfeyDB connection
    mock_murfey_session_entry = MagicMock()
    mock_murfey_session_entry.instrument_name = ExampleVisit.instrument_name
    mock_murfey_session_entry.visit = visit_name
    mock_murfey_db = MagicMock()
    mock_murfey_db.exec().return_value.one.return_value = mock_murfey_session_entry

    # Mock the registration helper functions
    mock_register_clem_series = mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results._register_clem_image_series"
    )
    mock_register_dcg_and_atlas = mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results._register_dcg_and_atlas"
    )
    mock_register_grid_square = mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results._register_grid_square"
    )

    # Mock the align and merge workflow call
    mock_align_and_merge_call = mocker.patch(
        "murfey.workflows.clem.register_preprocessing_results.submit_cluster_request"
    )

    for message in preprocessing_messages:
        result = run(
            message=message,
            murfey_db=mock_murfey_db,
        )
        assert result == {"success": True}
    assert mock_register_clem_series.call_count == len(preprocessing_messages)
    assert mock_register_dcg_and_atlas.call_count == len(preprocessing_messages)
    assert mock_register_grid_square.call_count == len(preprocessing_messages) - 1
    assert mock_align_and_merge_call.call_count == len(preprocessing_messages) * len(
        colors
    )
    assert run
