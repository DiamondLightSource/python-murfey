from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
from pytest_mock import MockerFixture

from murfey.workflows.spa.atlas import atlas_jpg_from_mrc


def test_atlas_jpg_from_mrc(mocker: MockerFixture, tmp_path: Path):
    visit_name = "test_visit"
    instrument_name = "test"

    # Create a 16-bit grayscale image
    shape = (64, 64)
    test_data = np.ones(shape).astype("uint16")

    # Mock out the data returned from openning the file
    mock_mrcfile = mocker.patch("murfey.workflows.spa.atlas.mrcfile")
    mock_mrc = MagicMock()
    mock_mrc.data = test_data
    mock_mrcfile.open.return_value.__enter__.return_value = mock_mrc

    # Mock the return result of 'get_machine_config()'
    mock_machine_config = MagicMock()
    mock_machine_config.processed_directory_name = "processed"
    mocker.patch(
        "murfey.workflows.spa.atlas.get_machine_config",
        return_value={"test": mock_machine_config},
    )

    # Create a test file
    test_dir = tmp_path / instrument_name / "data" / visit_name / "atlas"
    test_dir.mkdir(parents=True, exist_ok=True)
    test_file = test_dir / "Atlas1.mrc"
    test_file.touch(exist_ok=True)

    # Run the function
    atlas_jpg_from_mrc(instrument_name, visit_name, test_file)
