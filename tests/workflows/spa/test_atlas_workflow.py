from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest
from pytest_mock import MockerFixture
from werkzeug.utils import secure_filename

from murfey.workflows.spa.atlas import atlas_jpg_from_mrc

atlas_jpg_from_mrc_test_matrix = (
    ("Atlas1.mrc",),
    ("Sample1/Atlas1.mrc",),
)


@pytest.mark.parametrize("test_params", atlas_jpg_from_mrc_test_matrix)
def test_atlas_jpg_from_mrc(
    mocker: MockerFixture, tmp_path: Path, test_params: tuple[str]
):
    # Unpack test params
    (file_name_stub,) = test_params

    # Set up mock session params
    visit_name = "test_visit"
    instrument_name = "test"
    processed_dir_name = "processed"

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
    mock_machine_config.processed_directory_name = processed_dir_name
    mocker.patch(
        "murfey.workflows.spa.atlas.get_machine_config",
        return_value={"test": mock_machine_config},
    )

    # Create a test file
    test_file = (
        tmp_path / instrument_name / "data" / visit_name / "atlas" / file_name_stub
    )
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.touch(exist_ok=True)

    # Create the expected destination directory and file
    processed_dir = (
        tmp_path / instrument_name / "data" / visit_name / processed_dir_name / "atlas"
    )
    sample_id = "Sample"
    for part in file_name_stub.split("/"):
        if part.startswith("Sample"):
            sample_id = part
            break
    processed_file_name = processed_dir / secure_filename(
        f"{sample_id}_{test_file.stem}_fullres.jpg"
    )

    # Run the function and check that the expected calls are made
    atlas_jpg_from_mrc(instrument_name, visit_name, test_file)
    assert processed_file_name.exists()
