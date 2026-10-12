"""
Test module for the functions located in murfey.util.__init__
"""

from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from murfey.util import safe_chmod, secure_path

secure_path_test_matrix = (
    # Keep spaces? | Input path | Expected output
    # Tomo workflow examples
    (
        False,
        "D:/User_22220202_090000_cm40000-1/Position_1_1_001_0.00_22220202_090000_EER.eer",
        "D:/User_22220202_090000_cm40000-1/Position_1_1_001_0.00_22220202_090000_EER.eer",
    ),
    (
        False,
        "D:/User_22220202_090000_cm40000-1_/Position_1_1_001_0.00_22220202_090000_EER.eer",
        "D:/User_22220202_090000_cm40000-1_/Position_1_1_001_0.00_22220202_090000_EER.eer",
    ),
    # CLEM workflow examples
    (
        True,
        "D:/Session/cm40000-1/images/My Sample/TileScan 1/Position 1--Stage00--Z00--C00.tif",
        "D:/Session/cm40000-1/images/My Sample/TileScan 1/Position 1--Stage00--Z00--C00.tif",
    ),
    (
        True,
        "D:/Session/cm40000-1/images/My Sample_/TileScan 1/Position 1--Stage00--Z00--C00.tif",
        "D:/Session/cm40000-1/images/My Sample_/TileScan 1/Position 1--Stage00--Z00--C00.tif",
    ),
    (
        True,
        "D:/Session/cm40000-1/images/My_Sample_/TileScan 1/Position 1--Stage00--Z00--C00.tif",
        "D:/Session/cm40000-1/images/My_Sample_/TileScan 1/Position 1--Stage00--Z00--C00.tif",
    ),
    # Go wild
    (
        True,
        "D:/some path/to_/this/repo!/my_file.txt",
        "D:/some path/to_/this/repo/my_file.txt",
    ),
    (
        True,
        "D:/some path__/to_/this/repo/my file.txt",
        "D:/some path__/to_/this/repo/my file.txt",
    ),
    (
        False,
        "D:/some path__/to_/this/repo/my file.txt",
        "D:/some_path__/to_/this/repo/my_file.txt",
    ),
)


@pytest.mark.parametrize("test_params", secure_path_test_matrix)
def test_secure_path(test_params: tuple[bool, str, str]):
    # Unpack test params
    keep_spaces, input_path, expected_output = test_params
    assert secure_path(Path(input_path), keep_spaces) == Path(expected_output)


@pytest.mark.parametrize(
    "error_raised",
    (
        None,
        PermissionError,
        FileNotFoundError,
        Exception,
    ),
)
def test_safe_chmod(
    mocker: MockerFixture,
    error_raised: None | Exception,
    tmp_path: Path,
):
    # Create a test dir
    test_dir = tmp_path / "test"
    test_dir.mkdir(parents=True, exist_ok=True)

    # Mock the logger
    mock_logger = mocker.patch("murfey.util.logger")

    # Smooth case
    if error_raised is None:
        safe_chmod(test_dir, 0o777)
        mock_logger.warning.assert_not_called()
    # PermissionError allowed
    elif error_raised is PermissionError:
        mocker.patch("murfey.util.os.chmod", side_effect=[error_raised])
        safe_chmod(test_dir, 0o777)
        mock_logger.warning.assert_called_with(
            f"Unable to change permissions of {test_dir}, will attempt to proceed",
            exc_info=True,
        )
    # Every other exception should be raised
    else:
        mocker.patch("murfey.util.os.chmod", side_effect=[error_raised])
        with pytest.raises(error_raised):
            safe_chmod(test_dir, 0o777)
