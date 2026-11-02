from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.workflow_clem import (
    LifFileInfo,
    TIFFSeriesInfo,
    process_raw_lifs,
    process_raw_tiffs,
)
from murfey.util import sanitise_path

session_id = 1
visit_name = "cm12345-6"


@pytest.mark.parametrize(
    "test_params",
    (  # Has transport object | DB query success | Visits match
        # Successful case
        (True, True, True),
        # Fail cases (one False at a time)
        (False, True, True),
        (True, False, True),
        (True, True, False),
    ),
)
def test_process_raw_lifs(
    mocker: MockerFixture,
    tmp_path: Path,
    test_params: tuple[bool, bool, bool],
):
    # Unpack test params
    has_transport, query_successful, visits_match = test_params

    # Mock the transport object
    mock_transport = MagicMock(feedback_queue="clem")
    mocker.patch(
        "murfey.server._transport_object",
        mock_transport if has_transport else None,
    )

    # Mock the Murfey DB
    mock_murfey_session = MagicMock(
        visit=visit_name,
    )
    mock_db = MagicMock()
    if query_successful:
        mock_db.exec.return_value.one.return_value = mock_murfey_session
    else:
        mock_db.exec.return_value.one.side_effect = Exception("Something went wrong")

    # Create the test LIF file
    visit_dir = (
        tmp_path / "data" / "some_year" / (visit_name if visits_match else "cm12345-5")
    )
    test_file = visit_dir / "images" / "SomeLifProject.lif"
    lif_file = LifFileInfo(**{"lif_file": str(test_file)})

    # Mock the logger (check what the final logs are)
    mock_logger = mocker.patch("murfey.server.api.workflow_clem.logger")

    # Run the function and check that the outputs are as expected
    process_raw_lifs(
        session_id=session_id,
        lif_info=lif_file,
        murfey_db=mock_db,
    )

    if not has_transport:
        mock_logger.error.assert_called_with("No TransportManager object was set up")
    elif not query_successful:
        mock_logger.error.assert_called_with(
            "Error querying session information from database", exc_info=True
        )
        mock_transport.send.assert_not_called()
    elif not visits_match:
        mock_logger.error.assert_called_with(
            "Could not determine the visit directory from LIF file "
            f"{sanitise_path(lif_file.lif_file)}",
            exc_info=True,
        )
        mock_transport.send.assert_not_called()
    else:
        # Construct the expected recipe
        recipe = {
            "recipes": ["clem-process-raw-lifs"],
            "parameters": {
                # Job parameters
                "lif_file": f"{str(lif_file.lif_file)}",
                "root_folder": "images",
                # Other recipe parameters
                "session_dir": f"{str(visit_dir)}",
                "session_id": session_id,
                "job_name": f"{visit_name}/images/SomeLifProject",
                "feedback_queue": "clem",
            },
        }
        mock_transport.send.assert_called_once_with(
            queue="processing_recipe",
            message=recipe,
            new_connection=True,
        )


@pytest.mark.parametrize(
    "test_params",
    (  # Has transport object | Has TIFF files | DB query success | Visits match
        # Successful case
        (True, True, True, True),
        # Fail cases (one False at a time)
        (False, True, True, True),
        (True, False, True, True),
        (True, True, False, True),
        (True, True, True, False),
    ),
)
def test_process_raw_tiffs(
    mocker: MockerFixture,
    tmp_path: Path,
    test_params: tuple[bool, bool, bool, bool],
):
    # Unpack test params
    has_transport, has_tiffs, query_successful, visits_match = test_params

    # Mock the transport object
    mock_transport = MagicMock(feedback_queue="clem")
    mocker.patch(
        "murfey.server._transport_object",
        mock_transport if has_transport else None,
    )

    # Mock the Murfey DB
    mock_murfey_session = MagicMock(
        visit=visit_name,
    )
    mock_db = MagicMock()
    if query_successful:
        mock_db.exec.return_value.one.return_value = mock_murfey_session
    else:
        mock_db.exec.return_value.one.side_effect = Exception("Something went wrong")

    # Create the test TIFF files
    visit_dir = (
        tmp_path / "data" / "some_year" / (visit_name if visits_match else "cm12345-5")
    )
    series_name = f"{visit_name}/images/grid_1/TileScan 1/Position 1"
    tiff_files = (
        [
            visit_dir
            / "images"
            / "grid_1"
            / "TileScan 1"
            / f"Position 1--Z{str(i).zfill(2)}.tif"
            for i in range(10)
        ]
        if has_tiffs
        else []
    )
    metadata_file = (
        visit_dir / "images" / "grid_1" / "TileScan 1" / "Metadata" / "Position 1.xlif"
    )
    tiff_info = TIFFSeriesInfo(
        **{
            "series_name": series_name,
            "tiff_files": tiff_files,
            "series_metadata": metadata_file,
        }
    )

    # Mock the logger (check what the final logs are)
    mock_logger = mocker.patch("murfey.server.api.workflow_clem.logger")

    # Run the function and check that the outputs are as expected
    process_raw_tiffs(
        session_id=session_id,
        tiff_info=tiff_info,
        murfey_db=mock_db,
    )

    if not has_transport:
        mock_logger.error.assert_called_with("No TransportManager object was set up")
    elif not has_tiffs:
        mock_logger.error.assert_called_with(
            "No TIFF files were included in the incoming message"
        )
        mock_transport.send.assert_not_called()
    elif not query_successful:
        mock_logger.error.assert_called_with(
            "Error querying session information from database", exc_info=True
        )
        mock_transport.send.assert_not_called()
    elif not visits_match:
        mock_logger.error.assert_called_with(
            "Could not determine the visit directory from TIFF file "
            f"{sanitise_path(tiff_files[0])}",
            exc_info=True,
        )
        mock_transport.send.assert_not_called()
    else:
        # Construct the expected recipe
        recipe = {
            "recipes": ["clem-process-raw-tiffs"],
            "parameters": {
                # Job parameters
                "tiff_list": "null",
                "tiff_file": f"{str(tiff_files[0])}",
                "root_folder": "images",
                "metadata": f"{str(metadata_file)}",
                # Other recipe parameters
                "session_dir": f"{str(visit_dir)}",
                "session_id": session_id,
                "job_name": series_name,
                "feedback_queue": "clem",
            },
        }
        mock_transport.send.assert_called_once_with(
            queue="processing_recipe",
            message=recipe,
            new_connection=True,
        )
