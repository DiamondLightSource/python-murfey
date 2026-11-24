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
from murfey.util.config import MachineConfig
from tests.conftest import ExampleVisit

session_id = ExampleVisit.murfey_session_id
visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
instrument_name = ExampleVisit.instrument_name


@pytest.mark.parametrize(
    "has_transport",
    (True, False),
)
def test_process_raw_lifs(
    mocker: MockerFixture,
    tmp_path: Path,
    has_transport: bool,
):
    # Mock the transport object
    mock_transport = MagicMock(feedback_queue="clem")
    mocker.patch(
        "murfey.server._transport_object",
        mock_transport if has_transport else None,
    )

    # Mock the Murfey DB
    mock_murfey_session = MagicMock(
        visit=visit_name,
        instrument_name=instrument_name,
    )
    mock_db = MagicMock()
    mock_db.exec.return_value.one.return_value = mock_murfey_session

    # Mock the machine config
    machine_config = MachineConfig(processed_directory_name="processed")
    mocker.patch(
        "murfey.server.api.workflow_clem.get_machine_config",
        return_value={instrument_name: machine_config},
    )

    # Create the test LIF file
    visit_dir = tmp_path / "data" / "2020" / visit_name
    output_dir = visit_dir / machine_config.processed_directory_name
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
    else:
        # Construct the expected recipe
        recipe = {
            "recipes": ["clem-process-raw-lifs"],
            "parameters": {
                # Job parameters
                "lif_file": f"{str(lif_file.lif_file)}",
                "output_dir": str(output_dir),
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
    (  # Has transport object | Has TIFF files
        # Successful case
        (True, True),
        # Fail cases (one False at a time)
        (False, True),
        (True, False),
    ),
)
def test_process_raw_tiffs(
    mocker: MockerFixture,
    tmp_path: Path,
    test_params: tuple[bool, bool],
):
    # Unpack test params
    has_transport, has_tiffs = test_params

    # Mock the transport object
    mock_transport = MagicMock(feedback_queue="clem")
    mocker.patch(
        "murfey.server._transport_object",
        mock_transport if has_transport else None,
    )

    # Mock the Murfey DB
    mock_murfey_session = MagicMock(
        visit=visit_name,
        instrument_name=instrument_name,
    )
    mock_db = MagicMock()
    mock_db.exec.return_value.one.return_value = mock_murfey_session

    # Mock the machine config
    machine_config = MachineConfig(processed_directory_name="processed")
    mocker.patch(
        "murfey.server.api.workflow_clem.get_machine_config",
        return_value={instrument_name: machine_config},
    )

    # Create the test TIFF files
    visit_dir = tmp_path / "data" / "2020" / visit_name
    job_name = f"{visit_name}/images/grid_1/TileScan 1/Position 1"
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
            "series_name": job_name,
            "tiff_files": tiff_files,
            "series_metadata": metadata_file,
        }
    )

    # Construct the expected series name and output directory
    series_name = "grid_1/TileScan_1/Position_1"
    output_dir = (
        visit_dir / machine_config.processed_directory_name / series_name
    ).parent

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
    else:
        # Construct the expected recipe
        recipe = {
            "recipes": ["clem-process-raw-tiffs"],
            "parameters": {
                # Job parameters
                "tiff_list": None,
                "tiff_file": str(tiff_files[0]),
                "metadata": str(metadata_file),
                "output_dir": str(output_dir),
                "series_name": series_name,
                # Other recipe parameters
                "session_dir": str(visit_dir),
                "session_id": session_id,
                "job_name": job_name,
                "feedback_queue": "clem",
            },
        }
        mock_transport.send.assert_called_once_with(
            queue="processing_recipe",
            message=recipe,
            new_connection=True,
        )
