from pathlib import Path
from unittest.mock import MagicMock

import pytest

from murfey.server.ispyb import TransportManager
from murfey.workflows.clem.process_raw_tiffs import zocalo_cluster_request

# Set up variables
session_id = 0
instrument_name = "clem"
root_folder = "images"
visit_name = "cm12345-6"
area_name = "test_area"
feedback_queue = "murfey_feedback"

# Properties for TIFF images
num_z = 5
num_c = 3


@pytest.fixture
def raw_dir(tmp_path: Path):
    raw_dir = tmp_path / visit_name / root_folder
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


@pytest.fixture
def tiff_list(raw_dir: Path):
    (raw_dir / area_name).mkdir(parents=True, exist_ok=True)
    tiff_list = [
        raw_dir / area_name / f"test_series--Z{str(z).zfill(2)}--C{str(c).zfill(2)}.tif"
        for z in range(num_z)
        for c in range(num_c)
    ]
    for file in tiff_list:
        if not file.exists():
            file.touch()
    return tiff_list


@pytest.fixture
def metadata(raw_dir: Path):
    (raw_dir / area_name / "Metadata").mkdir(parents=True, exist_ok=True)
    metadata = raw_dir / area_name / "Metadata" / "test_series.xlif"
    if not metadata.exists():
        metadata.touch()
    return metadata


def test_zocalo_cluster_request(
    tiff_list: list[Path],
    metadata: Path,
    raw_dir: Path,
):

    # Create a mock tranpsort object
    mock_transport = MagicMock(spec=TransportManager)
    mock_transport.feedback_queue = feedback_queue

    # Run the function with the listed parameters
    zocalo_cluster_request(
        tiff_list=tiff_list,
        root_folder=root_folder,
        session_id=session_id,
        instrument_name=instrument_name,
        metadata=metadata,
        messenger=mock_transport,
    )

    # Construct the recipe that we expect to send
    job_name = "--".join(
        [
            p.replace(" ", "_") if " " in p else p
            for p in (
                tiff_list[0].parent.relative_to(raw_dir)
                / tiff_list[0].stem.split("--")[0]
            ).parts
        ]
    )
    sent_recipe = {
        "recipes": ["clem-tiff-to-stack"],
        "parameters": {
            # Job parameters
            "tiff_list": "null",
            "tiff_file": f"{str(tiff_list[0])}",
            "root_folder": root_folder,
            "metadata": f"{str(metadata)}",
            # Other recipe parameters
            "session_dir": f"{str(raw_dir.parent)}",
            "session_id": session_id,
            "job_name": job_name,
            "feedback_queue": feedback_queue,
        },
    }

    # Check that it sends the expected recipe
    mock_transport.send.assert_called_once_with(
        queue="processing_recipe",
        message=sent_recipe,
        new_connection=True,
    )
