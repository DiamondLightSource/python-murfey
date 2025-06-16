from pathlib import Path
from unittest.mock import MagicMock

import pytest

from murfey.server.ispyb import TransportManager
from murfey.workflows.clem.process_raw_lifs import zocalo_cluster_request

# Set up variables
visit_name = "cm12345-6"
root_folder = "images"
session_id = 0
instrument_name = "clem"
feedback_queue = "murfey_feedback"


@pytest.fixture
def raw_dir(tmp_path: Path):
    raw_dir = tmp_path / visit_name / root_folder
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


@pytest.fixture
def lif_file(raw_dir: Path):
    file = raw_dir / "test_file.lif"
    if not file.exists():
        file.touch()
    return file


def test_zocalo_cluster_request(
    lif_file: Path,
    raw_dir: Path,
):

    # Create a mock tranpsort object
    mock_transport = MagicMock(spec=TransportManager)
    mock_transport.feedback_queue = feedback_queue

    # Run the function with the listed parameters
    zocalo_cluster_request(
        file=lif_file,
        root_folder=root_folder,
        session_id=session_id,
        instrument_name=instrument_name,
        messenger=mock_transport,
    )

    # Construct the recipe that we expect to send
    job_name = "--".join(
        [
            p.replace(" ", "_") if " " in p else p
            for p in (lif_file.relative_to(raw_dir).parent / lif_file.stem).parts
        ]
    )
    sent_recipe = {
        "recipes": ["clem-lif-to-stack"],
        "parameters": {
            # Job parameters
            "lif_file": f"{str(lif_file)}",
            "root_folder": root_folder,
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
