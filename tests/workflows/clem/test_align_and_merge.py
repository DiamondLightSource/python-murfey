from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from murfey.server.ispyb import TransportManager
from murfey.util.config import MachineConfig
from murfey.workflows.clem.align_and_merge import submit_cluster_request

# Folder and file settings
session_id = 0
instrument_name = "clem"
raw_folder = "images"
processed_folder = "processed"
visit_name = "cm12345-6"
area_name = "test_area"
series_name = "test_series"
colors = [
    "gray",
    "green",
    "red",
]
feedback_queue = "murfey_feedback"

# Align and merge settings
crop_to_n_frames = 20
align_self = "enabled"
flatten = "max"
align_across = "enabled"


@pytest.fixture
def processed_dir(tmp_path: Path):
    processed_dir = tmp_path / visit_name / processed_folder
    processed_dir.mkdir(parents=True, exist_ok=True)
    return processed_dir


@pytest.fixture
def image_stacks(processed_dir: Path):

    image_dir = processed_dir / area_name / series_name
    image_dir.mkdir(parents=True, exist_ok=True)

    images = [image_dir / f"{color}.tiff" for color in colors]
    for image in images:
        if not image.exists():
            image.touch()

    return images


@pytest.fixture
def metadata(processed_dir: Path):

    metadata_dir = processed_dir / area_name / series_name / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata = metadata_dir / f"{series_name}.xml"
    if not metadata.exists():
        metadata.touch()

    return metadata


@patch("murfey.workflows.clem.align_and_merge.get_machine_config")
def test_submit_cluster_request(
    mock_get_machine_config,
    image_stacks: list[Path],
    metadata: Path,
    processed_dir: Path,
):

    # Construct the long series name
    series_name_long = "--".join(
        image_stacks[0].parent.relative_to(processed_dir).parts
    )

    # Create a mock tranpsort object
    mock_transport = MagicMock(spec=TransportManager)
    mock_transport.feedback_queue = feedback_queue

    # Construct a mock MachineConfig object for use within the function
    mock_machine_config = MagicMock(spec=MachineConfig)
    mock_machine_config.processed_directory_name = processed_folder
    mock_get_machine_config.return_value = {
        instrument_name: mock_machine_config,
    }

    # Run the function
    submit_cluster_request(
        session_id=session_id,
        instrument_name=instrument_name,
        series_name=series_name_long,
        images=image_stacks,
        metadata=metadata,
        crop_to_n_frames=crop_to_n_frames,
        align_self=align_self,
        flatten=flatten,
        align_across=align_across,
        messenger=mock_transport,
    )

    # Construct expected recipe to be sent
    sent_recipe = {
        "recipes": ["clem-align-and-merge"],
        "parameters": {
            # Job parameters
            "series_name": series_name_long,
            "images": [str(file) for file in image_stacks],
            "metadata": str(metadata),
            "crop_to_n_frames": crop_to_n_frames,
            "align_self": align_self,
            "flatten": flatten,
            "align_across": align_across,
            # Other recipe parameters
            "session_dir": str(processed_dir.parent),
            "session_id": session_id,
            "job_name": series_name_long,
            "feedback_queue": feedback_queue,
        },
    }

    # Check that it sends the expected recipe
    mock_transport.send.assert_called_once_with(
        "processing_recipe",
        sent_recipe,
        new_connection=True,
    )
