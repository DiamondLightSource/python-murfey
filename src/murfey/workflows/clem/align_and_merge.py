"""
Script to allow Murfey to request for an image alignment, colorisation, and merge job
from cryoemservices.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

from murfey.util.config import get_machine_config

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass  # Ignore if ISPyB credentials environment variable not set


def submit_cluster_request(
    # Session parameters
    session_id: int,
    instrument_name: str,
    # Processing parameters
    series_name: str,
    images: list[Path],
    metadata: Path,
    # Optional processing parameters
    crop_to_n_frames: Optional[int] = None,
    align_self: Literal["enabled", ""] = "",
    flatten: Literal["mean", "min", "max", ""] = "mean",
    align_across: Literal["enabled", ""] = "",
    # Optional session parameters
    messenger: Optional[TransportManager] = None,
):
    if not messenger:
        raise Exception("Unable to find transport manager")

    # Load feedback queue
    machine_config = get_machine_config()[instrument_name]
    feedback_queue: str = messenger.feedback_queue

    # Work out session directory from file path
    processed_folder = machine_config.processed_directory_name
    if not images:
        raise ValueError(f"No image files have been provided for {series_name!r}")
    reference_file = images[0]
    path_parts = list(reference_file.parts)
    path_parts[0] = "" if path_parts[0] == "/" else path_parts[0]
    try:
        root_index = path_parts.index(processed_folder)
    except ValueError:
        raise ValueError(
            f"The processed directory {processed_folder!r} could not be found in the "
            f"file path for {str(reference_file)!r}"
        )
    session_dir = Path("/".join(path_parts[:root_index]))

    # Submit message to cryoemservices
    messenger.send(
        "processing_recipe",
        {
            "recipes": ["clem-align-and-merge"],
            "parameters": {
                # Job parameters
                "series_name": series_name,
                "images": [str(file) for file in images],
                "metadata": str(metadata),
                "crop_to_n_frames": crop_to_n_frames,
                "align_self": align_self,
                "flatten": flatten,
                "align_across": align_across,
                # Other recipe parameters
                "session_dir": str(session_dir),
                "session_id": session_id,
                "job_name": series_name,
                "feedback_queue": feedback_queue,
            },
        },
        new_connection=True,
    )
    return True
