"""
Script to allow Murfey to submit the TIFF-to-stack job to the cluster.
The recipe referred to here is stored on GitLab.
"""

from logging import getLogger
from pathlib import Path
from typing import Optional

logger = getLogger("murfey.workflows.clem.process_raw_tiffs")

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass  # Ignore if ISPyB credentials environment variable not set


def zocalo_cluster_request(
    tiff_list: list[Path],
    root_folder: str,
    session_id: int,
    instrument_name: str,
    metadata: Optional[Path] = None,
    messenger: Optional[TransportManager] = None,
):
    if messenger:
        # Construct path to session directory
        path_parts = list(
            (tiff_list[0].parent / (tiff_list[0].stem.split("--")[0])).parts
        )
        # Replace leading "/" in Unix paths
        path_parts[0] = "" if path_parts[0] == "/" else path_parts[0]
        try:
            # Find the position of the root folder in the list
            root_index = [p.lower() for p in path_parts].index(root_folder.lower())
        except ValueError:
            raise Exception(
                f"Unable to find the root folder {root_folder!r} in the file path {tiff_list[0]!r}"
            )
        # Construct the session and job name
        session_dir = "/".join(path_parts[:root_index])
        job_name = "--".join(
            [p.replace(" ", "_") if " " in p else p for p in path_parts][
                root_index + 1 :
            ]
        )

        # If no metadata file provided, generate path to one
        if metadata is None:
            series_name = tiff_list[0].stem.split("--")[0]
            metadata = tiff_list[0].parent / "Metadata" / (series_name + ".xlif")

        # Load machine config to get the feedback queue
        feedback_queue: str = messenger.feedback_queue

        # Construct recipe and submit it for processing
        recipe = {
            "recipes": ["clem-tiff-to-stack"],
            "parameters": {
                # Job parameters
                "tiff_list": "null",
                "tiff_file": f"{str(tiff_list[0])}",
                "root_folder": root_folder,
                "metadata": f"{str(metadata)}",
                # Other recipe parameters
                "session_dir": f"{str(session_dir)}",
                "session_id": session_id,
                "job_name": job_name,
                "feedback_queue": feedback_queue,
            },
        }
        logger.debug(
            f"Submitting TIFF processing request to {messenger.feedback_queue!r} "
            "with the following recipe: \n"
            f"{recipe}"
        )
        messenger.send(
            queue="processing_recipe",
            message=recipe,
            new_connection=True,
        )
    else:
        raise Exception("Unable to find transport manager")
