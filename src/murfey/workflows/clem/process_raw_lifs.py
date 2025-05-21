"""
Script to allow Murfey to submit the LIF-to-STACK job to the cluster.
The recipe referred to here is stored on GitLab.
"""

from logging import getLogger
from pathlib import Path
from typing import Optional

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass  # Ignore if ISPyB credentials environment variable not set

logger = getLogger("murfey.workflows.clem.process_raw_lifs")


def zocalo_cluster_request(
    file: Path,
    root_folder: str,
    session_id: int,  # Provided by the client via the API endpoint
    instrument_name: str,  # Acquired by looking up the Session table
    messenger: Optional[TransportManager] = None,
):
    if messenger:
        # Use file path parts to construct parameters
        path_parts = list((file.parent / file.stem).parts)
        # Replace leading "/" in Unix paths
        path_parts[0] = "" if path_parts[0] == "/" else path_parts[0]
        try:
            # Find the position of the root folder in the list
            root_index = [p.lower() for p in path_parts].index(root_folder.lower())
        except ValueError:
            raise Exception(
                f"Unable to find the root folder {root_folder!r} in the file path {file!r}"
            )

        # Construct the session and job name
        session_dir = "/".join(path_parts[:root_index])
        job_name = "--".join(
            [p.replace(" ", "_") if " " in p else p for p in path_parts][
                root_index + 1 :
            ]
        )

        # Load machine config to get the feedback queue
        feedback_queue: str = messenger.feedback_queue

        # Construct recipe and submit it for processing
        recipe = {
            "recipes": ["clem-lif-to-stack"],
            "parameters": {
                # Job parameters
                "lif_file": f"{str(file)}",
                "root_folder": root_folder,
                # Other recipe parameters
                "session_dir": f"{str(session_dir)}",
                "session_id": session_id,
                "job_name": job_name,
                "feedback_queue": feedback_queue,
            },
        }
        logger.debug(
            f"Submitting LIF processing request to {messenger.feedback_queue!r} "
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
