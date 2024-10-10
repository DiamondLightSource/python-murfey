"""
Script to allow Murfey to submit the LIF-to-STACK job to the cluster.
The recipe referred to here is stored on GitLab.
"""

from pathlib import Path
from typing import Optional

from murfey.util.config import get_machine_config

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass  # Ignore if ISPyB credentials environment variable not set


def zocalo_cluster_request(
    file: Path,
    root_folder: str,
    session_id: int,  # Provided by the client via the API endpoint
    instrument_name: str,  # Acquired by looking up the Session table
    messenger: Optional[TransportManager] = None,
):
    if messenger:
        path_parts = list(file.parts)

        # Construct path to session directory
        session_dir_parts = []
        for p in range(len(path_parts)):
            part = path_parts[p]
            # Remove leading slash for subsequent rejoining
            if part == "/":
                part = ""
            # Append up to, but not including, root folder
            if part.lower() == root_folder.lower():
                break
            session_dir_parts.append(part)
        session_dir = Path("/".join(session_dir_parts))

        # Construct the job name
        job_name_parts = []
        trigger = False
        for p in range(len(path_parts)):
            part = path_parts[p].replace(" ", "_")  # Remove spaces
            if trigger is True:
                job_name_parts.append(part)
            # Start appending at the level below the root folder
            if part.lower() == root_folder.lower():
                trigger = True
        job_name = "--".join(job_name_parts)

        # Load machine config to get the feedback queue
        machine_config = get_machine_config()
        feedback_queue = machine_config[instrument_name].feedback_queue

        # Send the message
        messenger.send(
            "processing_recipe",
            {
                "recipes": ["clem-lif-to-stack"],
                "parameters": {
                    "session_dir": str(session_dir),
                    "lif_path": str(file),
                    "root_dir": root_folder,
                    "job_name": job_name,
                    "feedback_queue": feedback_queue,
                    "session_id": session_id,
                },
            },
            new_connection=True,
        )
    else:
        raise Exception("Unable to find transport manager")
