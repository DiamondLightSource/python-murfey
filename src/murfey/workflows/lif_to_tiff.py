"""
Script to allow Murfey to submit the LIF-to-TIFF job to the cluster.
The recipe referred to here is stored on GitLab.
"""

from pathlib import Path
from time import time

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass  # Ignore if ISPyB credentials environment variable not set


def zocalo_cluster_request(
    file: Path,
    root_folder: str,
    messenger: TransportManager | None = None,
):
    if messenger:
        # Construct paths to working and logging directories
        path_parts = list(file.parts)
        new_path = []
        log_path = []
        for p in range(len(path_parts)):
            part = path_parts[p]
            # Remove leading slash for subsequent rejoining
            if part == "/":
                part = ""
            # Append up to, but not including, root folder
            if part.lower() == root_folder.lower():
                log_path.append(part)
                break
            new_path.append(part)
            log_path.append(part)
        working_dir = Path("/".join(new_path))
        log_dir = Path("/".join(log_path)) / "tmp"

        messenger.send(
            "processing_recipe",
            {
                "recipes": ["lif-to-tiff"],
                "parameters": {
                    # Represent file paths canonically
                    "working_dir": repr(str(working_dir)),
                    "lif_path": repr(str(file)),
                    "root_dir": root_folder,
                    "log_dir": repr(str(log_dir)),
                    "job_id": str(int(round(time(), 0))),  # Use time as job ID for now
                },
            },
            new_connection=True,
        )
