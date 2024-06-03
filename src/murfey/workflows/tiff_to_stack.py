"""
Script to allow Murfey to submit the TIFF-to-stack job to the cluster.
The recipe referred to here is stored on GitLab.
"""

from pathlib import Path
from typing import Optional

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass  # Ignore if ISPyB credentials environment variable not set


def zocalo_cluster_request(
    tiff_file: Path,
    root_folder: str,
    metadata: Optional[Path],
    messenger: TransportManager | None = None,
):
    if messenger:

        # Set working directory to be the parent of the designated root folder
        path_parts = list(tiff_file.parts)
        new_path = []
        for p in range(len(path_parts)):
            part = path_parts[p]
            # Remove leading slash for subsequent rejoining
            if part == "/":
                part = ""
            # Append up to, but not including, root folder
            if part.lower() == root_folder.lower():
                break
            new_path.append(part)
        working_dir = Path("/".join(new_path))

        messenger.send(
            "processing_recipe",
            {
                "recipes": ["tiff-to-stack"],
                "parameters": {
                    "working_dir": str(working_dir),
                    "tiff_file": str(tiff_file),
                    "root_dir": root_folder,
                    "metadata": str(metadata),
                },
            },
            new_connection=True,
        )
